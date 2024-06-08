mod deserialize;
mod ids;

use std::{
    borrow::Borrow,
    cell::{RefCell, RefMut},
    rc::Rc,
    sync::Arc,
};

pub(crate) use deserialize::SeedContext;
use id_newtypes::IdRange;
pub use ids::*;
use itertools::Either;
use schema::{ObjectId, Schema};

use self::deserialize::UpdateSeed;

use super::{
    GraphqlError, InitialResponse, Response, ResponseData, ResponseEdge, ResponseObject, ResponseObjectRef,
    ResponsePath, ResponseValue, UnpackedResponseEdge,
};
use crate::plan::{OperationPlan, PlanBoundaryId, PlanWalker};

pub(crate) struct ResponseDataPart {
    id: ResponseDataPartId,
    objects: Vec<ResponseObject>,
    lists: Vec<ResponseValue>,
}

impl ResponseDataPart {
    fn new(id: ResponseDataPartId) -> Self {
        Self {
            id,
            objects: Vec::new(),
            lists: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.lists.is_empty()
    }
}

pub(crate) struct ResponseBuilder {
    // will be None if an error propagated up to the root.
    pub(super) root: Option<(ResponseObjectId, ObjectId)>,
    parts: Vec<ResponseDataPart>,
    errors: Vec<GraphqlError>,
}

// Only supporting additions for the current graph. Deletion are... tricky
// It shouldn't be that difficult to know whether a remaining plan still needs a field after
// execution plan creation. But it's definitely not efficient currently. I think we can at
// least wait until we face actual problems. We're focused on OLTP workloads, so might never
// happen.
impl ResponseBuilder {
    pub fn new(root_object_id: ObjectId) -> Self {
        let mut initial_part = ResponseDataPart {
            id: ResponseDataPartId::from(0),
            objects: Vec::new(),
            lists: Vec::new(),
        };
        let root_id = initial_part.push_object(ResponseObject::default());
        Self {
            root: Some((root_id, root_object_id)),
            parts: vec![initial_part],
            errors: Vec::new(),
        }
    }

    pub fn new_writer(
        &mut self,
        root_response_objects: Arc<Vec<ResponseObjectRef>>,
        plan_boundary_ids: IdRange<PlanBoundaryId>,
    ) -> ResponsePart {
        let id = ResponseDataPartId::from(self.parts.len());
        // reserving the spot until the actual data is written. It's safe as no one can reference
        // any data in this part before it's added. And a part can only be overwritten if it's
        // empty.
        self.parts.push(ResponseDataPart::new(id));
        ResponsePart::new(ResponseDataPart::new(id), root_response_objects, plan_boundary_ids)
    }

    pub fn root_response_boundary_item(&self) -> Option<ResponseObjectRef> {
        self.root.map(|(response_object_id, object_id)| ResponseObjectRef {
            id: response_object_id,
            path: ResponsePath::default(),
            definition_id: object_id,
        })
    }

    pub fn ingest(&mut self, part: ResponsePart) -> Vec<(PlanBoundaryId, Vec<ResponseObjectRef>)> {
        let part = Rc::into_inner(part.inner)
            .expect("All use of ResponsePart must be finished by now.")
            .into_inner();

        let reservation = &mut self.parts[usize::from(part.data.id)];
        assert!(reservation.is_empty(), "Part already has data");
        *reservation = part.data;

        self.errors.extend(part.errors);
        for (update, obj_ref) in part.updates.into_iter().zip(part.root_response_objects.iter()) {
            match update {
                UpdateSlot::Reserved => todo!(),
                UpdateSlot::Fields(fields) => {
                    self[obj_ref.id].extend(fields);
                }
                UpdateSlot::Error => {
                    self.propagate_error(&obj_ref.path);
                }
            }
        }
        for path in part.error_paths_to_propagate {
            self.propagate_error(&path);
        }
        // The boundary objects are only accessible after we ingested them
        part.plan_boundaries
    }

    pub fn build(self, schema: Arc<Schema>, operation: Arc<OperationPlan>) -> Response {
        Response::Initial(InitialResponse {
            data: ResponseData {
                schema,
                operation,
                root: self.root.map(|(id, _)| id),
                parts: self.parts,
            },
            errors: self.errors,
        })
    }

    // The path corresponds to place where a plan failed but couldn't go propagate higher as data
    // was in a different part (provided by a parent plan).
    // To correctly propagate error we're finding the last nullable element in the path and make it
    // nullable. If there's nothing, then root will be null.
    fn propagate_error(&mut self, path: &ResponsePath) {
        let Some((root, _)) = self.root else {
            return;
        };

        let mut last_nullable: Option<ResponseValueId> = None;
        let mut previous: Either<ResponseObjectId, ResponseListId> = Either::Left(root);
        for &edge in path.iter() {
            let (id, value) = match (previous, edge.unpack()) {
                (
                    Either::Left(object_id),
                    UnpackedResponseEdge::BoundResponseKey(_) | UnpackedResponseEdge::ExtraFieldResponseKey(_),
                ) => {
                    let Some(field_position) = self[object_id].field_position(edge) else {
                        // Shouldn't happen but equivalent to null
                        return;
                    };
                    let id = ResponseValueId::ObjectField {
                        object_id,
                        field_position,
                    };
                    let value = &self[object_id][field_position];
                    (id, value)
                }
                (Either::Right(list_id), UnpackedResponseEdge::Index(index)) => {
                    let id = ResponseValueId::ListItem { list_id, index };
                    let Some(value) = self[list_id].get(index) else {
                        // Shouldn't happen but equivalent to null
                        return;
                    };
                    (id, value)
                }
                _ => return,
            };
            if value.is_null() {
                return;
            }
            match *value {
                ResponseValue::Object {
                    nullable,
                    part_id,
                    index,
                } => {
                    if nullable {
                        last_nullable = Some(id);
                    }
                    previous = Either::Left(ResponseObjectId { part_id, index });
                }
                ResponseValue::List {
                    nullable,
                    part_id,
                    offset,
                    length,
                } => {
                    if nullable {
                        last_nullable = Some(id);
                    }
                    previous = Either::Right(ResponseListId {
                        part_id,
                        offset,
                        length,
                    });
                }
                _ => break,
            }
        }
        if let Some(last_nullable) = last_nullable {
            match last_nullable {
                ResponseValueId::ObjectField {
                    object_id,
                    field_position,
                } => {
                    self[object_id][field_position] = ResponseValue::Null;
                }
                ResponseValueId::ListItem { list_id, index } => {
                    self[list_id][index] = ResponseValue::Null;
                }
            }
        } else {
            self.root = None;
        }
    }
}

pub enum ResponseValueId {
    ObjectField {
        object_id: ResponseObjectId,
        field_position: usize,
    },
    ListItem {
        list_id: ResponseListId,
        index: usize,
    },
}

#[derive(Clone)]
pub(crate) struct ResponsePart {
    /// We end up writing objects or lists at various step of the de-serialization / query
    /// traversal, so having a RefCell is by far the easiest. We don't need a lock as executor are
    /// not expected to parallelize their work.
    /// The Rc makes it possible to write errors at one place and the data in another.
    inner: Rc<RefCell<ResponsePartInner>>,
}

pub(crate) struct ResponsePartInner {
    data: ResponseDataPart,
    root_response_objects: Arc<Vec<ResponseObjectRef>>,
    errors: Vec<GraphqlError>,
    updates: Vec<UpdateSlot>,
    error_paths_to_propagate: Vec<ResponsePath>,
    plan_boundary_ids_start: usize,
    plan_boundaries: Vec<(PlanBoundaryId, Vec<ResponseObjectRef>)>,
}

impl ResponsePart {
    fn new(
        data: ResponseDataPart,
        root_response_objects: Arc<Vec<ResponseObjectRef>>,
        plan_boundary_ids: IdRange<PlanBoundaryId>,
    ) -> ResponsePart {
        let inner = ResponsePartInner {
            data,
            root_response_objects,
            errors: Vec::new(),
            updates: Vec::new(),
            error_paths_to_propagate: Vec::new(),
            plan_boundary_ids_start: usize::from(plan_boundary_ids.start),
            plan_boundaries: plan_boundary_ids.map(|id| (id, Vec::new())).collect(),
        };
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }

    pub fn next_seed<'ctx>(&self, plan: PlanWalker<'ctx>) -> Option<UpdateSeed<'ctx>> {
        self.next_writer().map(|writer| UpdateSeed::new(plan, writer))
    }

    pub fn next_writer(&self) -> Option<ResponseWriter> {
        let index = {
            let mut inner = self.inner.borrow_mut();
            if inner.updates.len() == inner.data.objects.len() {
                return None;
            }
            inner.updates.push(UpdateSlot::Reserved);
            inner.updates.len() - 1
        };
        Some(ResponseWriter {
            index,
            part: self.clone(),
        })
    }

    pub fn push_error(&self, error: impl Into<GraphqlError>) {
        self.inner.borrow_mut().errors.push(error.into());
    }

    pub fn push_errors(&self, errors: Vec<GraphqlError>) {
        self.inner.borrow_mut().errors.extend(errors);
    }
}

pub struct ResponseWriter {
    index: usize,
    part: ResponsePart,
}

impl ResponseWriter {
    fn part(&self) -> RefMut<'_, ResponsePartInner> {
        self.part.inner.borrow_mut()
    }

    pub fn root_path(&self) -> ResponsePath {
        RefCell::borrow(&self.part.inner).root_response_objects[self.index]
            .path
            .clone()
    }

    pub fn push_object(&self, object: ResponseObject) -> ResponseObjectId {
        self.part().data.push_object(object)
    }

    pub fn push_list(&self, value: &[ResponseValue]) -> ResponseListId {
        self.part().data.push_list(value)
    }

    pub fn update_root_object_with(&self, fields: Vec<(ResponseEdge, ResponseValue)>) {
        self.part().updates[self.index] = UpdateSlot::Fields(fields);
    }

    pub fn propagate_error(&self, error: impl Into<GraphqlError>) {
        let mut part = self.part();
        part.errors.push(error.into());
        part.updates[self.index] = UpdateSlot::Error;
    }

    pub fn continue_error_propagation(&self) {
        self.part().updates[self.index] = UpdateSlot::Error;
    }

    pub fn push_error(&self, error: impl Into<GraphqlError>) {
        self.part().errors.push(error.into());
    }

    pub fn push_errors(&self, errors: Vec<GraphqlError>) {
        self.part().errors.extend(errors);
    }
}

// impl std::ops::Index<PlanBoundaryId> for ResponseWriter {
//     type Output = Vec<ResponseObjectRef>;
//
//     fn index(&self, id: PlanBoundaryId) -> &Self::Output {
//         let n = usize::from(id) - self.plan_boundary_ids_start;
//         &self.plan_boundaries[n].1
//     }
// }
//
// impl std::ops::IndexMut<PlanBoundaryId> for ResponseWriter {
//     fn index_mut(&mut self, id: PlanBoundaryId) -> &mut Self::Output {
//         let n = usize::from(id) - self.plan_boundary_ids_start;
//         &mut self.plan_boundaries[n].1
//     }
// }

enum UpdateSlot {
    Reserved,
    Fields(Vec<(ResponseEdge, ResponseValue)>),
    Error,
}

pub struct ResponseObjectUpdate {
    pub id: ResponseObjectId,
    pub fields: Vec<(ResponseEdge, ResponseValue)>,
}
