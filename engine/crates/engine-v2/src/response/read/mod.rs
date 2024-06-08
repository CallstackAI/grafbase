use std::{borrow::Cow, sync::Arc};

use super::ResponseBuilder;
mod selection_set;
mod ser;
mod view;

use schema::SchemaWalker;
pub use selection_set::{ReadField, ReadSelectionSet};
pub use view::{ResponseBoundaryObjectsView, ResponseObjectRef};

impl ResponseBuilder {
    pub fn read<'a>(
        &'a self,
        schema: SchemaWalker<'a, ()>,
        items: Arc<Vec<ResponseObjectRef>>,
        selection_set: Cow<'a, ReadSelectionSet>,
    ) -> ResponseBoundaryObjectsView<'a> {
        ResponseBoundaryObjectsView {
            schema,
            response: self,
            items,
            selection_set,
            extra_constant_fields: vec![],
        }
    }
}
