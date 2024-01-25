use std::sync::Arc;

use async_runtime::stream::StreamExt as _;
use engine::RequestHeaders;
use engine_parser::types::OperationType;
use futures::channel::mpsc;
use futures_util::{SinkExt, Stream};
use schema::Schema;
use tokio::sync::RwLock;

use crate::{
    execution::{ExecutorCoordinator, PreparedExecution, PreparedRequest, Variables},
    request::{parse_operation, Operation},
    response::{ExecutionMetadata, GraphqlError, Response},
};

pub struct Engine {
    // We use an Arc for the schema to have a self-contained response which may still
    // needs access to the schema strings
    pub(crate) schema: Arc<Schema>,
    pub(crate) env: EngineEnv,
    op_cache: RwLock<fnv::FnvHashMap<String, Arc<Operation>>>,
}

pub struct EngineEnv {
    pub fetcher: runtime::fetch::Fetcher,
}

impl Engine {
    pub fn new(schema: Schema, env: EngineEnv) -> Self {
        Self {
            schema: Arc::new(schema),
            env,
            op_cache: Default::default(),
        }
    }

    pub async fn execute(self: &Arc<Self>, mut request: engine::Request, headers: RequestHeaders) -> PreparedExecution {
        let operation = match self.cached_prepare_operation(&request).await {
            Ok(operation) => operation,
            Err(error) => {
                return PreparedExecution::bad_request(Response::from_error(error, ExecutionMetadata::default()))
            }
        };
        let variables = match Variables::from_request(&operation, self.schema.as_ref(), &mut request.variables) {
            Ok(variables) => variables,
            Err(errors) => {
                return PreparedExecution::bad_request(Response::from_errors(
                    errors,
                    ExecutionMetadata::build(&operation),
                ))
            }
        };

        if matches!(operation.ty, OperationType::Subscription) {
            return PreparedExecution::bad_request(Response::from_error(
                GraphqlError::new("Subscriptions are only suported on streaming transports.  Try making a request with SSE or WebSockets"),
                ExecutionMetadata::build(&operation),
            ));
        }

        PreparedExecution::PreparedRequest(PreparedRequest {
            query: request.query,
            operation,
            variables,
            headers,
            engine: Arc::clone(self),
        })
    }

    pub fn execute_stream(
        self: &Arc<Self>,
        request: engine::Request,
        headers: RequestHeaders,
    ) -> impl Stream<Item = Response> {
        let (mut sender, receiver) = mpsc::channel(2);
        let engine = Arc::clone(self);

        receiver.join(async move {
            let coordinator = match engine.prepare_coordinator(request, headers) {
                Ok(coordinator) => coordinator,
                Err(response) => {
                    sender.send(response).await.ok();
                    return;
                }
            };

            if matches!(
                coordinator.operation_type(),
                OperationType::Query | OperationType::Mutation
            ) {
                sender.send(coordinator.execute().await).await.ok();
                return;
            }

            coordinator.execute_subscription(sender).await
        })
    }

    fn prepare_coordinator(
        &self,
        mut request: engine::Request,
        headers: RequestHeaders,
    ) -> Result<ExecutorCoordinator<'_>, Response> {
        let operation = match self.prepare_operation(&request) {
            Ok(operation) => operation,
            Err(error) => return Err(Response::from_error(error, ExecutionMetadata::default())),
        };
        let variables = match Variables::from_request(&operation, self.schema.as_ref(), &mut request.variables) {
            Ok(variables) => variables,
            Err(errors) => return Err(Response::from_errors(errors, ExecutionMetadata::build(&operation))),
        };

        Ok(ExecutorCoordinator::new(self, operation, variables, headers))
    }

    async fn cached_prepare_operation(&self, request: &engine::Request) -> Result<Arc<Operation>, GraphqlError> {
        let guard = self.op_cache.read().await;
        if let Some(operation) = guard.get(&request.query).cloned() {
            Ok(operation)
        } else {
            drop(guard);
            let operation = self.prepare_operation(request)?;
            self.op_cache
                .write()
                .await
                .insert(request.query.clone(), Arc::clone(&operation));
            Ok(operation)
        }
    }
    fn prepare_operation(&self, request: &engine::Request) -> Result<Arc<Operation>, GraphqlError> {
        let unbound_operation = parse_operation(request)?;
        let operation = Operation::build(&self.schema, unbound_operation)?;
        Ok(Arc::new(operation))
    }
}
