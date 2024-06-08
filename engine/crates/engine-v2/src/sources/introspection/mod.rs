use super::{ExecutionError, Executor, ExecutorInput};
use crate::{execution::ExecutionContext, plan::PlanWalker, response::ResponsePart};

mod writer;

pub(crate) struct IntrospectionExecutionPlan;

impl IntrospectionExecutionPlan {
    #[allow(clippy::unnecessary_wraps)]
    pub fn new_executor<'ctx>(
        &'ctx self,
        ExecutorInput {
            ctx,
            plan,
            response_part,
            ..
        }: ExecutorInput<'ctx, '_>,
    ) -> Result<Executor<'ctx>, ExecutionError> {
        Ok(Executor::Introspection(IntrospectionExecutor {
            ctx,
            plan,
            response_part,
        }))
    }
}

pub(crate) struct IntrospectionExecutor<'ctx> {
    ctx: ExecutionContext<'ctx>,
    plan: PlanWalker<'ctx>,
    response_part: ResponsePart,
}

impl<'ctx> IntrospectionExecutor<'ctx> {
    pub async fn execute(self) -> Result<ResponsePart, ExecutionError> {
        writer::IntrospectionWriter {
            schema: self.ctx.engine.schema.walker(),
            metadata: self.ctx.engine.schema.walker().introspection_metadata(),
            plan: self.plan,
            response: self.response_part.next_writer().ok_or_else(|| "No objects to update")?,
        }
        .execute();
        Ok(self.response_part)
    }
}
