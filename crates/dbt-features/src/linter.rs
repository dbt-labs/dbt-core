use crate::feature_stack::CommandHandler;

pub(crate) struct LinterCommandHandler;

impl CommandHandler for LinterCommandHandler {
    fn process_eval_args(
        &self,
        _eval_args: &dbt_common::io_args::EvalArgs,
        _resolver_state: &dbt_schemas::state::ResolverState,
    ) -> dbt_common::FsResult<()> {
        unimplemented!("source-available lint command is not yet implemented")
    }
}
