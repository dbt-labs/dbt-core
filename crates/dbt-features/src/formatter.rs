use crate::feature_stack::CommandHandler;

pub(crate) struct FormatterCommandHandler;

impl CommandHandler for FormatterCommandHandler {
    fn process_eval_args(
        &self,
        _eval_args: &dbt_common::io_args::EvalArgs,
        _resolver_state: &dbt_schemas::state::ResolverState,
    ) -> dbt_common::FsResult<()> {
        unimplemented!("source-available format command is not yet implemented")
    }
}
