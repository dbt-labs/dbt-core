use crate::{jinja_environment::JinjaEnv, listener};
use dbt_common::{ErrorCode, FsError, FsResult, fs_err, io_args::IoArgs, show_error};
use minijinja::{Value, compiler::codegen::CodeGenerationProfile, load_builtins};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

#[allow(clippy::too_many_arguments)]
/// Typecheck a batch of files
pub fn typecheck(
    arg_io: &IoArgs,
    env: Arc<JinjaEnv>,
    noqa_comments: &HashMap<PathBuf, HashSet<u32>>,
    jinja_typechecking_listener_factory: Arc<dyn listener::JinjaTypeCheckingEventListenerFactory>,
    target_package_name: Option<String>,
    root_package_name: &str,
    dbt_and_adapters_namespace: Value,
    relative_file_path: &Path,
    content: &str,
    offset: &dbt_common::CodeLocation,
    unique_id: &str,
) -> FsResult<()> {
    let function_signatures = env.jinja_function_registry.clone();
    let mut jinja_typecheck_env = env.env.clone();
    let macro_namespace_registry = env.env.get_macro_namespace_registry();
    let builtins = load_builtins(macro_namespace_registry)
        .map_err(|e| FsError::from_jinja_err(e, "Failed to load built-ins"))?;

    let mut typecheck_resolved_context: BTreeMap<String, Value> = BTreeMap::new();

    if let Some(target_package_name) = target_package_name {
        typecheck_resolved_context.insert(
            "TARGET_PACKAGE_NAME".to_string(),
            Value::from(target_package_name),
        );
    }

    typecheck_resolved_context.insert(
        "ROOT_PACKAGE_NAME".to_string(),
        Value::from(root_package_name.to_string()),
    );

    typecheck_resolved_context.insert(
        "DBT_AND_ADAPTERS_NAMESPACE".to_string(),
        dbt_and_adapters_namespace,
    );

    jinja_typecheck_env.profile = CodeGenerationProfile::TypeCheck(
        function_signatures.clone(),
        typecheck_resolved_context.clone(),
    );

    let absolute_file_path = arg_io.in_dir.join(relative_file_path);

    let listener = jinja_typechecking_listener_factory.create_listener(
        arg_io,
        offset.clone(),
        noqa_comments.get(relative_file_path).cloned(),
        unique_id,
    );

    let source = content.to_string();
    let tmpl = match jinja_typecheck_env.template_from_str(&source) {
        Ok(tmpl) => tmpl,
        Err(e) => {
            show_error!(
                &arg_io,
                fs_err!(ErrorCode::Generic, "Failed to create template: {}", e)
            );
            return Ok(());
        }
    };

    match tmpl.typecheck(
        function_signatures,
        builtins,
        listener.clone(),
        typecheck_resolved_context.clone(),
    ) {
        Ok(_) => {
            listener.flush();
        }
        Err(e) => {
            listener.flush();
            show_error!(
                &arg_io,
                fs_err!(
                    ErrorCode::Generic,
                    "Type checking failed for file {}: {}",
                    absolute_file_path.display(),
                    e
                )
            );
        }
    }
    jinja_typechecking_listener_factory.destroy_listener(relative_file_path, listener);

    Ok(())
}
