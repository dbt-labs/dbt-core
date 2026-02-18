use crate::args::ResolveArgs;
use crate::dbt_project_config::RootProjectConfigs;
use crate::dbt_project_config::init_project_config;
use crate::python_ast::parse_python;
use crate::python_file_info::PythonFileInfo;
use crate::python_validation::validate_python_model;
use crate::python_visitor::analyze_python_file;
use crate::renderer::RenderCtx;
use crate::renderer::RenderCtxInner;
use crate::renderer::SqlFileRenderResult;
use crate::renderer::collect_adapter_identifiers_detect_unsafe;
use crate::renderer::render_unresolved_sql_files;
use crate::utils::RelationComponents;
use crate::utils::get_node_fqn;
use crate::utils::get_original_file_path;
use crate::utils::get_unique_id;
use crate::utils::update_node_relation_components;

use dbt_common::ErrorCode;
use dbt_common::FsResult;
use dbt_common::adapter::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::error::AbstractLocation;
use dbt_common::fs_err;
use dbt_common::io_args::StaticAnalysisKind;
use dbt_common::io_args::StaticAnalysisOffReason;
use dbt_common::static_analysis::{
    StaticAnalysisDeprecationOrigin, check_deprecated_static_analysis_kind,
};
use dbt_common::tokiofs::read_to_string;
use dbt_common::tracing::emit::emit_error_log_from_fs_error;
use dbt_common::tracing::emit::emit_warn_log_from_fs_error;
use dbt_common::tracing::emit::emit_warn_log_message;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use dbt_jinja_utils::listener::JinjaTypeCheckingEventListenerFactory;
use dbt_jinja_utils::node_resolver::NodeResolver;
use dbt_jinja_utils::serde::from_yaml_raw;
use dbt_jinja_utils::utils::dependency_package_name_from_ctx;
use dbt_schemas::schemas::CommonAttributes;
use dbt_schemas::schemas::DbtModel;
use dbt_schemas::schemas::DbtModelAttr;
use dbt_schemas::schemas::InternalDbtNodeAttributes;
use dbt_schemas::schemas::IntrospectionKind;
use dbt_schemas::schemas::NodeBaseAttributes;
use dbt_schemas::schemas::TimeSpine;
use dbt_schemas::schemas::TimeSpinePrimaryColumn;
use dbt_schemas::schemas::common::DbtMaterialization;
use dbt_schemas::schemas::common::DbtQuoting;
use dbt_schemas::schemas::common::ModelFreshnessRules;
use dbt_schemas::schemas::common::NodeDependsOn;
use dbt_schemas::schemas::common::Versions;
use dbt_schemas::schemas::dbt_column::ColumnInheritanceRules;
use dbt_schemas::schemas::dbt_column::ColumnProperties;
use dbt_schemas::schemas::dbt_column::DbtColumnRef;
use dbt_schemas::schemas::dbt_column::process_columns;
use dbt_schemas::schemas::manifest::semantic_model::NodeRelation;
use dbt_schemas::schemas::nodes::AdapterAttr;
use dbt_schemas::schemas::project::DbtProject;
use dbt_schemas::schemas::project::DefaultTo;
use dbt_schemas::schemas::project::ModelConfig;
use dbt_schemas::schemas::project::TypedRecursiveConfig;
use dbt_schemas::schemas::properties::ModelProperties;
use dbt_schemas::schemas::ref_and_source::{DbtRef, DbtSourceWrapper};
use dbt_schemas::state::DbtPackage;
use dbt_schemas::state::DbtRuntimeConfig;
use dbt_schemas::state::GenericTestAsset;
use dbt_schemas::state::ModelStatus;
use dbt_schemas::state::NodeResolverTracker;
use minijinja::MacroSpans;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::resolve_properties::MinimalPropertiesEntry;
use super::resolve_tests::persist_generic_data_tests::TestableNodeTrait;
use super::validate_models::validate_model;

#[derive(serde::Deserialize)]
struct DbtProjectModelsOnly {
    models: Option<dbt_schemas::schemas::project::ProjectModelConfig>,
}

/// Build an *unrendered* project config tree from the raw `dbt_project.yml`.
/// This lets us populate `unrendered_config` for dbt-core compatible state comparisons.
async fn build_raw_model_project_config(
    arg: &ResolveArgs,
    env: &JinjaEnv,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
) -> FsResult<crate::dbt_project_config::DbtProjectConfig<ModelConfig>> {
    /// Build a `DbtProjectConfig` tree without emitting strict-parse errors.
    ///
    /// This is used only for hydrating `unrendered_config` for state comparisons and must
    /// not impact the user-visible parse/compile output. In particular, malformed keys or
    /// Jinja-y values in `dbt_project.yml` should not add extra errors/warnings.
    fn recur_build_dbt_project_config_silent<
        T: DefaultTo<T>,
        S: TypedRecursiveConfig + Into<T> + Clone,
    >(
        parent_config: &T,
        child: &S,
    ) -> crate::dbt_project_config::DbtProjectConfig<T> {
        let mut child_config: T = child.clone().into();
        child_config.default_to(parent_config);

        let mut children = indexmap::IndexMap::new();
        for (key, maybe_child_config_variant) in child.iter_children() {
            let child_config_variant = match maybe_child_config_variant {
                dbt_yaml::ShouldBe::AndIs(config) => config,
                dbt_yaml::ShouldBe::ButIsnt(..) => {
                    // Skip invalid children silently: the fully-rendered dbt_project.yml loader
                    // will report errors. This raw/unrendered pass must not add noise.
                    continue;
                }
            };

            children.insert(
                key.clone(),
                recur_build_dbt_project_config_silent(&child_config, child_config_variant),
            );
        }

        crate::dbt_project_config::DbtProjectConfig {
            config: child_config,
            children,
        }
    }

    let dependency_package_name = dependency_package_name_from_ctx(env, base_ctx);
    let dbt_project_yml_path = package.package_root_path.join("dbt_project.yml");

    // Best-effort read: embedded packages have synthetic paths that don't exist on disk.
    // Check embedded_file_contents first, then fall back to disk.
    let raw_yml = match package
        .embedded_file_contents
        .as_ref()
        .and_then(|m| m.get(Path::new("dbt_project.yml")))
    {
        Some(content) => content.clone(),
        None => match read_to_string(&dbt_project_yml_path).await {
            Ok(content) => content,
            Err(e) => {
                return Err(fs_err!(
                    code => ErrorCode::IoError,
                    loc => dbt_project_yml_path.clone(),
                    "Failed to read {}: {}",
                    dbt_project_yml_path.display(),
                    e
                ));
            }
        },
    };

    // NOTE: This is intentionally best-effort. We use it only to populate `unrendered_config`
    // for state comparisons, and it must not break compilation when the dbt_project.yml contains
    // Jinja-templated values that don't conform to our typed schema (e.g. model-paths).
    //
    // We therefore only deserialize the `models:` subtree and fall back to an empty config tree
    // if parsing fails for any reason (including Jinja).
    let raw_models_only: Option<DbtProjectModelsOnly> = from_yaml_raw::<DbtProjectModelsOnly>(
        &arg.io,
        &raw_yml,
        Some(dbt_project_yml_path.as_path()),
        false,
        dependency_package_name,
    )
    .ok();

    let default_config = ModelConfig {
        enabled: Some(true),
        quoting: Some(package_quoting),
        ..Default::default()
    };

    // Use the parsed model config if available; otherwise build an empty config tree.
    // This must be best-effort and must not emit extra errors.
    Ok(match raw_models_only.and_then(|p| p.models) {
        Some(models_cfg) => recur_build_dbt_project_config_silent(&default_config, &models_cfg),
        None => crate::dbt_project_config::DbtProjectConfig {
            config: default_config,
            children: indexmap::IndexMap::new(),
        },
    })
}

/// Populate `unrendered_config` on a model node for dbt-core compatible `state:*` comparisons.
///
/// This should be called before relation components are rendered/resolved, and it should prefer
/// values from a raw (unrendered) `dbt_project.yml` config tree.
fn insert_unrendered_grants(
    unrendered: &mut BTreeMap<String, dbt_yaml::Value>,
    unrendered_project_cfg: &ModelConfig,
) {
    use dbt_yaml::Value as YmlValue;

    // Config-level unrendered values (e.g. grants) matter for Mantle-compatible `state:modified`
    // semantics. Mantle compares configured/unrendered config, not rendered config.
    if let Some(grants) = unrendered_project_cfg.grants.as_ref() {
        let mut grants_map = dbt_yaml::Mapping::new();
        for (k, v) in &grants.0 {
            let key = YmlValue::String(k.clone(), Default::default());
            let val = match v {
                dbt_schemas::schemas::serde::StringOrArrayOfStrings::String(s) => {
                    YmlValue::String(s.clone(), Default::default())
                }
                dbt_schemas::schemas::serde::StringOrArrayOfStrings::ArrayOfStrings(vec) => {
                    YmlValue::Sequence(
                        vec.iter()
                            .cloned()
                            .map(|s| YmlValue::String(s, Default::default()))
                            .collect(),
                        Default::default(),
                    )
                }
            };
            grants_map.insert(key, val);
        }
        if !grants_map.is_empty() {
            unrendered.insert(
                "grants".to_string(),
                YmlValue::Mapping(grants_map, Default::default()),
            );
        }
    }
}

fn insert_unrendered_hooks(
    unrendered: &mut BTreeMap<String, dbt_yaml::Value>,
    unrendered_project_cfg: &ModelConfig,
) {
    use dbt_yaml::Value as YmlValue;

    // Hooks (`pre-hook` / `post-hook`) show up in dbt-core manifests as lists of SQL strings.
    // Extract unrendered hook SQL strings in a stable representation.
    fn hooks_to_yaml(hooks: &dbt_schemas::schemas::common::Hooks) -> YmlValue {
        match hooks {
            dbt_schemas::schemas::common::Hooks::String(s) => {
                YmlValue::String(s.clone(), Default::default())
            }
            dbt_schemas::schemas::common::Hooks::ArrayOfStrings(v) => YmlValue::Sequence(
                v.iter()
                    .cloned()
                    .map(|s| YmlValue::String(s, Default::default()))
                    .collect(),
                Default::default(),
            ),
            dbt_schemas::schemas::common::Hooks::HookConfig(cfg) => YmlValue::Sequence(
                cfg.sql
                    .iter()
                    .cloned()
                    .map(|s| YmlValue::String(s, Default::default()))
                    .collect(),
                Default::default(),
            ),
            dbt_schemas::schemas::common::Hooks::HookConfigArray(cfgs) => YmlValue::Sequence(
                cfgs.iter()
                    .filter_map(|c| c.sql.clone())
                    .map(|s| YmlValue::String(s, Default::default()))
                    .collect(),
                Default::default(),
            ),
        }
    }

    if let Some(pre) = (*unrendered_project_cfg.pre_hook).as_ref() {
        unrendered.insert("pre-hook".to_string(), hooks_to_yaml(pre));
    }
    if let Some(post) = (*unrendered_project_cfg.post_hook).as_ref() {
        unrendered.insert("post-hook".to_string(), hooks_to_yaml(post));
    }
}

fn insert_unrendered_tags(
    unrendered: &mut BTreeMap<String, dbt_yaml::Value>,
    unrendered_project_cfg: &ModelConfig,
) {
    use dbt_yaml::Value as YmlValue;

    // Tags: store as list if configured as list, or scalar string if configured as string.
    if let Some(tags) = unrendered_project_cfg.tags.as_ref() {
        let yaml = match tags {
            dbt_schemas::schemas::serde::StringOrArrayOfStrings::String(s) => {
                YmlValue::String(s.clone(), Default::default())
            }
            dbt_schemas::schemas::serde::StringOrArrayOfStrings::ArrayOfStrings(v) => {
                YmlValue::Sequence(
                    v.iter()
                        .cloned()
                        .map(|s| YmlValue::String(s, Default::default()))
                        .collect(),
                    Default::default(),
                )
            }
        };
        unrendered.insert("tags".to_string(), yaml);
    }
}

fn insert_unrendered_persist_docs(
    unrendered: &mut BTreeMap<String, dbt_yaml::Value>,
    unrendered_project_cfg: &ModelConfig,
) {
    use dbt_yaml::Value as YmlValue;

    // Persist docs: store the configured/unrendered mapping if present.
    if let Some(pd) = unrendered_project_cfg.persist_docs.as_ref() {
        let mut pd_map = dbt_yaml::Mapping::new();
        if let Some(relation) = pd.relation {
            pd_map.insert(
                YmlValue::String("relation".to_string(), Default::default()),
                YmlValue::Bool(relation, Default::default()),
            );
        }
        if let Some(columns) = pd.columns {
            pd_map.insert(
                YmlValue::String("columns".to_string(), Default::default()),
                YmlValue::Bool(columns, Default::default()),
            );
        }
        if !pd_map.is_empty() {
            unrendered.insert(
                "persist_docs".to_string(),
                YmlValue::Mapping(pd_map, Default::default()),
            );
        }
    }
}

fn set_model_unrendered_relation_config(
    dbt_model: &mut DbtModel,
    raw_local_project_config: &crate::dbt_project_config::DbtProjectConfig<ModelConfig>,
    raw_root_project_models_cfg: Option<&dbt_schemas::schemas::project::ProjectModelConfig>,
    inline_overrides: &RelationComponents,
    components: &RelationComponents,
) {
    use dbt_common::serde_utils::Omissible;
    use dbt_yaml::Value as YmlValue;

    let mut unrendered = BTreeMap::new();

    // Root project config can apply overrides to dependency packages under:
    //   models:
    //     <package_name>:
    //       +schema: ...
    // Those must be reflected in `unrendered_config` for state comparisons.
    fn get_root_model_cfg_for_fqn<'a>(
        root: &'a dbt_schemas::schemas::project::ProjectModelConfig,
        fqn: &[String],
    ) -> &'a dbt_schemas::schemas::project::ProjectModelConfig {
        let mut cur = root;
        for component in fqn {
            let Some(child) = cur.__additional_properties__.get(component) else {
                break;
            };
            let dbt_yaml::ShouldBe::AndIs(cfg) = child else {
                break;
            };
            cur = cfg;
        }
        cur
    }

    fn omissible_opt_str(v: &Omissible<Option<String>>) -> Option<String> {
        match v {
            Omissible::Present(inner) => inner.clone(),
            Omissible::Omitted => None,
        }
    }

    let unrendered_project_cfg =
        raw_local_project_config.get_config_for_fqn(&dbt_model.__common_attr__.fqn);
    let unrendered_root_cfg = raw_root_project_models_cfg
        .map(|root| get_root_model_cfg_for_fqn(root, &dbt_model.__common_attr__.fqn));

    // dbt-core precedence: inline SQL config(...) overrides project-level config.
    //
    // For Mantle-compatible `state:*` comparisons, we want the *effective configured*
    // (unrendered) value, not the rendered target-derived value.
    let unrendered_db = inline_overrides
        .database
        .clone()
        .or_else(|| unrendered_root_cfg.and_then(|cfg| omissible_opt_str(&cfg.database)))
        .or_else(|| {
            unrendered_project_cfg
                .database
                .clone()
                .into_inner()
                .unwrap_or(None)
        })
        .or_else(|| components.database.clone());
    let unrendered_schema = inline_overrides
        .schema
        .clone()
        .or_else(|| unrendered_root_cfg.and_then(|cfg| omissible_opt_str(&cfg.schema)))
        .or_else(|| {
            unrendered_project_cfg
                .schema
                .clone()
                .into_inner()
                .unwrap_or(None)
        })
        .or_else(|| components.schema.clone());
    let unrendered_alias = inline_overrides
        .alias
        .clone()
        .or_else(|| unrendered_root_cfg.and_then(|cfg| cfg.alias.clone()))
        .or_else(|| unrendered_project_cfg.alias.clone())
        .or_else(|| components.alias.clone());

    if let Some(db) = unrendered_db.as_ref() {
        unrendered.insert("database".to_string(), YmlValue::string(db.clone()));
    }
    if let Some(sch) = unrendered_schema.as_ref() {
        unrendered.insert("schema".to_string(), YmlValue::string(sch.clone()));
    }
    if let Some(alias) = unrendered_alias.as_ref() {
        unrendered.insert("alias".to_string(), YmlValue::string(alias.clone()));
    }

    insert_unrendered_grants(&mut unrendered, unrendered_project_cfg);
    insert_unrendered_hooks(&mut unrendered, unrendered_project_cfg);
    insert_unrendered_tags(&mut unrendered, unrendered_project_cfg);
    insert_unrendered_persist_docs(&mut unrendered, unrendered_project_cfg);

    dbt_model.__base_attr__.unrendered_config = unrendered;
}

#[allow(
    clippy::cognitive_complexity,
    clippy::expect_fun_call,
    clippy::too_many_arguments
)]
pub async fn resolve_models(
    arg: &ResolveArgs,
    package: &DbtPackage,
    package_quoting: DbtQuoting,
    root_project: &DbtProject,
    root_project_configs: &RootProjectConfigs,
    models_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    database: &str,
    schema: &str,
    adapter_type: AdapterType,
    package_name: &str,
    env: Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    runtime_config: Arc<DbtRuntimeConfig>,
    collected_generic_tests: &mut Vec<GenericTestAsset>,
    test_name_truncations: &mut HashMap<String, String>,
    node_resolver: &mut NodeResolver,
    token: &CancellationToken,
    jinja_type_checking_event_listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory>,
) -> FsResult<(
    HashMap<String, Arc<DbtModel>>,
    HashMap<String, (String, MacroSpans)>,
    HashMap<String, Arc<DbtModel>>,
)> {
    let mut models: HashMap<String, Arc<DbtModel>> = HashMap::new();
    let mut models_with_execute: HashMap<String, DbtModel> = HashMap::new();
    let mut disabled_models: HashMap<String, Arc<DbtModel>> = HashMap::new();
    let mut node_names = HashSet::new();
    let mut rendering_results: HashMap<String, (String, MacroSpans)> = HashMap::new();
    let dependency_package_name = dependency_package_name_from_ctx(&env, base_ctx);

    let local_project_config = if package.dbt_project.name == root_project.name {
        root_project_configs.models.clone()
    } else {
        init_project_config(
            &arg.io,
            &package.dbt_project.models,
            ModelConfig {
                enabled: Some(true),
                quoting: Some(package_quoting),
                ..Default::default()
            },
            dependency_package_name,
        )?
    };

    let raw_local_project_config =
        build_raw_model_project_config(arg, env.as_ref(), base_ctx, package, package_quoting)
            .await?;
    // Best-effort raw parse of the root project's `models:` subtree, used only to hydrate
    // dependency package nodes' `unrendered_config` with root overrides (preserving Jinja).
    let raw_root_project_models_cfg = if package.dbt_project.name == root_project.name {
        None
    } else {
        let dbt_project_yml_path = arg.io.in_dir.join("dbt_project.yml");
        let raw_yml = read_to_string(&dbt_project_yml_path).await.map_err(|e| {
            fs_err!(
                code => ErrorCode::IoError,
                loc => dbt_project_yml_path.clone(),
                "Failed to read {}: {}",
                dbt_project_yml_path.display(),
                e
            )
        })?;
        from_yaml_raw::<DbtProjectModelsOnly>(
            &arg.io,
            &raw_yml,
            Some(dbt_project_yml_path.as_path()),
            false,
            None,
        )
        .ok()
        .and_then(|p| p.models)
    };

    let render_ctx = RenderCtx {
        inner: Arc::new(RenderCtxInner {
            args: arg.clone(),
            root_project_name: root_project.name.clone(),
            root_project_config: root_project_configs.models.clone(),
            package_quoting,
            base_ctx: base_ctx.clone(),
            package_name: package_name.to_string(),
            adapter_type,
            database: database.to_string(),
            schema: schema.to_string(),
            local_project_config: local_project_config.clone(),
            resource_paths: package
                .dbt_project
                .model_paths
                .as_ref()
                .unwrap_or(&vec![])
                .clone(),
        }),
        jinja_env: env.clone(),
        runtime_config: runtime_config.clone(),
    };

    // HACK: strip semantic resources out of all model properties
    // this is because semantic resources have fields that have jinja expressions
    // but should not be rendered (they are hydrated verbatim in manifest.json)
    //
    // This is a hack because we treat models and models.metrics differently in an attempt
    // for only-once parsing of model yaml properties in resolver.rs, which duplicates the knowledge
    // that you must treat them separately, such as the removal of semantic properties here.
    let mut models_properties_sans_semantics: BTreeMap<String, MinimalPropertiesEntry> =
        BTreeMap::new();
    models_properties.iter().for_each(|(model_key, v)| {
        let mut v = v.clone();
        if let Some(m) = v.schema_value.as_mapping_mut() {
            // NOTE: do not remove derived_semantics not because it has jinja
            // but because we want to report any yaml errors that we didn't
            // show in resolve_inner's parsing of model yaml properties
            m.remove("metrics");
        }

        models_properties_sans_semantics.insert(model_key.clone(), v);
    });

    // Split SQL and Python models for different processing paths
    let (sql_files, python_files): (Vec<_>, Vec<_>) =
        package.model_sql_files.iter().cloned().partition(|asset| {
            asset
                .path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("sql"))
                .unwrap_or(true)
        });

    // Process SQL models through Jinja rendering
    let mut model_sql_resources_map: Vec<SqlFileRenderResult<ModelConfig, ModelProperties>> =
        // FIXME -- this attempts to deserialize the model properties
        // and renders jinja but we shouldn't be doing so with metrics.filter
        render_unresolved_sql_files::<ModelConfig, ModelProperties>(
            &render_ctx,
            &sql_files,
            &mut models_properties_sans_semantics,
            token,
            jinja_type_checking_event_listener_factory.clone(),
        )
        .await?;

    // Process Python models through AST analysis (no Jinja rendering)
    let python_results = process_python_models(
        arg,
        &env,
        base_ctx,
        package_name,
        &package.dbt_project,
        &local_project_config,
        python_files,
        &mut models_properties_sans_semantics,
    )?;
    model_sql_resources_map.extend(python_results);

    // make deterministic
    model_sql_resources_map.sort_by(|a, b| {
        a.asset
            .path
            .file_name()
            .cmp(&b.asset.path.file_name())
            .then(a.asset.path.cmp(&b.asset.path))
    });

    // Initialize a counter struct to track the version of each model
    let mut duplicates = Vec::new();

    for SqlFileRenderResult {
        asset: dbt_asset,
        sql_file_info,
        rendered_sql,
        macro_spans,
        properties: maybe_properties,
        status,
        patch_path,
    } in model_sql_resources_map.into_iter()
    {
        let ref_name = dbt_asset.path.file_stem().unwrap().to_str().unwrap();
        // Is there a better way to handle this if the model doesn't have a config?
        let mut model_config = *sql_file_info.config;
        // Capture inline SQL config overrides (from `{{ config(...) }}`) separately.
        // This should include only values explicitly set in the SQL file, not inherited defaults.
        let inline_overrides = if let Some(explicit) = sql_file_info.explicit_config.as_ref() {
            RelationComponents {
                database: explicit.database.clone().into_inner().unwrap_or(None),
                schema: explicit.schema.clone().into_inner().unwrap_or(None),
                alias: explicit.alias.clone(),
                store_failures: None,
            }
        } else {
            RelationComponents {
                database: None,
                schema: None,
                alias: None,
                store_failures: None,
            }
        };
        // Default to View if no materialized is set
        if model_config.materialized.is_none() {
            model_config.materialized = Some(DbtMaterialization::View);
        }

        // Set to Inline if this is the inline file
        let is_inline_file = package
            .inline_file
            .as_ref()
            .map(|inline_file| inline_file == &dbt_asset)
            .unwrap_or(false);
        if is_inline_file {
            model_config.materialized = Some(DbtMaterialization::Inline);
        }

        let mut model_name = models_properties_sans_semantics
            .get(ref_name)
            .map(|mpe| mpe.name.clone())
            .unwrap_or_else(|| ref_name.to_owned());

        if is_inline_file {
            // Inline nodes should present a stable name for logging and manifest output
            model_name = "inline".to_owned();
        }

        let maybe_version = models_properties_sans_semantics
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.version.clone()));

        let maybe_latest_version = models_properties_sans_semantics
            .get(ref_name)
            .and_then(|mpe| mpe.version_info.as_ref().map(|v| v.latest_version.clone()));

        let unique_id = get_unique_id(&model_name, package_name, maybe_version.clone(), "model");

        model_config.enabled = Some(!(status == ModelStatus::Disabled));

        if let Some(freshness) = &model_config.freshness {
            ModelFreshnessRules::validate(freshness.build_after.as_ref()).map_err(|e| {
                fs_err!(
                    code => ErrorCode::InvalidConfig,
                    loc => dbt_asset.path.clone(),
                    "{}",
                    e
                )
            })?;
        }

        // Keep track of duplicates (often happens with versioned models)
        if (models.contains_key(&unique_id) || models_with_execute.contains_key(&unique_id))
            && !(status == ModelStatus::Disabled)
        {
            duplicates.push((
                unique_id.clone(),
                model_name.clone(),
                maybe_version.clone(),
                dbt_asset.path.clone(),
            ));
            continue;
        }

        let original_file_path =
            get_original_file_path(&dbt_asset.base_path, &arg.io.in_dir, &dbt_asset.path);

        // Model fqn includes v{version} for versioned models
        let fqn_components = if let Some(version) = &maybe_version {
            vec![model_name.to_owned(), format!("v{}", version)]
        } else {
            vec![model_name.to_owned()]
        };
        let fqn = get_node_fqn(
            package_name,
            dbt_asset.path.to_owned(),
            fqn_components,
            package.dbt_project.model_paths.as_ref().unwrap_or(&vec![]),
        );

        let properties = if let Some(properties) = maybe_properties {
            properties
        } else {
            ModelProperties::empty(model_name.to_owned())
        };

        // Validate model properties (versions, time spine, etc.)
        match validate_model(&properties) {
            Ok(errors) => {
                if !errors.is_empty() {
                    // Show each error individually
                    for error in errors {
                        emit_error_log_from_fs_error(&error, arg.io.status_reporter.as_ref());
                    }
                    continue;
                }
            }
            Err(e) => {
                emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());

                continue;
            }
        }

        let model_constraints = properties.constraints.clone().unwrap_or_default();

        // Iterate over metrics and construct the dependencies
        let mut metrics = Vec::new();
        for (metric, package) in sql_file_info.metrics.iter() {
            if let Some(package_str) = package {
                metrics.push(vec![package_str.to_owned(), metric.to_owned()]);
            } else {
                metrics.push(vec![metric.to_owned()]);
            }
        }

        let mut columns = process_columns(
            properties.columns.as_ref(),
            model_config.meta.clone(),
            model_config.tags.clone().map(|tags| tags.into()),
        )?;

        if let Some(versions) = &properties.versions {
            columns = process_versioned_columns(
                &model_config,
                maybe_version.as_ref(),
                versions,
                columns,
            )?;
        }

        validate_merge_update_columns_xor(&model_config, &dbt_asset.path)?;

        if let Some(freshness) = &model_config.freshness {
            ModelFreshnessRules::validate(freshness.build_after.as_ref())?;
        }

        let static_analysis = if let Some(static_analysis) = model_config.static_analysis.clone() {
            check_deprecated_static_analysis_kind(
                static_analysis.clone().into_inner(),
                StaticAnalysisDeprecationOrigin::NodeConfig {
                    unique_id: unique_id.as_str(),
                },
                dependency_package_name,
                arg.io.status_reporter.as_ref(),
            );
            static_analysis
        } else {
            StaticAnalysisKind::Strict.into()
        };

        // Hydrate time_spine from model properties
        let mut time_spine: Option<TimeSpine> = None;
        if let Some(props_time_spine) = properties.time_spine.clone() {
            let standard_granularity_column_dimension = properties.columns.clone().unwrap_or_default()
                .into_iter()
                .find(|d| {
                    d.name == props_time_spine.standard_granularity_column.clone()
                }).expect(&format!("Cannot find standard granularity column '{}'. There should have been a validation error.", props_time_spine.standard_granularity_column));

            let primary_column = TimeSpinePrimaryColumn {
                name: props_time_spine.standard_granularity_column.clone(),
                time_granularity: standard_granularity_column_dimension
                    .granularity
                    .unwrap_or_default(),
            };

            // Create a temporary node_relation for the time_spine
            let node_relation = NodeRelation {
                database: Some(database.to_string()),
                schema_name: schema.to_string(),
                alias: model_name.to_string(), // will be updated after relation components are resolved
                relation_name: None,
            };

            time_spine = Some(TimeSpine {
                node_relation,
                primary_column,
                custom_granularities: props_time_spine.custom_granularities.unwrap_or_default(),
            });
        }

        jinja_type_checking_event_listener_factory
            .update_unique_id(&format!("{package_name}.{model_name}"), &unique_id);

        // Create the DbtModel with all properties already set
        let mut dbt_model = DbtModel {
            __common_attr__: CommonAttributes {
                name: model_name.to_owned(),
                package_name: package_name.to_owned(),
                path: dbt_asset.path.to_owned(),
                name_span: dbt_common::Span::default(),
                original_file_path,
                patch_path: patch_path.clone(),
                unique_id: unique_id.clone(),
                fqn,
                // dbt-core: description is always default ''
                description: Some(properties.description.clone().unwrap_or_default()),
                checksum: sql_file_info.checksum.clone(),
                // NOTE: raw_code has to be this value for dbt-evaluator to return truthy
                // hydrating it with get_original_file_contents would actually break dbt-evaluator
                raw_code: Some("--placeholder--".to_string()),
                language: if is_python_model(&dbt_asset) {
                    Some("python".to_string())
                } else {
                    Some("sql".to_string())
                },
                tags: model_config
                    .tags
                    .clone()
                    .map(|tags| tags.into())
                    .unwrap_or_default(),
                meta: model_config.meta.clone().unwrap_or_default(),
            },
            __base_attr__: NodeBaseAttributes {
                database: database.to_string(), // will be updated below
                schema: schema.to_string(),     // will be updated below
                alias: "".to_owned(),           // will be updated below
                relation_name: None,            // will be updated below
                enabled: model_config.enabled.unwrap_or(true),
                extended_model: false,
                persist_docs: model_config.persist_docs.clone(),
                columns,
                depends_on: NodeDependsOn {
                    macros: vec![],
                    nodes: vec![],
                    nodes_with_ref_location: vec![],
                },
                refs: sql_file_info
                    .refs
                    .iter()
                    .map(|(model, project, version, location)| DbtRef {
                        name: model.to_owned(),
                        package: project.to_owned(),
                        version: version.clone().map(|v| v.into()),
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                functions: sql_file_info
                    .functions
                    .iter()
                    .map(|(function_name, package, location)| DbtRef {
                        name: function_name.to_owned(),
                        package: package.to_owned(),
                        version: None, // Functions don't have versions
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                sources: sql_file_info
                    .sources
                    .iter()
                    .map(|(source, table, location)| DbtSourceWrapper {
                        source: vec![source.to_owned(), table.to_owned()],
                        location: Some(location.with_file(&dbt_asset.path)),
                    })
                    .collect(),
                metrics,
                materialized: model_config
                    .materialized
                    .clone()
                    .expect("materialized is required"),
                quoting: model_config
                    .quoting
                    .expect("quoting is required")
                    .try_into()
                    .expect("quoting is required"),
                quoting_ignore_case: model_config
                    .quoting
                    .unwrap_or_default()
                    .snowflake_ignore_case
                    .unwrap_or(false),
                static_analysis_off_reason: (static_analysis.clone().into_inner()
                    == StaticAnalysisKind::Off)
                    .then_some(StaticAnalysisOffReason::ConfiguredOff),
                static_analysis,
                unrendered_config: Default::default(),
            },
            __model_attr__: DbtModelAttr {
                introspection: if sql_file_info.this {
                    IntrospectionKind::This
                } else {
                    IntrospectionKind::None
                },
                version: maybe_version.map(|v| v.into()),
                latest_version: maybe_latest_version.map(|v| v.into()),
                constraints: model_constraints,
                deprecation_date: properties.deprecation_date.clone(),
                primary_key: vec![], // applied in resolver.rs -> primary_key_inference.rs
                time_spine,
                access: model_config.access.clone().unwrap_or_default(),
                group: model_config.group.clone(),
                contract: model_config.contract.clone(),
                incremental_strategy: model_config.incremental_strategy.clone(),
                freshness: model_config.freshness.clone(),
                event_time: model_config.event_time.clone(),
                catalog_name: model_config.catalog_name.clone(),
                table_format: model_config.table_format.clone(),
                sync: model_config.sync.clone(),
            },
            __adapter_attr__: AdapterAttr::from_config_and_dialect(
                &model_config.__warehouse_specific_config__,
                adapter_type,
            ),
            // Derived from the model config
            deprecated_config: model_config.clone(),
            __other__: BTreeMap::new(),
        };

        let components = RelationComponents {
            database: model_config.database.into_inner().unwrap_or(None),
            schema: model_config.schema.into_inner().unwrap_or(None),
            alias: model_config.alias.clone(),
            store_failures: None,
        };

        // Record unrendered (configured) values for dbt-core compatible state comparisons.
        // This must happen *before* we render/resolve target-derived components.
        set_model_unrendered_relation_config(
            &mut dbt_model,
            &raw_local_project_config,
            raw_root_project_models_cfg.as_ref(),
            &inline_overrides,
            &components,
        );

        // update model components using the generate_relation_components function
        update_node_relation_components(
            &mut dbt_model,
            &env,
            &root_project.name,
            package_name,
            base_ctx,
            &components,
            adapter_type,
        )?;

        // Update time_spine node_relation with the resolved relation components
        if dbt_model.__model_attr__.time_spine.is_some() {
            let database = dbt_model.database();
            let schema = dbt_model.schema();
            let alias = dbt_model.alias();
            let relation_name = dbt_model.__base_attr__.relation_name.clone();

            if let Some(ref mut ts) = dbt_model.__model_attr__.time_spine {
                ts.node_relation = NodeRelation {
                    database: Some(database),
                    schema_name: schema,
                    alias,
                    relation_name,
                };
            }
        }
        match node_resolver.insert_ref(&dbt_model, adapter_type, status, false) {
            Ok(_) => (),
            Err(e) => {
                let err_with_loc = e.with_location(dbt_asset.path.clone());
                emit_error_log_from_fs_error(&err_with_loc, arg.io.status_reporter.as_ref());
            }
        }

        match status {
            ModelStatus::Enabled => {
                // merge them later for the returned models
                if sql_file_info.execute {
                    models_with_execute.insert(unique_id.to_owned(), dbt_model);
                } else {
                    models.insert(unique_id.to_owned(), Arc::new(dbt_model));
                }
                node_names.insert(model_name.to_owned());
                rendering_results.insert(unique_id, (rendered_sql.clone(), macro_spans.clone()));

                properties.as_testable().persist(
                    package_name,
                    &root_project.name,
                    collected_generic_tests,
                    test_name_truncations,
                    adapter_type,
                    &arg.io,
                    patch_path.as_ref().unwrap_or(&dbt_asset.path),
                )?;
            }
            ModelStatus::Disabled => {
                disabled_models.insert(unique_id.to_owned(), Arc::new(dbt_model));
            }
            ModelStatus::ParsingFailed => {}
        }
    }

    for (model_name, mpe) in models_properties_sans_semantics.iter() {
        // Skip until we support better error messages for versioned models
        if mpe.version_info.is_some() {
            continue;
        }
        if !mpe.schema_value.is_null() {
            // Validate that the model is not latest and flattened
            let err = fs_err!(
                code =>ErrorCode::InvalidConfig,
                loc => mpe.relative_path.clone(),
                "Unused schema.yml entry for model '{}'",
                model_name,
            );
            emit_warn_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
        }
    }

    // Report duplicates
    if !duplicates.is_empty() {
        let mut errs = Vec::new();
        for (_, model_name, maybe_version, path) in duplicates {
            let msg = if let Some(version) = maybe_version {
                format!("Found duplicate model '{model_name}' with version '{version}'")
            } else {
                format!("Found duplicate model '{model_name}'")
            };
            let err = fs_err!(
                code => ErrorCode::InvalidConfig,
                loc => path.clone(),
                "{}",
                msg,
            );
            errs.push(err);
        }
        while let Some(err) = errs.pop() {
            if errs.is_empty() {
                return Err(err);
            }
            emit_error_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
        }
    }

    // Second pass to capture all identifiers with the appropriate context
    // `models_with_execute` should never have overlapping Arc pointers with `models` and `disabled_models`
    // otherwise make_mut will clone the inner model, and the modifications inside this function call will be lost
    let models_rest = collect_adapter_identifiers_detect_unsafe(
        arg,
        models_with_execute,
        node_resolver,
        env,
        adapter_type,
        package_name,
        &root_project.name,
        runtime_config,
        token,
    )
    .await?;

    models.extend(
        models_rest
            .into_iter()
            .map(|(v, _)| (v.__common_attr__.unique_id.to_string(), Arc::new(v))),
    );
    Ok((models, rendering_results, disabled_models))
}

fn process_versioned_columns(
    model_config: &ModelConfig,
    maybe_version: Option<&String>,
    versions: &[Versions],
    columns: Vec<DbtColumnRef>,
) -> Result<Vec<DbtColumnRef>, Box<dbt_common::FsError>> {
    for version in versions.iter() {
        if maybe_version.is_some_and(|v| Some(v) == version.get_version().as_ref())
            && let Some(column_props) = version.__additional_properties__.get("columns")
        {
            let column_map: Vec<ColumnProperties> = column_props
                .as_sequence()
                .map(|cols| {
                    cols.iter()
                        .filter_map(|col| col.as_mapping())
                        .filter(|map| !(map.contains_key("include") || map.contains_key("exclude")))
                        .filter_map(|map| {
                            dbt_yaml::from_value::<ColumnProperties>(map.clone().into()).ok()
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut versioned_columns = process_columns(
                Some(&column_map),
                model_config.meta.clone(),
                model_config.tags.clone().map(|tags| tags.into()),
            )?;

            if let Some(rules) = ColumnInheritanceRules::from_version_columns(column_props) {
                columns
                    .iter()
                    .filter(|col| rules.should_include_column(&col.name))
                    .for_each(|col| {
                        versioned_columns.push(col.clone());
                    });
            }
            return Ok(versioned_columns);
        }
    }

    Ok(columns)
}

pub fn validate_merge_update_columns_xor(model_config: &ModelConfig, path: &Path) -> FsResult<()> {
    if model_config.merge_update_columns.is_some() && model_config.merge_exclude_columns.is_some() {
        let err = fs_err!(
            code => ErrorCode::InvalidConfig,
            loc => path.to_path_buf(),
            "merge_update_columns and merge_exclude_columns cannot both be set",
        );
        return Err(err);
    }
    Ok(())
}

/// Process Python model files through AST analysis
///
/// Unlike SQL models which go through Jinja rendering, Python models are:
/// 1. Parsed with a Python AST parser
/// 2. Validated for correct structure (model function signature)
/// 3. Analyzed to extract dbt.ref(), dbt.source(), dbt.config() calls
/// 4. Merged with project/properties configs
///
/// Returns SqlFileRenderResult for uniform downstream processing with SQL models
#[allow(clippy::too_many_arguments)]
fn process_python_models(
    arg: &ResolveArgs,
    env: &Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    package_name: &str,
    dbt_project: &DbtProject,
    local_project_config: &crate::dbt_project_config::DbtProjectConfig<ModelConfig>,
    python_files: Vec<dbt_schemas::state::DbtAsset>,
    models_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
) -> FsResult<Vec<SqlFileRenderResult<ModelConfig, ModelProperties>>> {
    let mut results = Vec::new();
    let dependency_package_name = dependency_package_name_from_ctx(env.as_ref(), base_ctx);

    for python_asset in python_files {
        // Read and parse Python source
        let absolute_path = python_asset.base_path.join(&python_asset.path);
        // Strip leading/trailing whitespace to match dbt-core's behavior (load_file_contents with strip=True)
        let source = std::fs::read_to_string(&absolute_path)?.trim().to_string();

        let stmts = match parse_python(&source, &python_asset.path) {
            Ok(stmts) => stmts,
            Err(e) => {
                emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());
                continue;
            }
        };

        // Validate Python model structure (def model(dbt, session): ...)
        if let Err(e) = validate_python_model(&python_asset.path, &stmts) {
            emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());
            continue;
        }

        // Analyze Python AST to extract dbt function calls
        let checksum = dbt_schemas::schemas::common::DbtChecksum::default(); // TODO: compute actual checksum
        let python_file_info: PythonFileInfo<ModelConfig> = match analyze_python_file(
            &python_asset.path,
            &source,
            &stmts,
            checksum,
            &arg.io,
            dependency_package_name,
            Some(python_asset.path.clone()),
        ) {
            Ok(info) => info,
            Err(e) => {
                emit_error_log_from_fs_error(&e, arg.io.status_reporter.as_ref());
                continue;
            }
        };

        // Extract and parse properties from YAML if they exist
        let ref_name = python_asset.path.file_stem().unwrap().to_str().unwrap();
        let (maybe_properties, patch_path) =
            extract_model_properties(arg, env, base_ctx, models_properties, ref_name)?;

        // Merge Python model config with project config and schema.yml properties
        let merged_config = match merge_python_config(
            &python_file_info,
            &python_asset,
            package_name,
            dbt_project,
            local_project_config,
            maybe_properties.as_ref(),
            arg,
        ) {
            Ok(config) => config,
            Err(err) => {
                emit_error_log_from_fs_error(&err, arg.io.status_reporter.as_ref());
                continue;
            }
        };

        // Convert to SqlFileRenderResult for uniform downstream processing
        let python_result = SqlFileRenderResult {
            asset: python_asset.clone(),
            sql_file_info: crate::sql_file_info::SqlFileInfo {
                sources: python_file_info.sources,
                refs: python_file_info.refs,
                this: false,
                metrics: vec![],
                config: Box::new(merged_config),
                explicit_config: None,
                tests: vec![],
                macros: vec![],
                materializations: vec![],
                docs: vec![],
                snapshots: vec![],
                functions: vec![],
                checksum: python_file_info.checksum,
                execute: false,
            },
            rendered_sql: source.clone(),
            macro_spans: Default::default(),
            properties: maybe_properties,
            status: ModelStatus::Enabled,
            patch_path,
        };

        results.push(python_result);
    }

    Ok(results)
}

/// Extract model properties from YAML schema files
///
/// Consumes the schema_value from models_properties to mark it as "used"
/// and prevent "Unused schema.yml entry" warnings
fn extract_model_properties(
    arg: &ResolveArgs,
    env: &Arc<JinjaEnv>,
    base_ctx: &BTreeMap<String, minijinja::Value>,
    models_properties: &mut BTreeMap<String, MinimalPropertiesEntry>,
    ref_name: &str,
) -> FsResult<(Option<ModelProperties>, Option<PathBuf>)> {
    if let Some(mpe) = models_properties.get_mut(ref_name)
        && !mpe.schema_value.is_null()
    {
        // Consume the schema_value by replacing it with null
        // This marks the entry as "used" to prevent unused warnings
        let schema_value = std::mem::replace(&mut mpe.schema_value, dbt_yaml::Value::null());
        let properties = dbt_jinja_utils::serde::into_typed_with_jinja::<ModelProperties, _>(
            &arg.io,
            schema_value,
            false,
            env,
            base_ctx,
            &[],
            dependency_package_name_from_ctx(env, base_ctx),
            true,
        )?;
        return Ok((Some(properties), Some(mpe.relative_path.clone())));
    }
    Ok((None, None))
}

/// Warn when config.get() accesses keys that exist in config.meta
fn check_config_get_on_meta_keys(
    python_file_info: &PythonFileInfo<ModelConfig>,
    config: &ModelConfig,
    path: &Path,
    arg: &ResolveArgs,
) -> FsResult<()> {
    if let Some(meta) = &config.meta {
        for key in &python_file_info.config_keys_used {
            if meta.contains_key(key) {
                let warning = fs_err!(
                    code => ErrorCode::Generic,
                    loc => path.to_path_buf(),
                    "The key '{}' was accessed using dbt.config.get('{}'), \
                     but was detected as a custom config under 'meta'. \
                     Please use dbt.config.meta_get('{}') instead of dbt.config.get('{}') \
                     to access the custom config value.",
                    key, key, key, key
                );
                emit_warn_log_from_fs_error(&warning, arg.io.status_reporter.as_ref());
            }
        }
    }
    Ok(())
}

/// Merge Python model config with project config and schema.yml properties
///
/// Python models collect config from dbt.config() calls during AST analysis.
/// These need to be merged with:
/// 1. Project-level config (from dbt_project.yml)
/// 2. Schema.yml properties config (if present)
fn merge_python_config(
    python_file_info: &PythonFileInfo<ModelConfig>,
    python_asset: &dbt_schemas::state::DbtAsset,
    package_name: &str,
    dbt_project: &DbtProject,
    local_project_config: &crate::dbt_project_config::DbtProjectConfig<ModelConfig>,
    maybe_properties: Option<&ModelProperties>,
    arg: &ResolveArgs,
) -> FsResult<ModelConfig> {
    let model_name = python_asset
        .path
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let fqn = get_node_fqn(
        package_name,
        python_asset.path.clone(),
        vec![model_name],
        dbt_project.model_paths.as_ref().unwrap_or(&vec![]),
    );

    // Config precedence (highest to lowest):
    // 1. dbt.config() in Python file
    // 2. config: in schema.yml
    // 3. dbt_project.yml
    //
    // Build up config from lowest to highest priority, matching SQL model behavior
    // where later configs override earlier ones (see sql_file_info.rs:85-88)

    let project_config = local_project_config.get_config_for_fqn(&fqn);

    // Start with project config (lowest priority)
    let mut merged_config = project_config.clone();

    // Apply schema.yml config on top (medium priority)
    // For Python models, we always create a config layer with materialized="table" as default
    // See https://github.com/dbt-labs/dbt-core/blob/34bb3f94dde716a3f9c36481d2ead85c211075dd/core/dbt/parser/base.py#L338
    // This ensures the default overrides project config but can be overridden by Python file config
    let mut properties_config = if let Some(properties) = maybe_properties {
        // Schema.yml exists - use its config if present, otherwise create default
        properties.config.clone().unwrap_or_default()
    } else {
        // No schema.yml - create default config
        ModelConfig::default()
    };

    // Set default materialized if not specified
    if properties_config.materialized.is_none() {
        properties_config.materialized = Some(DbtMaterialization::Table);
    }

    // Merge with project config (properties_config overrides project_config)
    properties_config.default_to(&merged_config);
    merged_config = properties_config;

    // Apply Python file config on top (highest priority)
    let mut python_config = *python_file_info.config.clone();
    python_config.default_to(&merged_config);
    merged_config = python_config;

    // Transfer Python-specific config key tracking from PythonFileInfo to merged config
    // These fields should come from the Python file analysis, not from project/schema configs
    if !python_file_info.config_keys_used.is_empty() {
        merged_config.config_keys_used = Some(python_file_info.config_keys_used.clone());
        merged_config.config_keys_defaults = Some(python_file_info.config_keys_defaults.clone());
    }

    // Transfer meta key tracking
    if !python_file_info.meta_keys_used.is_empty() {
        merged_config.meta_keys_used = Some(python_file_info.meta_keys_used.clone());
        merged_config.meta_keys_defaults = Some(python_file_info.meta_keys_defaults.clone());
    }

    // Warn if config.get() used on meta keys
    check_config_get_on_meta_keys(python_file_info, &merged_config, &python_asset.path, arg)?;

    // Warn if user explicitly enabled static_analysis for a Python model
    // This check happens after all config sources are merged
    if merged_config.static_analysis == Some(StaticAnalysisKind::On.into())
        || merged_config.static_analysis == Some(StaticAnalysisKind::Strict.into())
    {
        emit_warn_log_message(
            ErrorCode::InvalidConfig,
            format!(
                "Python model '{}' has static_analysis set to 'on', but static analysis is not supported for Python models. Setting will be ignored.",
                python_asset.path.display()
            ),
            arg.io.status_reporter.as_ref(),
        );
    }

    if let Some(ref mat) = merged_config.materialized {
        if *mat != DbtMaterialization::Table && *mat != DbtMaterialization::Incremental {
            let err = fs_err!(
                code => ErrorCode::InvalidConfig,
                loc => python_asset.path.to_path_buf(),
                "Invalid materialization '{}' for Python model. Only 'table' or 'incremental' are allowed.",
                mat,
            );
            return Err(err);
        }
    } else {
        merged_config.materialized = Some(DbtMaterialization::Table);
    }

    // Python models always have static_analysis turned off
    // SQL analysis is not applicable to Python code
    merged_config.static_analysis = Some(StaticAnalysisKind::Off.into());

    Ok(merged_config)
}

/// Determine if a DbtAsset is a Python model based on file extension
fn is_python_model(asset: &dbt_schemas::state::DbtAsset) -> bool {
    asset
        .path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "py")
        .unwrap_or(false)
}
