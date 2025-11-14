# Configuration System

<details>
<summary>Relevant source files</summary>

The following files were used as context for generating this wiki page:

- [.changes/unreleased/Features-20250617-142516.yaml](https://github.com/dbt-labs/dbt-core/blob/64b58ec6/.changes/unreleased/Features-20250617-142516.yaml)

</details>



## Purpose and Scope

The Configuration System manages all project-level configuration in dbt-core, including schema validation, configuration parsing, and the hierarchical resolution of settings across multiple sources. This system processes `dbt_project.yml` files, validates configurations against JSON schemas, and provides a unified interface for accessing configuration data throughout the dbt execution pipeline.

For information about project-specific configuration schemas and catalog integration, see [Project Configuration and Schema](#5.1). For details about hierarchical configuration parsing and nested configuration management, see [Hierarchical Configuration Parsing](#5.2).

## Configuration Architecture Overview

The Configuration System serves as the foundation for all dbt operations by managing how configuration data flows from various sources into the execution environment. It handles validation, parsing, and resolution of configuration conflicts across multiple hierarchical levels.

```mermaid
graph TB
    subgraph "Configuration Sources"
        dbt_project["dbt_project.yml"]
        env_vars["Environment Variables"]
        cli_flags["CLI Flags"]
        profiles["profiles.yml"]
    end
    
    subgraph "Configuration Processing"
        schema_validator["JSON Schema Validator"]
        config_parser["Configuration Parser"]
        hierarchical_resolver["Hierarchical Resolver"]
    end
    
    subgraph "Configuration Storage"
        project_config["Project Configuration"]
        runtime_config["Runtime Configuration"]
        validation_cache["Validation Cache"]
    end
    
    subgraph "Consumer Systems"
        model_processor["Model Processing"]
        source_processor["Source Processing"]
        test_processor["Test Processing"]
        cli_interface["CLI Interface"]
    end
    
    dbt_project --> schema_validator
    env_vars --> config_parser
    cli_flags --> hierarchical_resolver
    profiles --> config_parser
    
    schema_validator --> project_config
    config_parser --> runtime_config
    hierarchical_resolver --> validation_cache
    
    project_config --> model_processor
    runtime_config --> source_processor
    validation_cache --> test_processor
    project_config --> cli_interface
```

Sources: `.changes/unreleased/Features-20250617-142516.yaml`

## Configuration Types and Schema Validation

The system manages multiple configuration types through a comprehensive JSON schema validation framework. Each configuration type has specific validation rules and processing requirements.

```mermaid
graph LR
    subgraph "Schema Types"
        project_schema["dbt_project.yml Schema"]
        model_schema["Model Config Schema"]
        source_schema["Source Config Schema"]
        test_schema["Test Properties Schema"]
        exposure_schema["Exposure Config Schema"]
    end
    
    subgraph "Validation Engine"
        json_validator["JSONSchemaValidator"]
        builtin_validator["BuiltinDataTestValidator"]
        deprecation_validator["DeprecationValidator"]
    end
    
    subgraph "Configuration Objects"
        ProjectConfig["ProjectConfig"]
        ModelConfig["ModelConfig"]
        SourceConfig["SourceConfig"]
        TestConfig["TestConfig"]
        ExposureConfig["ExposureConfig"]
    end
    
    project_schema --> json_validator
    model_schema --> builtin_validator
    source_schema --> json_validator
    test_schema --> builtin_validator
    exposure_schema --> deprecation_validator
    
    json_validator --> ProjectConfig
    builtin_validator --> ModelConfig
    json_validator --> SourceConfig
    builtin_validator --> TestConfig
    deprecation_validator --> ExposureConfig
```

Sources: `.changes/unreleased/Features-20250617-142516.yaml`

## Configuration Processing Pipeline

The configuration processing follows a structured pipeline that validates, parses, and resolves configuration data from multiple sources with proper precedence handling.

| Processing Stage | Input Sources | Validation Type | Output |
|-----------------|---------------|-----------------|---------|
| Schema Validation | `dbt_project.yml`, model configs | JSON Schema | Validated config objects |
| Hierarchical Resolution | CLI flags, env vars, project configs | Precedence rules | Merged configuration |
| Runtime Application | Merged config, context data | Runtime validation | Active configuration |
| Deprecation Checking | All config sources | Deprecation rules | Warning notifications |

```mermaid
flowchart TD
    input_stage["Configuration Input Stage"]
    
    subgraph "Input Processing"
        read_project["Read dbt_project.yml"]
        parse_env["Parse Environment Variables"]
        extract_cli["Extract CLI Arguments"]
    end
    
    subgraph "Validation Stage"
        schema_check["JSON Schema Validation"]
        builtin_check["Builtin Data Test Validation"]
        exposure_check["Exposure Config Validation"]
    end
    
    subgraph "Resolution Stage"
        hierarchy_merge["Hierarchical Merge"]
        conflict_resolve["Conflict Resolution"]
        default_apply["Apply Defaults"]
    end
    
    subgraph "Output Stage"
        runtime_config["Runtime Configuration"]
        validation_errors["Validation Errors"]
        deprecation_warnings["Deprecation Warnings"]
    end
    
    input_stage --> read_project
    input_stage --> parse_env
    input_stage --> extract_cli
    
    read_project --> schema_check
    parse_env --> builtin_check
    extract_cli --> exposure_check
    
    schema_check --> hierarchy_merge
    builtin_check --> conflict_resolve
    exposure_check --> default_apply
    
    hierarchy_merge --> runtime_config
    conflict_resolve --> validation_errors
    default_apply --> deprecation_warnings
```

Sources: `.changes/unreleased/Features-20250617-142516.yaml`

## Configuration Precedence and Hierarchical Resolution

The system implements a multi-level precedence system where configuration values are resolved based on their source priority and specificity.

### Configuration Precedence Order

1. **CLI Flags** - Highest precedence, runtime-specific
2. **Environment Variables** - System-level overrides
3. **Project Configuration** - Project-specific settings in `dbt_project.yml`
4. **Default Values** - Built-in system defaults

### Hierarchical Configuration Structure

```mermaid
graph TB
    subgraph "Global Level"
        global_defaults["Global Defaults"]
        system_env["System Environment"]
    end
    
    subgraph "Project Level"
        project_yml["dbt_project.yml"]
        project_env["Project Environment Variables"]
    end
    
    subgraph "Model Level"
        model_config["Model-specific Config"]
        model_overrides["Model Overrides"]
    end
    
    subgraph "Runtime Level"
        cli_overrides["CLI Flag Overrides"]
        runtime_context["Runtime Context"]
    end
    
    global_defaults --> project_yml
    system_env --> project_env
    project_yml --> model_config
    project_env --> model_overrides
    model_config --> cli_overrides
    model_overrides --> runtime_context
```

Sources: `.changes/unreleased/Features-20250617-142516.yaml`

## JSON Schema Integration and Validation

The Configuration System integrates deeply with JSON Schema validation to ensure configuration accuracy and provide meaningful error messages for invalid configurations.

### Schema Validation Components

- **Built-in Data Test Properties**: Validates test configuration properties against schema definitions
- **Exposure Configuration Validation**: Ensures exposure configurations conform to expected structure
- **Deprecation-Aware Validation**: Provides warnings for deprecated configuration patterns

### Validation Error Handling

The system provides structured validation error reporting that includes:
- Schema violation details
- Suggested corrections
- Deprecation warnings with migration guidance
- Context information for debugging

```mermaid
graph LR
    subgraph "Schema Sources"
        builtin_schemas["Built-in Schemas"]
        custom_schemas["Custom Schemas"]
        adapter_schemas["Adapter Schemas"]
    end
    
    subgraph "Validation Process"
        schema_loader["Schema Loader"]
        validation_engine["Validation Engine"]
        error_formatter["Error Formatter"]
    end
    
    subgraph "Validation Results"
        valid_config["Valid Configuration"]
        validation_errors["Validation Errors"]
        deprecation_warnings["Deprecation Warnings"]
    end
    
    builtin_schemas --> schema_loader
    custom_schemas --> schema_loader
    adapter_schemas --> validation_engine
    
    schema_loader --> validation_engine
    validation_engine --> error_formatter
    
    error_formatter --> valid_config
    error_formatter --> validation_errors
    error_formatter --> deprecation_warnings
```

Sources: `.changes/unreleased/Features-20250617-142516.yaml`

## Integration with Other Systems

The Configuration System serves as a central hub that provides configuration data to all other major systems in dbt-core.

### System Integration Points

| Consumer System | Configuration Data Used | Integration Method |
|----------------|------------------------|-------------------|
| Model Processing | Model configs, project settings | Direct config object access |
| Source Processing | Source configs, freshness settings | Configuration injection |
| Test Processing | Test properties, validation rules | Schema-validated configs |
| CLI Interface | Runtime flags, environment settings | Hierarchical resolution |

### Configuration Distribution Pattern

The system uses a centralized configuration distribution pattern where validated configuration objects are passed to consuming systems rather than having each system parse configuration independently.