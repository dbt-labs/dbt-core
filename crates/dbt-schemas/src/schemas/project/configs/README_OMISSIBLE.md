# Omissible Field Handling Guide

This guide explains how to use the omissible utility functions to implement hierarchical configuration overrides with proper null handling in dbt configurations.

## Overview

The `Omissible<T>` type distinguishes between:
- **`Omitted`**: Field not specified in the configuration
- **`Present(None)`**: Field explicitly set to null (e.g., `+schema: null`)
- **`Present(Some(value))`**: Field explicitly set to a value

This distinction is crucial for hierarchical configurations where explicit null values should override parent configurations.

## Quick Start

### 1. Add Omissible Fields to Your Config

```rust
use dbt_common::serde_utils::Omissible;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MyConfig {
    // Omissible fields for hierarchical override support
    pub schema: Omissible<Option<String>>,
    pub database: Omissible<Option<String>>,
    pub catalog: Omissible<Option<String>>,
    
    // Regular fields
    pub enabled: Option<bool>,
    pub description: Option<String>,
}
```

### 2. Add Accessor Methods

```rust
impl MyConfig {
    fn schema(&self) -> Option<String> {
        self.schema.into_inner().unwrap_or(None)
    }
    
    fn database(&self) -> Option<String> {
        self.database.into_inner().unwrap_or(None)
    }
}
```

### 3. Update Your DefaultTo Implementation

```rust
impl DefaultTo<MyConfig> for MyConfig {
    fn default_to(&mut self, parent: &MyConfig) {
        // Handle Omissible fields with hierarchical override logic
        handle_omissible_override(
            &mut self.schema,
            &parent.schema
        );
        handle_omissible_override(
            &mut self.database,
            &parent.database
        );
        
        // Handle regular fields normally
        if self.enabled.is_none() {
            self.enabled = parent.enabled;
        }
        if self.description.is_none() {
            self.description = parent.description.clone();
        }
    }
    
    // ... other required methods
}
```

## Behavior Rules

The `handle_omissible_override` function implements these rules:

1. **Child Omitted**: Inherits parent's value (whether Omitted, None, or Some)
2. **Child Present(None)**: Keeps its null value, overriding parent
3. **Child Present(Some)**: Keeps its value, overriding parent
4. **Parent Present(None)**: Overrides child's Some value with null
5. **Parent Omitted**: Never overrides child's Present values

## Advanced Usage

### Non-Option Omissible Fields

For fields like `tags: Omissible<Vec<String>>`:

```rust
Self::handle_omissible_override_non_option(
    &mut self.tags,
    &parent.tags,
);
```

## Real-World Example

```yaml
# Root project dbt_project.yml
models:
  my_package:
    +schema: null      # Explicitly override to null
    +database: null    # Explicitly override to null
```

```sql
-- Package model with config
{{ config(schema = 'something_else', database = 'my_db') }}
select 1 as id
```

Result: Model uses null for both schema and database, as the root project's explicit null values override the package's settings.

## Testing

See `omissible_utils_tests.rs` for comprehensive test examples covering:
- Basic inheritance patterns
- Explicit null overrides
- Chain of overrides (grandparent → parent → child)
- Cross-package context behavior
- Non-Option Omissible fields
- Serialization/deserialization

## Benefits

1. **Clear Semantics**: Distinguishes between "not specified" and "explicitly null"
2. **Hierarchical Control**: Parent configs can force null values on children
3. **Type Safety**: Compile-time guarantees for override behavior
4. **Easy Extension**: Simple function calls for any config type
5. **Consistent Behavior**: Same logic works across all configuration types