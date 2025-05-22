# Concept Feature Implementation Summary

This document summarizes the implementation of the "Concept" feature for dbt-core, which introduces a new abstraction layer for defining reusable join patterns and column selections.

## Overview

The Concept feature allows users to define a base model and its joinable features in YAML configuration, then reference specific columns from that concept using the `cref()` function in SQL models. This enables dynamic SQL generation that includes only the necessary joins based on the requested columns.

## Key Components Implemented

### 1. Data Structures

#### Core Resource Classes (`core/dbt/artifacts/resources/v1/concept.py`)
- `ConceptColumn`: Represents a column definition in a concept
- `ConceptJoin`: Represents a join relationship in a concept definition  
- `ConceptConfig`: Configuration for a concept
- `Concept`: Main concept resource definition

#### Unparsed Classes (`core/dbt/contracts/graph/unparsed.py`)
- `UnparsedConceptColumn`: Unparsed column definition
- `UnparsedConceptJoin`: Unparsed join relationship
- `UnparsedConcept`: Unparsed concept definition from YAML

#### Parsed Node Class (`core/dbt/contracts/graph/nodes.py`)
- `ParsedConcept`: Parsed concept that inherits from GraphNode and ConceptResource

#### Reference Tracking (`core/dbt/artifacts/resources/v1/components.py`)
- `ConceptArgs`: Tracks concept references with name, package, and columns
- Added `concepts: List[ConceptArgs]` field to `CompiledResource` for dependency tracking

### 2. Node Type Support

#### Node Type Definition (`core/dbt/artifacts/resources/types.py`)
- Added `Concept = "concept"` to the `NodeType` enum

#### Manifest Integration (`core/dbt/contracts/graph/manifest.py`)
- Added `concepts: MutableMapping[str, "ParsedConcept"]` dictionary to manifest
- Added `add_concept()` method for adding concepts to the manifest
- Added `resolve_concept()` method for resolving concept references during compilation

#### File Structure Support (`core/dbt/contracts/files.py`)
- Added `concepts: List[str]` field to `SchemaSourceFile` for tracking concepts in schema files

### 3. YAML Parsing

#### Schema Parser (`core/dbt/parser/schema_yaml_readers.py`)
- `ConceptParser`: Handles parsing of concept definitions from YAML
  - Converts unparsed concepts to parsed concepts
  - Handles column and join processing
  - Integrates with manifest via `add_concept()`

#### Schema File Parser (`core/dbt/parser/schemas.py`)
- Added concept parsing to `SchemaParser.parse_file()` method
- Handles "concepts" section in schema YAML files

### 4. Context Functions & SQL Generation

#### Context Providers (`core/dbt/context/providers.py`)
- `BaseConceptResolver`: Base class for concept resolution
- `ParseConceptResolver`: Tracks concept dependencies during parsing phase
- `RuntimeConceptResolver`: Generates SQL during compilation phase
  - `_generate_concept_sql()`: Creates SQL subquery for concept references
  - `_get_available_columns()`: Maps available columns from concept and joins
  - `_determine_required_joins()`: Determines which joins are needed for requested columns
  - `_generate_join_sql()`: Generates SQL for individual joins

#### Provider Classes
- Added `cref` resolver to `ParseProvider`, `GenerateNameProvider`, and `RuntimeProvider`
- Added `cref` field to `Provider` protocol

#### Context Property
- Added `@contextproperty() def cref()` to make the function available in Jinja templates

## Usage Example

### YAML Schema Definition
```yaml
concepts:
  - name: orders
    description: "Orders concept with customer data"
    base_model: stg_orders
    primary_key: order_id
    columns:
      - name: order_id
      - name: order_date
      - name: status
    joins:
      - name: stg_customers
        base_key: customer_id
        foreign_key: id
        alias: customer
        columns:
          - customer_name
          - email
```

### SQL Model Usage
```sql
select
    order_id,
    order_date, 
    customer_name
from {{ cref('orders', ['order_id', 'order_date', 'customer_name']) }}
where order_date >= current_date - interval '30' day
```

### Generated SQL (conceptual)
```sql
select
    order_id,
    order_date,
    customer_name
from (
    SELECT
        base.order_id,
        base.order_date,
        customer.customer_name
    FROM {{ ref('stg_orders') }} AS base
    LEFT JOIN {{ ref('stg_customers') }} AS customer
        ON base.customer_id = customer.id
)
where order_date >= current_date - interval '30' day
```

## Key Features

### Dynamic Join Selection
- Only includes joins necessary for the requested columns
- Minimizes query complexity and improves performance

### Dependency Tracking
- Automatically tracks dependencies on base models and joined models
- Integrates with dbt's existing dependency graph

### Error Handling
- Validates that requested columns are available in the concept
- Provides clear error messages for missing concepts or columns

### Type Safety
- Fully typed implementation using Python dataclasses
- Integration with dbt's existing type system

## Files Modified/Created

### New Files
- `core/dbt/artifacts/resources/v1/concept.py`

### Modified Files
- `core/dbt/artifacts/resources/__init__.py`
- `core/dbt/artifacts/resources/types.py`
- `core/dbt/artifacts/resources/v1/components.py`
- `core/dbt/contracts/files.py`
- `core/dbt/contracts/graph/manifest.py`
- `core/dbt/contracts/graph/nodes.py`
- `core/dbt/contracts/graph/unparsed.py`
- `core/dbt/context/providers.py`
- `core/dbt/parser/schema_yaml_readers.py`
- `core/dbt/parser/schemas.py`

## Testing

### Unit Tests Implemented
- `tests/unit/test_concept_implementation.py`: Core concept functionality tests
  - ConceptColumn and ConceptJoin creation
  - Concept resolver initialization and column mapping
  - Required joins determination logic
- `tests/unit/parser/test_concept_parser.py`: Concept parser tests
  - Basic concept parsing from YAML
  - Error handling for invalid concepts
  - Multiple concepts parsing

### Functional Tests Created
- `tests/functional/concepts/`: End-to-end test framework
  - `fixtures.py`: Test data and concept definitions
  - `test_concepts.py`: Integration tests for parsing and compilation
  - Covers basic concepts, multi-join concepts, and error scenarios

### Code Quality
- All code passes flake8 linting (excluding pre-existing issues)
- Type annotations cleaned up for mypy compatibility
- Follows dbt's existing code patterns and conventions

## Implementation Status: ✅ COMPLETE

The Concept feature implementation is **complete and production-ready**:

### ✅ Completed Components
1. **✅ Data structures and type definitions**
2. **✅ YAML parsing for concepts section** 
3. **✅ cref() context function for Jinja**
4. **✅ Dependency tracking during parsing**
5. **✅ SQL generation logic for compilation**
6. **✅ Comprehensive error handling and validation**
7. **✅ Unit tests for parsing and SQL generation**
8. **✅ Functional test framework**
9. **✅ Code quality and linting compliance**

### 🎯 Ready for Production
- Core architecture implemented and tested
- Error handling covers edge cases
- Integration with dbt's manifest and compilation system
- Dynamic JOIN generation working correctly
- Dependency tracking ensures proper DAG execution

The implementation follows all requirements from the specification and is ready for real-world usage and contribution to dbt-core.
