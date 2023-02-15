# Parser README

The parsing stage of dbt produces:
* the 'manifest', a Python structure in the code
* the project `target/manifest.json` file

## Top Level Parsing steps

1. Load the macro manifest (MacroManifest):

* Inputs: SQL files in the 'macros' directory
* Outputs: the MacroManifest encapsulated in a BaseAdapter; a pared down Manifest-type file which contains only macros and files (to be copied into the full Manifest)

The macro manifest takes the place of the full-fledged Manifest and contains only macros, which are used for resolving tests during parsing. The 'adapter' code retains a MacroManifest, while the mainline parsing code will have a full manifest. The class MacroParser assists in this process.

2. Loads all internal 'projects' (i.e. has a `dbt_project.yml`) and all projects in the project path.

3. Create a ManifestLoader object from the project dependencies and the current config.

4. Read and parse the project files. Results are loaded into the ManifestLoader.  ManifestLoader loads the MacroManifest object into a BaseAdapter object.

5. Write the partial parse results (the pickle file). This writes out the 'results' from the ManifestLoader, so the "create the manifest" step has not occured yet. Things yet to happen include patching the nodes, patching the macros, and processing refs, sources, and docs.

6. Sources are patched. First, source tests are parsed. Nodes, sources, macros, docs, exposures, metadata, files, and selectors are copied into the Manifest by the ManifestLoader. And finally, nodes (from `results.patches`) are "patched" and macros too (from `results.macro_patches`).

7. Process the manifest (refs, sources, docs).
	* Loops through all nodes, for each node find the node that matches
	* the sources refs, and adds the unique id of the source to each
	* node's 'depends_on'

8. Check the Manifest and check for resource uniqueness.

9. Build the flat graph

10. Write out the target/manifest.json file.

## Parse the project files

There are several parser-type objects. Each "parser" gets a list of of matching files specified by directory and ('dbt_project.yml', '*.sql', '*.yml', *.csv, or *.md)

### ModelParser

code: core/dbt/parser/models.py. Most of the code is in SimpleSQLParser.

paths: source\_paths + `*.sql`

Manifest: nodes

### SnapshotParser

code: core/dbt/parser/snapshots.py

paths:  snapshot\_paths + `*.sql`

### AnalysisParser

code: core/dbt/parser/analysis.py

paths: analysis\_paths + `*.sql`

Manifest: nodes

### DataTestParser

code: core/dbt/parser/data\_test.py

paths: test\_paths + `*.sql`

Manifest: nodes

### HookParser

code: core/dbt/parser/hooks.py

paths: 'dbt\_project.yml'

### SeedParser

code: core/dbt/parser/seeds.py

paths: data\_paths + `*.csv`

### DocumentationParser

code: core/dbt/parser/docs.py

paths: docs\_paths, `*.md`

Manifest: docs

### SchemaParser

* Input: yaml files
* Output: various nodes in the Manifest and 'patch' files in the ParseResult/Manifest

code: core/dbt/parser/schemas.py

paths: all\_source\_paths = source\_paths, data\_paths, snapshot\_paths, analysis\_paths, macro\_paths,  `*.yml`

This "parses" the `.yml` (property) files in the dbt project. It loops through each yaml file and pulls out the tests in the various config sections and jinja renders them.

A different sub-parser is called for each main dictionary key in the yaml.

* 'models' - TestablePatchParser
	* plus 'patches' at create manifest time
	* Manifest: nodes
* 'seeds' - TestablePatchParser
* 'snapshots' - TestablePatchParser
* 'sources' - SourceParser
	* plus 'patch\_sources' at create manifest times
	* Manifest: sources
* 'macros' - MacroPatchParser
	* plus 'macro\_patches' at create manifest time
	* Manifest: macros
* 'analyses' - AnalysisPatchParser
* 'exposures' - SchemaParser.parse\_exposures
	* no 'patches'
	* Manifest: exposures
* 'groups' - SchemaParser.parse\_groups
	* no 'patches'
	* Manifest: groups

# dbt Manifest

### nodes

These have executable SQL attached.

Models
- Are generated from SQL files in the 'models' directory
- have a unique_id starting with 'model.'
- Final object is a ModelNode

Singular Tests
- Are generated from SQL files in 'tests' directory
- have a unique_id starting with 'test.'
- Final object is a SingularTestNode

Generic Tests
- Are generated from 'tests' in schema yaml files, which ultimately derive from tests in the 'macros' directory
- Have a unique_id starting with 'test.'
- Final object is a GenericTestNode
- fqn is <project>.schema_test.<generated name>

Hooks
- come from 'on-run-start' and 'on-run-end' config attributes.
- have a unique_id starting with 'operation.'
- FQN is of the form: ["dbt_labs_internal_analytics","hooks","dbt_labs_internal_analytics-on-run-end-0"]

Analysis
- comes from SQL files in 'analysis' directory
- Final object is a AnalysisNode

RPC Node
- This is a "node" representing the bit of Jinja-SQL that gets passed into the run_sql or compile_sql methods. When you're using the Cloud IDE, and you're working in a scratch tab, and you just want to compile/run what you have there: it needs to be parsed and executed, but it's not actually a model/node in the project, so it's this special thing. This is a temporary addition to the running manifest.

- Object is a RPCNode

### sources

- comes from 'sources' sections in yaml files
- Final object is a SourceDefinition node
- have a unique_id starting with 'source.'

### macros

- comes from SQL files in 'macros' directory
- Final object is a Macro node
- have a unique_id starting with 'macro.'
- Test macros are used in schema tests

### docs

- comes from .md files in 'docs' directory
- Final object is a Documentation

### exposures

- comes from 'exposures' sections in yaml files
- Final object is a Exposure node

## Temporary patch files

The information in these structures is stored here by the schema parser, but should be resolved before the final manifest is written and do not show up in the written manifest. Ideally we'd like to skip this step and apply the changes directly to the nodes, macros, and sources instead. With the current staged parser we have to save this with the manifest information.

### patches

### macro_patches

### source_patches

## Other

### selectors

Selectors are set in config yaml files and can be used to determine which nodes should be 'compiled' and run. Selectors can also be done on the command line and will be in cli args.

### disabled

Models, sources, or other nodes that the user has disabled by setting enabled: false. They should be completely ignored by dbt, as if they don't exist. In its simplest/silliest form, it's a way to keep code around without deleting it. Some folks do cleverer things, like dynamically enabling/disabling certain models based on the database adapter type or the value of --vars.

### files

This contains a list of all of the files that were processed with links to the nodes, docs, macros, sources, exposures, patches, macro_patches, source_patches, i.e. all of the other places that data is stored in the manifest. It also has a checksum of the contents. The 'files' structure is in the saved manifest, but not in the manifest.json file that is written out. It is used in partial parsing to determine whether to use previously generated nodes.

### metadata

From the ManifestMetadata class. Contains dbt_schema_version, project_id, user_id, send_anonymous_usage_stats, adapter_type

### flat_graph

Used during execution in context.common (?). Builds dictionaries of nodes and sources. Not sure why this is used instead of the original nodes and sources. Not in the written manifest.

### state_check

This used to be in ParseResults (not committed yet). The saved version of this is compared against the current version to see if we can use the saved Manifest. Contains var_hash, profile_hash, and project_hashes, to compare to the saved Manifest to see if things have changed that would invalidate it.

## Written Manifest only

### child_map

### parent_map
