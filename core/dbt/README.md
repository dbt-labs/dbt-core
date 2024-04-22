# core/dbt directory README

## The following are individual files in this directory.

### compilation.py

### constants.py

### dataclass_schema.py

### deprecations.py

### exceptions.py

### flags.py

### helper_types.py

### hooks.py

### lib.py

### links.py

### logger.py

### main.py

### node_types.py

### profiler.py

### selected_resources.py

### semver.py

### tracking.py

### ui.py

### utils.py

### version.py


## The subdirectories will be documented in a README in the subdirectory
* adapters
* cli
* clients
* config
* context
* contracts
* deps
* docs
* events
* graph
* include
* parser
* task
* tests



how the selector module gets loaded

File "/Users/chenyuli/git/dbt-core/env_core/bin/dbt", line 33, in <module>
    sys.exit(load_entry_point('dbt-core', 'console_scripts', 'dbt')())
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/core.py", line 1157, in __call__
    return self.main(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/core.py", line 1078, in main
    rv = self.invoke(ctx)
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/core.py", line 1688, in invoke
    return _process_result(sub_ctx.command.invoke(sub_ctx))
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/core.py", line 1434, in invoke
    return ctx.invoke(self.callback, **ctx.params)
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/core.py", line 783, in invoke
    return __callback(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/env_core/lib/python3.11/site-packages/click/decorators.py", line 33, in new_func
    return f(get_current_context(), *args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/core/dbt/cli/main.py", line 148, in wrapper
    return func(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/core/dbt/cli/requires.py", line 106, in wrapper
    result, success = func(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/core/dbt/cli/requires.py", line 91, in wrapper
    return func(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/core/dbt/cli/requires.py", line 184, in wrapper
    return func(*args, **kwargs)
  File "/Users/chenyuli/git/dbt-core/core/dbt/cli/requires.py", line 200, in wrapper
    project = load_project(
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/runtime.py", line 51, in load_project
    project = Project.from_project_root(
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/project.py", line 765, in from_project_root
    return partial.render(renderer)
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/project.py", line 333, in render
    return self.create_project(rendered)
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/project.py", line 483, in create_project
    selectors = selector_config_from_data(rendered.selectors_dict)
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/selectors.py", line 110, in selector_config_from_data
    selectors = SelectorConfig.selectors_from_dict(selectors_data)
  File "/Users/chenyuli/git/dbt-core/core/dbt/config/selectors.py", line 39, in selectors_from_dict
    selectors = parse_from_selectors_definition(selector_file)
  File "/Users/chenyuli/git/dbt-core/core/dbt/graph/cli.py", line 261, in parse_from_selectors_definition
    "definition": parse_from_definition(
  File "/Users/chenyuli/git/dbt-core/core/dbt/graph/cli.py", line 241, in parse_from_definition
    return parse_union_definition(definition, result=result)
  File "/Users/chenyuli/git/dbt-core/core/dbt/graph/cli.py", line 163, in parse_union_definition
    union = SelectionUnion(components=include)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/bdb.py", line 90, in trace_dispatch
    return self.dispatch_line(frame)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/bdb.py", line 114, in dispatch_line
    self.user_line(frame)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/pdb.py", line 340, in user_line
    self.interaction(frame, None)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/pdb.py", line 435, in interaction
    self._cmdloop()
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/pdb.py", line 400, in _cmdloop
    self.cmdloop()
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/cmd.py", line 138, in cmdloop
    stop = self.onecmd(line)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/pdb.py", line 500, in onecmd
    return cmd.Cmd.onecmd(self, line)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/cmd.py", line 216, in onecmd
    return self.default(line)
  File "/Users/chenyuli/.asdf/installs/python/3.11.0/lib/python3.11/pdb.py", line 459, in default
    exec(code, globals, locals)
  File "<stdin>", line 1, in <module>
