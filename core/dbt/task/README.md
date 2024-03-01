# Task README

### Task Hierarchy
```
BaseTask
 ┣ CleanTask
 ┣ ConfiguredTask
 ┃ ┣ GraphRunnableTask
 ┃ ┃  ┣ CloneTask
 ┃ ┃  ┣ CompileTask
 ┃ ┃  ┃ ┣ GenerateTask
 ┃ ┃  ┃ ┣ RunTask
 ┃ ┃  ┃ ┃ ┣ BuildTask
 ┃ ┃  ┃ ┃ ┣ FreshnessTask
 ┃ ┃  ┃ ┃ ┣ SeedTask
 ┃ ┃  ┃ ┃ ┣ SnapshotTask
 ┃ ┃  ┃ ┃ ┗ TestTask 
 ┃ ┃  ┃ ┗ ShowTask
 ┃ ┃  ┗ ListTask
 ┃ ┣ RetryTask 
 ┃ ┣ RunOperationTask
 ┃ ┗ ServeTask
 ┣ DebugTask
 ┣ DepsTask
 ┗ InitTask
```

### Runner Hierarchy
```
BaseRunner
 ┣ CloneRunner
 ┣ CompileRunner
 ┃ ┣ GenericSqlRunner
 ┃ ┃  ┣ SqlCompileRunner
 ┃ ┃  ┗ SqlExecuteRunner
 ┃ ┣ ModelRunner
 ┃ ┃ ┣ SeedRunner
 ┃ ┃ ┗ SnapshotRunner
 ┃ ┣ ShowRunner
 ┃ ┗ TestRunner
 ┣ FreshnessRunner
 ┗ SavedQueryRunner
```
