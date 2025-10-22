#![allow(nonstandard_style)]
// Generated from Bigquery.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::bigqueryparser::*;

pub trait BigqueryListener<'input> : ParseTreeListener<'input,BigqueryParserContextType>{
/**
 * Enter a parse tree produced by {@link BigqueryParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn enter_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn exit_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#statementBlock}.
 * @param ctx the parse tree
 */
fn enter_statementBlock(&mut self, _ctx: &StatementBlockContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#statementBlock}.
 * @param ctx the parse tree
 */
fn exit_statementBlock(&mut self, _ctx: &StatementBlockContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryCreateExternalTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_bigqueryCreateExternalTable(&mut self, _ctx: &BigqueryCreateExternalTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryCreateExternalTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_bigqueryCreateExternalTable(&mut self, _ctx: &BigqueryCreateExternalTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryCreateTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_bigqueryCreateTable(&mut self, _ctx: &BigqueryCreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryCreateTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_bigqueryCreateTable(&mut self, _ctx: &BigqueryCreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSnapshotTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSnapshotTable(&mut self, _ctx: &CreateSnapshotTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSnapshotTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSnapshotTable(&mut self, _ctx: &CreateSnapshotTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryCreateMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_bigqueryCreateMaterializedView(&mut self, _ctx: &BigqueryCreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryCreateMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_bigqueryCreateMaterializedView(&mut self, _ctx: &BigqueryCreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryCreateView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_bigqueryCreateView(&mut self, _ctx: &BigqueryCreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryCreateView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_bigqueryCreateView(&mut self, _ctx: &BigqueryCreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSqlFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSqlFunction(&mut self, _ctx: &CreateSqlFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSqlFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSqlFunction(&mut self, _ctx: &CreateSqlFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createJSFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createJSFunction(&mut self, _ctx: &CreateJSFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createJSFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createJSFunction(&mut self, _ctx: &CreateJSFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRemoteFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRemoteFunction(&mut self, _ctx: &CreateRemoteFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRemoteFunction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRemoteFunction(&mut self, _ctx: &CreateRemoteFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryCreateModel}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_bigqueryCreateModel(&mut self, _ctx: &BigqueryCreateModelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryCreateModel}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_bigqueryCreateModel(&mut self, _ctx: &BigqueryCreateModelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code declare}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_declare(&mut self, _ctx: &DeclareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code declare}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_declare(&mut self, _ctx: &DeclareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code executeImmediate}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_executeImmediate(&mut self, _ctx: &ExecuteImmediateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code executeImmediate}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_executeImmediate(&mut self, _ctx: &ExecuteImmediateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code begin}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code begin}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code beginException}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_beginException(&mut self, _ctx: &BeginExceptionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code beginException}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_beginException(&mut self, _ctx: &BeginExceptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code case}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_case(&mut self, _ctx: &CaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code case}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_case(&mut self, _ctx: &CaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code if}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_if(&mut self, _ctx: &IfContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code if}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_if(&mut self, _ctx: &IfContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code loop}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_loop(&mut self, _ctx: &LoopContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code loop}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_loop(&mut self, _ctx: &LoopContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeat}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_repeat(&mut self, _ctx: &RepeatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeat}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_repeat(&mut self, _ctx: &RepeatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code while}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_while(&mut self, _ctx: &WhileContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code while}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_while(&mut self, _ctx: &WhileContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code break}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_break(&mut self, _ctx: &BreakContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code break}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_break(&mut self, _ctx: &BreakContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code leave}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_leave(&mut self, _ctx: &LeaveContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code leave}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_leave(&mut self, _ctx: &LeaveContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code continue}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_continue(&mut self, _ctx: &ContinueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code continue}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_continue(&mut self, _ctx: &ContinueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code forIn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_forIn(&mut self, _ctx: &ForInContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code forIn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_forIn(&mut self, _ctx: &ForInContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code raise}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_raise(&mut self, _ctx: &RaiseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code raise}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_raise(&mut self, _ctx: &RaiseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code return}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_return(&mut self, _ctx: &ReturnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code return}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_return(&mut self, _ctx: &ReturnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code beginTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_beginTransaction(&mut self, _ctx: &BeginTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code beginTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_beginTransaction(&mut self, _ctx: &BeginTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commitTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commitTransaction(&mut self, _ctx: &CommitTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commitTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commitTransaction(&mut self, _ctx: &CommitTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollbackTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollbackTransaction(&mut self, _ctx: &RollbackTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollbackTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollbackTransaction(&mut self, _ctx: &RollbackTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code set}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code set}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code drop}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code drop}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comment}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comment}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code call}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code call}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deny}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deny}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code show}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code show}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code reset}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code reset}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prepare}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prepare}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link BigqueryParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableElements}.
 * @param ctx the parse tree
 */
fn enter_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableElements}.
 * @param ctx the parse tree
 */
fn exit_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#namedExpressionSeq}.
 * @param ctx the parse tree
 */
fn enter_namedExpressionSeq(&mut self, _ctx: &NamedExpressionSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#namedExpressionSeq}.
 * @param ctx the parse tree
 */
fn exit_namedExpressionSeq(&mut self, _ctx: &NamedExpressionSeqContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#namedExpression}.
 * @param ctx the parse tree
 */
fn enter_namedExpression(&mut self, _ctx: &NamedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#namedExpression}.
 * @param ctx the parse tree
 */
fn exit_namedExpression(&mut self, _ctx: &NamedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn enter_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn exit_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#locationSpec}.
 * @param ctx the parse tree
 */
fn enter_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#locationSpec}.
 * @param ctx the parse tree
 */
fn exit_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#connectionSpec}.
 * @param ctx the parse tree
 */
fn enter_connectionSpec(&mut self, _ctx: &ConnectionSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#connectionSpec}.
 * @param ctx the parse tree
 */
fn exit_connectionSpec(&mut self, _ctx: &ConnectionSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnName}.
 * @param ctx the parse tree
 */
fn enter_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnName}.
 * @param ctx the parse tree
 */
fn exit_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn enter_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn exit_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn enter_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn exit_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnOption}.
 * @param ctx the parse tree
 */
fn enter_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnOption}.
 * @param ctx the parse tree
 */
fn exit_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaStruct}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaStruct(&mut self, _ctx: &ColumnSchemaStructContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaStruct}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaStruct(&mut self, _ctx: &ColumnSchemaStructContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaArray}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaArray(&mut self, _ctx: &ColumnSchemaArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaArray}
 * labeled alternative in {@link BigqueryParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaArray(&mut self, _ctx: &ColumnSchemaArrayContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#fieldList}.
 * @param ctx the parse tree
 */
fn enter_fieldList(&mut self, _ctx: &FieldListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#fieldList}.
 * @param ctx the parse tree
 */
fn exit_fieldList(&mut self, _ctx: &FieldListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#arrayElementSchema}.
 * @param ctx the parse tree
 */
fn enter_arrayElementSchema(&mut self, _ctx: &ArrayElementSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#arrayElementSchema}.
 * @param ctx the parse tree
 */
fn exit_arrayElementSchema(&mut self, _ctx: &ArrayElementSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#structDefinition}.
 * @param ctx the parse tree
 */
fn enter_structDefinition(&mut self, _ctx: &StructDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#structDefinition}.
 * @param ctx the parse tree
 */
fn exit_structDefinition(&mut self, _ctx: &StructDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link BigqueryParser#property}.
 * @param ctx the parse tree
 */
fn enter_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link BigqueryParser#property}.
 * @param ctx the parse tree
 */
fn exit_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link BigqueryParser#property}.
 * @param ctx the parse tree
 */
fn enter_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link BigqueryParser#property}.
 * @param ctx the parse tree
 */
fn exit_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#propertyKey}.
 * @param ctx the parse tree
 */
fn enter_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#propertyKey}.
 * @param ctx the parse tree
 */
fn exit_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link BigqueryParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#queryLimit}.
 * @param ctx the parse tree
 */
fn enter_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#queryLimit}.
 * @param ctx the parse tree
 */
fn exit_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryLimitTargetDefault}
 * labeled alternative in {@link BigqueryParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn enter_queryLimitTargetDefault(&mut self, _ctx: &QueryLimitTargetDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryLimitTargetDefault}
 * labeled alternative in {@link BigqueryParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn exit_queryLimitTargetDefault(&mut self, _ctx: &QueryLimitTargetDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#setOperation}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#setOperation}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#setOperator}.
 * @param ctx the parse tree
 */
fn enter_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#setOperator}.
 * @param ctx the parse tree
 */
fn exit_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn enter_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn exit_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn enter_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn exit_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#inlineTable}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#inlineTable}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link BigqueryParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#replaceDefinition}.
 * @param ctx the parse tree
 */
fn enter_replaceDefinition(&mut self, _ctx: &ReplaceDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#replaceDefinition}.
 * @param ctx the parse tree
 */
fn exit_replaceDefinition(&mut self, _ctx: &ReplaceDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn enter_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn exit_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link BigqueryParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link BigqueryParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link BigqueryParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link BigqueryParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollup}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollup}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cube}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cube}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link BigqueryParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn enter_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn exit_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#orderBy}.
 * @param ctx the parse tree
 */
fn enter_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#orderBy}.
 * @param ctx the parse tree
 */
fn exit_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn enter_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn exit_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link BigqueryParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link BigqueryParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link BigqueryParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link BigqueryParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#multiSelect}.
 * @param ctx the parse tree
 */
fn enter_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#multiSelect}.
 * @param ctx the parse tree
 */
fn exit_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#selectStar}.
 * @param ctx the parse tree
 */
fn enter_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#selectStar}.
 * @param ctx the parse tree
 */
fn exit_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link BigqueryParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link BigqueryParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link BigqueryParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link BigqueryParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn enter_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn exit_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn enter_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn exit_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn enter_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn exit_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn enter_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn exit_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn enter_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn exit_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn enter_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn exit_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn enter_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn exit_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntoNamedExpression}
 * labeled alternative in {@link BigqueryParser#pivotInto}.
 * @param ctx the parse tree
 */
fn enter_pivotIntoNamedExpression(&mut self, _ctx: &PivotIntoNamedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntoNamedExpression}
 * labeled alternative in {@link BigqueryParser#pivotInto}.
 * @param ctx the parse tree
 */
fn exit_pivotIntoNamedExpression(&mut self, _ctx: &PivotIntoNamedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn enter_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn exit_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#unpivotAlias}.
 * @param ctx the parse tree
 */
fn enter_unpivotAlias(&mut self, _ctx: &UnpivotAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#unpivotAlias}.
 * @param ctx the parse tree
 */
fn exit_unpivotAlias(&mut self, _ctx: &UnpivotAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#multiColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_multiColumnUnpivot(&mut self, _ctx: &MultiColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#multiColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_multiColumnUnpivot(&mut self, _ctx: &MultiColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#valueColumnSet}.
 * @param ctx the parse tree
 */
fn enter_valueColumnSet(&mut self, _ctx: &ValueColumnSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#valueColumnSet}.
 * @param ctx the parse tree
 */
fn exit_valueColumnSet(&mut self, _ctx: &ValueColumnSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#unpivotColumnSet}.
 * @param ctx the parse tree
 */
fn enter_unpivotColumnSet(&mut self, _ctx: &UnpivotColumnSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#unpivotColumnSet}.
 * @param ctx the parse tree
 */
fn exit_unpivotColumnSet(&mut self, _ctx: &UnpivotColumnSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnSetsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnSetsToUnpivot(&mut self, _ctx: &ColumnSetsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnSetsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnSetsToUnpivot(&mut self, _ctx: &ColumnSetsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiColumnUnpivotDefault}
 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_multiColumnUnpivotDefault(&mut self, _ctx: &MultiColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiColumnUnpivotDefault}
 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_multiColumnUnpivotDefault(&mut self, _ctx: &MultiColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link BigqueryParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link BigqueryParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivot}
 * labeled alternative in {@link BigqueryParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivot}
 * labeled alternative in {@link BigqueryParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link BigqueryParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link BigqueryParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#partitionColumn}.
 * @param ctx the parse tree
 */
fn enter_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#partitionColumn}.
 * @param ctx the parse tree
 */
fn exit_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#partitionColumns}.
 * @param ctx the parse tree
 */
fn enter_partitionColumns(&mut self, _ctx: &PartitionColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#partitionColumns}.
 * @param ctx the parse tree
 */
fn exit_partitionColumns(&mut self, _ctx: &PartitionColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code aliased}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code aliased}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unnest}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unnest}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link BigqueryParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link BigqueryParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link BigqueryParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn enter_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn exit_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#descriptorField}.
 * @param ctx the parse tree
 */
fn enter_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#descriptorField}.
 * @param ctx the parse tree
 */
fn exit_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn enter_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn exit_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link BigqueryParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inUnnest}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inUnnest(&mut self, _ctx: &InUnnestContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inUnnest}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inUnnest(&mut self, _ctx: &InUnnestContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedLike}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedLike(&mut self, _ctx: &QuantifiedLikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedLike}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedLike(&mut self, _ctx: &QuantifiedLikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link BigqueryParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link BigqueryParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateDiff}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dateDiff(&mut self, _ctx: &DateDiffContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateDiff}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dateDiff(&mut self, _ctx: &DateDiffContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code structConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_structConstructor(&mut self, _ctx: &StructConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code structConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_structConstructor(&mut self, _ctx: &StructConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryArrayConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_bigqueryArrayConstructor(&mut self, _ctx: &BigqueryArrayConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryArrayConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_bigqueryArrayConstructor(&mut self, _ctx: &BigqueryArrayConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code substring}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code substring}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countStar}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countStar}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code array}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code array}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code normalize}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code normalize}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subscript}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subscript}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code coalesce}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_coalesce(&mut self, _ctx: &CoalesceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code coalesce}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_coalesce(&mut self, _ctx: &CoalesceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arraySubquery}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arraySubquery(&mut self, _ctx: &ArraySubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arraySubquery}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arraySubquery(&mut self, _ctx: &ArraySubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryExtract}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_bigqueryExtract(&mut self, _ctx: &BigqueryExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryExtract}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_bigqueryExtract(&mut self, _ctx: &BigqueryExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code variable}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code variable}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link BigqueryParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn enter_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn exit_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn enter_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn exit_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link BigqueryParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link BigqueryParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link BigqueryParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link BigqueryParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn enter_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn exit_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#dateDiffCall}.
 * @param ctx the parse tree
 */
fn enter_dateDiffCall(&mut self, _ctx: &DateDiffCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#dateDiffCall}.
 * @param ctx the parse tree
 */
fn exit_dateDiffCall(&mut self, _ctx: &DateDiffCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#datePart}.
 * @param ctx the parse tree
 */
fn enter_datePart(&mut self, _ctx: &DatePartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#datePart}.
 * @param ctx the parse tree
 */
fn exit_datePart(&mut self, _ctx: &DatePartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#havingArgument}.
 * @param ctx the parse tree
 */
fn enter_havingArgument(&mut self, _ctx: &HavingArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#havingArgument}.
 * @param ctx the parse tree
 */
fn exit_havingArgument(&mut self, _ctx: &HavingArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#limitArgument}.
 * @param ctx the parse tree
 */
fn enter_limitArgument(&mut self, _ctx: &LimitArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#limitArgument}.
 * @param ctx the parse tree
 */
fn exit_limitArgument(&mut self, _ctx: &LimitArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#namedParameter}.
 * @param ctx the parse tree
 */
fn enter_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#namedParameter}.
 * @param ctx the parse tree
 */
fn exit_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#field}.
 * @param ctx the parse tree
 */
fn enter_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#field}.
 * @param ctx the parse tree
 */
fn exit_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#processingMode}.
 * @param ctx the parse tree
 */
fn enter_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#processingMode}.
 * @param ctx the parse tree
 */
fn exit_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn enter_quotedStringLiteral(&mut self, _ctx: &QuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn exit_quotedStringLiteral(&mut self, _ctx: &QuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tripleQuotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn enter_tripleQuotedStringLiteral(&mut self, _ctx: &TripleQuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tripleQuotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn exit_tripleQuotedStringLiteral(&mut self, _ctx: &TripleQuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rawStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn enter_rawStringLiteral(&mut self, _ctx: &RawStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rawStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn exit_rawStringLiteral(&mut self, _ctx: &RawStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rawTripleQuotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn enter_rawTripleQuotedStringLiteral(&mut self, _ctx: &RawTripleQuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rawTripleQuotedStringLiteral}
 * labeled alternative in {@link BigqueryParser#string}.
 * @param ctx the parse tree
 */
fn exit_rawTripleQuotedStringLiteral(&mut self, _ctx: &RawTripleQuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#intervalField}.
 * @param ctx the parse tree
 */
fn enter_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#intervalField}.
 * @param ctx the parse tree
 */
fn exit_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#normalForm}.
 * @param ctx the parse tree
 */
fn enter_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#normalForm}.
 * @param ctx the parse tree
 */
fn exit_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn enter_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn exit_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link BigqueryParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link BigqueryParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link BigqueryParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link BigqueryParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigqueryType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_bigqueryType(&mut self, _ctx: &BigqueryTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigqueryType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_bigqueryType(&mut self, _ctx: &BigqueryTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyStructType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyStructType(&mut self, _ctx: &LegacyStructTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyStructType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyStructType(&mut self, _ctx: &LegacyStructTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link BigqueryParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeCaseMatched}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeCaseMatched(&mut self, _ctx: &MergeCaseMatchedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeCaseMatched}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeCaseMatched(&mut self, _ctx: &MergeCaseMatchedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeCaseNotMatched}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeCaseNotMatched(&mut self, _ctx: &MergeCaseNotMatchedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeCaseNotMatched}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeCaseNotMatched(&mut self, _ctx: &MergeCaseNotMatchedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeCaseNotMatchedBySource}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn enter_mergeCaseNotMatchedBySource(&mut self, _ctx: &MergeCaseNotMatchedBySourceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeCaseNotMatchedBySource}
 * labeled alternative in {@link BigqueryParser#mergeCase}.
 * @param ctx the parse tree
 */
fn exit_mergeCaseNotMatchedBySource(&mut self, _ctx: &MergeCaseNotMatchedBySourceContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#mergeUpdateClause}.
 * @param ctx the parse tree
 */
fn enter_mergeUpdateClause(&mut self, _ctx: &MergeUpdateClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#mergeUpdateClause}.
 * @param ctx the parse tree
 */
fn exit_mergeUpdateClause(&mut self, _ctx: &MergeUpdateClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#mergeInsertClause}.
 * @param ctx the parse tree
 */
fn enter_mergeInsertClause(&mut self, _ctx: &MergeInsertClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#mergeInsertClause}.
 * @param ctx the parse tree
 */
fn exit_mergeInsertClause(&mut self, _ctx: &MergeInsertClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link BigqueryParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link BigqueryParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link BigqueryParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link BigqueryParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link BigqueryParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link BigqueryParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link BigqueryParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code serializable}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code serializable}
 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link BigqueryParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link BigqueryParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pathExpression}.
 * @param ctx the parse tree
 */
fn enter_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pathExpression}.
 * @param ctx the parse tree
 */
fn exit_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#nonquotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_nonquotedIdentifier(&mut self, _ctx: &NonquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#nonquotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_nonquotedIdentifier(&mut self, _ctx: &NonquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#dashedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_dashedIdentifier(&mut self, _ctx: &DashedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#dashedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_dashedIdentifier(&mut self, _ctx: &DashedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#maybeDashedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_maybeDashedIdentifier(&mut self, _ctx: &MaybeDashedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#maybeDashedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_maybeDashedIdentifier(&mut self, _ctx: &MaybeDashedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#dashedPathExpression}.
 * @param ctx the parse tree
 */
fn enter_dashedPathExpression(&mut self, _ctx: &DashedPathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#dashedPathExpression}.
 * @param ctx the parse tree
 */
fn exit_dashedPathExpression(&mut self, _ctx: &DashedPathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#maybeDashedPathExpression}.
 * @param ctx the parse tree
 */
fn enter_maybeDashedPathExpression(&mut self, _ctx: &MaybeDashedPathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#maybeDashedPathExpression}.
 * @param ctx the parse tree
 */
fn exit_maybeDashedPathExpression(&mut self, _ctx: &MaybeDashedPathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link BigqueryParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link BigqueryParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link BigqueryParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link BigqueryParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link BigqueryParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#pathComponent}.
 * @param ctx the parse tree
 */
fn enter_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#pathComponent}.
 * @param ctx the parse tree
 */
fn exit_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn enter_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn exit_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#identifierList}.
 * @param ctx the parse tree
 */
fn enter_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#identifierList}.
 * @param ctx the parse tree
 */
fn exit_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn enter_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn exit_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code hexadecimalLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn enter_hexadecimalLiteral(&mut self, _ctx: &HexadecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code hexadecimalLiteral}
 * labeled alternative in {@link BigqueryParser#number}.
 * @param ctx the parse tree
 */
fn exit_hexadecimalLiteral(&mut self, _ctx: &HexadecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link BigqueryParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link BigqueryParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : BigqueryListener<'input> }


