#![allow(nonstandard_style)]
// Generated from Trino.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::trinoparser::*;

pub trait TrinoListener<'input> : ParseTreeListener<'input,TrinoParserContextType>{
/**
 * Enter a parse tree produced by {@link TrinoParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn enter_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn exit_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRecursiveTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRecursiveTable(&mut self, _ctx: &CreateRecursiveTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRecursiveTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRecursiveTable(&mut self, _ctx: &CreateRecursiveTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code set}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code set}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code drop}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code drop}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comment}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comment}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code call}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code call}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deny}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deny}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explain}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explain}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code show}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code show}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code reset}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code reset}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prepare}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prepare}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link TrinoParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableElements}.
 * @param ctx the parse tree
 */
fn enter_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableElements}.
 * @param ctx the parse tree
 */
fn exit_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnName}.
 * @param ctx the parse tree
 */
fn enter_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnName}.
 * @param ctx the parse tree
 */
fn exit_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn enter_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn exit_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn enter_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn exit_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnOption}.
 * @param ctx the parse tree
 */
fn enter_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnOption}.
 * @param ctx the parse tree
 */
fn exit_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link TrinoParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link TrinoParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link TrinoParser#property}.
 * @param ctx the parse tree
 */
fn enter_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link TrinoParser#property}.
 * @param ctx the parse tree
 */
fn exit_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link TrinoParser#property}.
 * @param ctx the parse tree
 */
fn enter_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link TrinoParser#property}.
 * @param ctx the parse tree
 */
fn exit_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#propertyKey}.
 * @param ctx the parse tree
 */
fn enter_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#propertyKey}.
 * @param ctx the parse tree
 */
fn exit_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link TrinoParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#queryLimit}.
 * @param ctx the parse tree
 */
fn enter_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#queryLimit}.
 * @param ctx the parse tree
 */
fn exit_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryLimitTargetDefault}
 * labeled alternative in {@link TrinoParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn enter_queryLimitTargetDefault(&mut self, _ctx: &QueryLimitTargetDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryLimitTargetDefault}
 * labeled alternative in {@link TrinoParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn exit_queryLimitTargetDefault(&mut self, _ctx: &QueryLimitTargetDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#rowOrRows}.
 * @param ctx the parse tree
 */
fn enter_rowOrRows(&mut self, _ctx: &RowOrRowsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#rowOrRows}.
 * @param ctx the parse tree
 */
fn exit_rowOrRows(&mut self, _ctx: &RowOrRowsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#setOperation}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#setOperation}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#setOperator}.
 * @param ctx the parse tree
 */
fn enter_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#setOperator}.
 * @param ctx the parse tree
 */
fn exit_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn enter_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn exit_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn enter_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn exit_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#inlineTable}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#inlineTable}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link TrinoParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn enter_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn exit_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link TrinoParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link TrinoParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollup}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollup}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cube}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cube}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link TrinoParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn enter_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn exit_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#orderBy}.
 * @param ctx the parse tree
 */
fn enter_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#orderBy}.
 * @param ctx the parse tree
 */
fn exit_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn enter_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn exit_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link TrinoParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link TrinoParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link TrinoParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link TrinoParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#multiSelect}.
 * @param ctx the parse tree
 */
fn enter_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#multiSelect}.
 * @param ctx the parse tree
 */
fn exit_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#selectStar}.
 * @param ctx the parse tree
 */
fn enter_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#selectStar}.
 * @param ctx the parse tree
 */
fn exit_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link TrinoParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link TrinoParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link TrinoParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link TrinoParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn enter_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn exit_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn enter_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn exit_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn enter_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn exit_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn enter_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn exit_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn enter_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn exit_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#patternRecognitionTarget}.
 * @param ctx the parse tree
 */
fn enter_patternRecognitionTarget(&mut self, _ctx: &PatternRecognitionTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#patternRecognitionTarget}.
 * @param ctx the parse tree
 */
fn exit_patternRecognitionTarget(&mut self, _ctx: &PatternRecognitionTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn enter_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn exit_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn enter_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn exit_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn enter_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn exit_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn enter_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn exit_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#skipTo}.
 * @param ctx the parse tree
 */
fn enter_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#skipTo}.
 * @param ctx the parse tree
 */
fn exit_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn enter_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn exit_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelationTarget(&mut self, _ctx: &AliasedRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelationTarget(&mut self, _ctx: &AliasedRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unnest}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unnest}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_unnest(&mut self, _ctx: &UnnestContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lateral}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lateral}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link TrinoParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link TrinoParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link TrinoParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn enter_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn exit_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#descriptorField}.
 * @param ctx the parse tree
 */
fn enter_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#descriptorField}.
 * @param ctx the parse tree
 */
fn exit_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn enter_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn exit_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link TrinoParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link TrinoParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link TrinoParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code substring}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code substring}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countStar}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countStar}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code array}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code array}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code normalize}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code normalize}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subscript}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subscript}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extract}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extract}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code variable}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code variable}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code position}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code position}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code listagg}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code listagg}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link TrinoParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn enter_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn exit_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn enter_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn exit_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link TrinoParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link TrinoParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn enter_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn exit_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#field}.
 * @param ctx the parse tree
 */
fn enter_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#field}.
 * @param ctx the parse tree
 */
fn exit_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn enter_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn exit_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn enter_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn exit_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn enter_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn exit_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn enter_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn exit_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#processingMode}.
 * @param ctx the parse tree
 */
fn enter_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#processingMode}.
 * @param ctx the parse tree
 */
fn exit_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link TrinoParser#string}.
 * @param ctx the parse tree
 */
fn enter_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link TrinoParser#string}.
 * @param ctx the parse tree
 */
fn exit_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link TrinoParser#string}.
 * @param ctx the parse tree
 */
fn enter_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link TrinoParser#string}.
 * @param ctx the parse tree
 */
fn exit_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#intervalField}.
 * @param ctx the parse tree
 */
fn enter_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#intervalField}.
 * @param ctx the parse tree
 */
fn exit_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#normalForm}.
 * @param ctx the parse tree
 */
fn enter_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#normalForm}.
 * @param ctx the parse tree
 */
fn exit_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn enter_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn exit_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link TrinoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link TrinoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link TrinoParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link TrinoParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link TrinoParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link TrinoParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link TrinoParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link TrinoParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link TrinoParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link TrinoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link TrinoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link TrinoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link TrinoParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code serializable}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code serializable}
 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explainFormat}
 * labeled alternative in {@link TrinoParser#explainOption}.
 * @param ctx the parse tree
 */
fn enter_explainFormat(&mut self, _ctx: &ExplainFormatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explainFormat}
 * labeled alternative in {@link TrinoParser#explainOption}.
 * @param ctx the parse tree
 */
fn exit_explainFormat(&mut self, _ctx: &ExplainFormatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explainType}
 * labeled alternative in {@link TrinoParser#explainOption}.
 * @param ctx the parse tree
 */
fn enter_explainType(&mut self, _ctx: &ExplainTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explainType}
 * labeled alternative in {@link TrinoParser#explainOption}.
 * @param ctx the parse tree
 */
fn exit_explainType(&mut self, _ctx: &ExplainTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link TrinoParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link TrinoParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#pathExpression}.
 * @param ctx the parse tree
 */
fn enter_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#pathExpression}.
 * @param ctx the parse tree
 */
fn exit_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link TrinoParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link TrinoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link TrinoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link TrinoParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link TrinoParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#pathComponent}.
 * @param ctx the parse tree
 */
fn enter_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#pathComponent}.
 * @param ctx the parse tree
 */
fn exit_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn enter_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn exit_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#identifierList}.
 * @param ctx the parse tree
 */
fn enter_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#identifierList}.
 * @param ctx the parse tree
 */
fn exit_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn enter_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn exit_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link TrinoParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentStruct}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentStruct(&mut self, _ctx: &PrestoFunctionArgumentStructContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentStruct}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentStruct(&mut self, _ctx: &PrestoFunctionArgumentStructContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentMap}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentMap(&mut self, _ctx: &PrestoFunctionArgumentMapContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentMap}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentMap(&mut self, _ctx: &PrestoFunctionArgumentMapContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentArray}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentArray(&mut self, _ctx: &PrestoFunctionArgumentArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentArray}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentArray(&mut self, _ctx: &PrestoFunctionArgumentArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentLambda}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentLambda(&mut self, _ctx: &PrestoFunctionArgumentLambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentLambda}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentLambda(&mut self, _ctx: &PrestoFunctionArgumentLambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentInteger}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentInteger(&mut self, _ctx: &PrestoFunctionArgumentIntegerContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentInteger}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentInteger(&mut self, _ctx: &PrestoFunctionArgumentIntegerContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentDefault}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentDefault(&mut self, _ctx: &PrestoFunctionArgumentDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentDefault}
 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentDefault(&mut self, _ctx: &PrestoFunctionArgumentDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#prestoShowFunctionRowField}.
 * @param ctx the parse tree
 */
fn enter_prestoShowFunctionRowField(&mut self, _ctx: &PrestoShowFunctionRowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#prestoShowFunctionRowField}.
 * @param ctx the parse tree
 */
fn exit_prestoShowFunctionRowField(&mut self, _ctx: &PrestoShowFunctionRowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#prestoShowFunctionTypes}.
 * @param ctx the parse tree
 */
fn enter_prestoShowFunctionTypes(&mut self, _ctx: &PrestoShowFunctionTypesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#prestoShowFunctionTypes}.
 * @param ctx the parse tree
 */
fn exit_prestoShowFunctionTypes(&mut self, _ctx: &PrestoShowFunctionTypesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TrinoParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TrinoParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : TrinoListener<'input> }


