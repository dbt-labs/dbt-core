#![allow(nonstandard_style)]
// Generated from Redshift.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::redshiftparser::*;

pub trait RedshiftListener<'input> : ParseTreeListener<'input,RedshiftParserContextType>{
/**
 * Enter a parse tree produced by {@link RedshiftParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn enter_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn exit_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateExternalTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateExternalTable(&mut self, _ctx: &RedshiftCreateExternalTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateExternalTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateExternalTable(&mut self, _ctx: &RedshiftCreateExternalTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateExternalTableAs}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateExternalTableAs(&mut self, _ctx: &RedshiftCreateExternalTableAsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateExternalTableAs}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateExternalTableAs(&mut self, _ctx: &RedshiftCreateExternalTableAsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateTableAsSelect}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateTableAsSelect(&mut self, _ctx: &RedshiftCreateTableAsSelectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateTableAsSelect}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateTableAsSelect(&mut self, _ctx: &RedshiftCreateTableAsSelectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateTable(&mut self, _ctx: &RedshiftCreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateTable(&mut self, _ctx: &RedshiftCreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertInto}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_insertInto(&mut self, _ctx: &InsertIntoContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateMaterializedView(&mut self, _ctx: &RedshiftCreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateMaterializedView(&mut self, _ctx: &RedshiftCreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftCreateView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateView(&mut self, _ctx: &RedshiftCreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftCreateView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateView(&mut self, _ctx: &RedshiftCreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code abort}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_abort(&mut self, _ctx: &AbortContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code abort}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_abort(&mut self, _ctx: &AbortContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code alter}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code alter}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code attach}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_attach(&mut self, _ctx: &AttachContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code attach}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_attach(&mut self, _ctx: &AttachContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code begin}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code begin}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cancel}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_cancel(&mut self, _ctx: &CancelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cancel}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_cancel(&mut self, _ctx: &CancelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code close}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_close(&mut self, _ctx: &CloseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code close}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_close(&mut self, _ctx: &CloseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code copy}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_copy(&mut self, _ctx: &CopyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code copy}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_copy(&mut self, _ctx: &CopyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code set}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code set}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code drop}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code drop}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comment}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comment}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code call}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code call}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deny}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deny}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explain}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explain}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code show}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code show}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code reset}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code reset}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prepare}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prepare}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createExternalSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createExternalSchema(&mut self, _ctx: &CreateExternalSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createExternalSchema}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createExternalSchema(&mut self, _ctx: &CreateExternalSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createGroup}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createGroup(&mut self, _ctx: &CreateGroupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createGroup}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createGroup(&mut self, _ctx: &CreateGroupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createIdentity}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createIdentity(&mut self, _ctx: &CreateIdentityContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createIdentity}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createIdentity(&mut self, _ctx: &CreateIdentityContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createProcedure}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createProcedure(&mut self, _ctx: &CreateProcedureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createProcedure}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createProcedure(&mut self, _ctx: &CreateProcedureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createUser}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createUser(&mut self, _ctx: &CreateUserContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createUser}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createUser(&mut self, _ctx: &CreateUserContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code declare}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_declare(&mut self, _ctx: &DeclareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code declare}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_declare(&mut self, _ctx: &DeclareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code detach}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_detach(&mut self, _ctx: &DetachContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code detach}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_detach(&mut self, _ctx: &DetachContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code end}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_end(&mut self, _ctx: &EndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code end}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_end(&mut self, _ctx: &EndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fetch}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_fetch(&mut self, _ctx: &FetchContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fetch}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_fetch(&mut self, _ctx: &FetchContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lock}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_lock(&mut self, _ctx: &LockContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lock}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_lock(&mut self, _ctx: &LockContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unload}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_unload(&mut self, _ctx: &UnloadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unload}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_unload(&mut self, _ctx: &UnloadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vacuum}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn enter_vacuum(&mut self, _ctx: &VacuumContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vacuum}
 * labeled alternative in {@link RedshiftParser#statement}.
 * @param ctx the parse tree
 */
fn exit_vacuum(&mut self, _ctx: &VacuumContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableElements}.
 * @param ctx the parse tree
 */
fn enter_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableElements}.
 * @param ctx the parse tree
 */
fn exit_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn enter_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn exit_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#redshiftCreateExternalTableClauses}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateExternalTableClauses(&mut self, _ctx: &RedshiftCreateExternalTableClausesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#redshiftCreateExternalTableClauses}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateExternalTableClauses(&mut self, _ctx: &RedshiftCreateExternalTableClausesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#redshiftCreateExternalTableAsClauses}.
 * @param ctx the parse tree
 */
fn enter_redshiftCreateExternalTableAsClauses(&mut self, _ctx: &RedshiftCreateExternalTableAsClausesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#redshiftCreateExternalTableAsClauses}.
 * @param ctx the parse tree
 */
fn exit_redshiftCreateExternalTableAsClauses(&mut self, _ctx: &RedshiftCreateExternalTableAsClausesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#locationSpec}.
 * @param ctx the parse tree
 */
fn enter_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#locationSpec}.
 * @param ctx the parse tree
 */
fn exit_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#partitionedByNameSpec}.
 * @param ctx the parse tree
 */
fn enter_partitionedByNameSpec(&mut self, _ctx: &PartitionedByNameSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#partitionedByNameSpec}.
 * @param ctx the parse tree
 */
fn exit_partitionedByNameSpec(&mut self, _ctx: &PartitionedByNameSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#partitionedByFieldSpec}.
 * @param ctx the parse tree
 */
fn enter_partitionedByFieldSpec(&mut self, _ctx: &PartitionedByFieldSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#partitionedByFieldSpec}.
 * @param ctx the parse tree
 */
fn exit_partitionedByFieldSpec(&mut self, _ctx: &PartitionedByFieldSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn enter_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn exit_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#rowFormatedSpec}.
 * @param ctx the parse tree
 */
fn enter_rowFormatedSpec(&mut self, _ctx: &RowFormatedSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#rowFormatedSpec}.
 * @param ctx the parse tree
 */
fn exit_rowFormatedSpec(&mut self, _ctx: &RowFormatedSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#rowFormatedAndSerdeSpec}.
 * @param ctx the parse tree
 */
fn enter_rowFormatedAndSerdeSpec(&mut self, _ctx: &RowFormatedAndSerdeSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#rowFormatedAndSerdeSpec}.
 * @param ctx the parse tree
 */
fn exit_rowFormatedAndSerdeSpec(&mut self, _ctx: &RowFormatedAndSerdeSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableProperties}.
 * @param ctx the parse tree
 */
fn enter_tableProperties(&mut self, _ctx: &TablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableProperties}.
 * @param ctx the parse tree
 */
fn exit_tableProperties(&mut self, _ctx: &TablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionLanguage}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionLanguage(&mut self, _ctx: &FunctionLanguageContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionLanguage}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionLanguage(&mut self, _ctx: &FunctionLanguageContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionVolatility}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionVolatility(&mut self, _ctx: &FunctionVolatilityContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionVolatility}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionVolatility(&mut self, _ctx: &FunctionVolatilityContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionIAMRole}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionIAMRole(&mut self, _ctx: &FunctionIAMRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionIAMRole}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionIAMRole(&mut self, _ctx: &FunctionIAMRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionLambda}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionLambda(&mut self, _ctx: &FunctionLambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionLambda}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionLambda(&mut self, _ctx: &FunctionLambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionRetryTimeout}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionRetryTimeout(&mut self, _ctx: &FunctionRetryTimeoutContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionRetryTimeout}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionRetryTimeout(&mut self, _ctx: &FunctionRetryTimeoutContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionMaxBatchRows}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionMaxBatchRows(&mut self, _ctx: &FunctionMaxBatchRowsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionMaxBatchRows}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionMaxBatchRows(&mut self, _ctx: &FunctionMaxBatchRowsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionMaxBatchSize}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionMaxBatchSize(&mut self, _ctx: &FunctionMaxBatchSizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionMaxBatchSize}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionMaxBatchSize(&mut self, _ctx: &FunctionMaxBatchSizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSagemaker}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionSagemaker(&mut self, _ctx: &FunctionSagemakerContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSagemaker}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionSagemaker(&mut self, _ctx: &FunctionSagemakerContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionBody}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn enter_functionBody(&mut self, _ctx: &FunctionBodyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionBody}
 * labeled alternative in {@link RedshiftParser#functionPropertySpec}.
 * @param ctx the parse tree
 */
fn exit_functionBody(&mut self, _ctx: &FunctionBodyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn enter_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn exit_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnName}.
 * @param ctx the parse tree
 */
fn enter_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnName}.
 * @param ctx the parse tree
 */
fn exit_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn enter_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn exit_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn enter_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn exit_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnOption}.
 * @param ctx the parse tree
 */
fn enter_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnOption}.
 * @param ctx the parse tree
 */
fn exit_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link RedshiftParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link RedshiftParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnAttributes}.
 * @param ctx the parse tree
 */
fn enter_columnAttributes(&mut self, _ctx: &ColumnAttributesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnAttributes}.
 * @param ctx the parse tree
 */
fn exit_columnAttributes(&mut self, _ctx: &ColumnAttributesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnConstraints}.
 * @param ctx the parse tree
 */
fn enter_columnConstraints(&mut self, _ctx: &ColumnConstraintsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnConstraints}.
 * @param ctx the parse tree
 */
fn exit_columnConstraints(&mut self, _ctx: &ColumnConstraintsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#likeClause}.
 * @param ctx the parse tree
 */
fn enter_likeClause(&mut self, _ctx: &LikeClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#likeClause}.
 * @param ctx the parse tree
 */
fn exit_likeClause(&mut self, _ctx: &LikeClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#redshiftTableAttributes}.
 * @param ctx the parse tree
 */
fn enter_redshiftTableAttributes(&mut self, _ctx: &RedshiftTableAttributesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#redshiftTableAttributes}.
 * @param ctx the parse tree
 */
fn exit_redshiftTableAttributes(&mut self, _ctx: &RedshiftTableAttributesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link RedshiftParser#property}.
 * @param ctx the parse tree
 */
fn enter_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link RedshiftParser#property}.
 * @param ctx the parse tree
 */
fn exit_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link RedshiftParser#property}.
 * @param ctx the parse tree
 */
fn enter_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link RedshiftParser#property}.
 * @param ctx the parse tree
 */
fn exit_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#propertyKey}.
 * @param ctx the parse tree
 */
fn enter_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#propertyKey}.
 * @param ctx the parse tree
 */
fn exit_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link RedshiftParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#queryLimit}.
 * @param ctx the parse tree
 */
fn enter_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#queryLimit}.
 * @param ctx the parse tree
 */
fn exit_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
 * labeled alternative in {@link RedshiftParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn enter_queryLimitTargetRedshiftSnowflake(&mut self, _ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
 * labeled alternative in {@link RedshiftParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn exit_queryLimitTargetRedshiftSnowflake(&mut self, _ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#setOperation}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#setOperation}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#setOperator}.
 * @param ctx the parse tree
 */
fn enter_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#setOperator}.
 * @param ctx the parse tree
 */
fn exit_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn enter_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn exit_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn enter_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn exit_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#inlineTable}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#inlineTable}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link RedshiftParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn enter_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn exit_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link RedshiftParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link RedshiftParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollup}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollup}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cube}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cube}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link RedshiftParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn enter_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn exit_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#orderBy}.
 * @param ctx the parse tree
 */
fn enter_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#orderBy}.
 * @param ctx the parse tree
 */
fn exit_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn enter_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn exit_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link RedshiftParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link RedshiftParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link RedshiftParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link RedshiftParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#multiSelect}.
 * @param ctx the parse tree
 */
fn enter_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#multiSelect}.
 * @param ctx the parse tree
 */
fn exit_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#selectStar}.
 * @param ctx the parse tree
 */
fn enter_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#selectStar}.
 * @param ctx the parse tree
 */
fn exit_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link RedshiftParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link RedshiftParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link RedshiftParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link RedshiftParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn enter_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn exit_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn enter_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn exit_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn enter_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn exit_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotedRelationDefault}
 * labeled alternative in {@link RedshiftParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelationDefault(&mut self, _ctx: &PivotedRelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotedRelationDefault}
 * labeled alternative in {@link RedshiftParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelationDefault(&mut self, _ctx: &PivotedRelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedPivotedRelation}
 * labeled alternative in {@link RedshiftParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedPivotedRelation(&mut self, _ctx: &ParenthesizedPivotedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedPivotedRelation}
 * labeled alternative in {@link RedshiftParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedPivotedRelation(&mut self, _ctx: &ParenthesizedPivotedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn enter_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn exit_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn enter_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn exit_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntoDefault}
 * labeled alternative in {@link RedshiftParser#pivotInto}.
 * @param ctx the parse tree
 */
fn enter_pivotIntoDefault(&mut self, _ctx: &PivotIntoDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntoDefault}
 * labeled alternative in {@link RedshiftParser#pivotInto}.
 * @param ctx the parse tree
 */
fn exit_pivotIntoDefault(&mut self, _ctx: &PivotIntoDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn enter_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn exit_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link RedshiftParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link RedshiftParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link RedshiftParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link RedshiftParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivot}
 * labeled alternative in {@link RedshiftParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivot}
 * labeled alternative in {@link RedshiftParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link RedshiftParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link RedshiftParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link RedshiftParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link RedshiftParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link RedshiftParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link RedshiftParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#partitionColumn}.
 * @param ctx the parse tree
 */
fn enter_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#partitionColumn}.
 * @param ctx the parse tree
 */
fn exit_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#partitionColumns}.
 * @param ctx the parse tree
 */
fn enter_partitionColumns(&mut self, _ctx: &PartitionColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#partitionColumns}.
 * @param ctx the parse tree
 */
fn exit_partitionColumns(&mut self, _ctx: &PartitionColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code aliased}
 * labeled alternative in {@link RedshiftParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code aliased}
 * labeled alternative in {@link RedshiftParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code objectUnpivot}
 * labeled alternative in {@link RedshiftParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_objectUnpivot(&mut self, _ctx: &ObjectUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code objectUnpivot}
 * labeled alternative in {@link RedshiftParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_objectUnpivot(&mut self, _ctx: &ObjectUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link RedshiftParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link RedshiftParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link RedshiftParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link RedshiftParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link RedshiftParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link RedshiftParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn enter_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn exit_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#descriptorField}.
 * @param ctx the parse tree
 */
fn enter_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#descriptorField}.
 * @param ctx the parse tree
 */
fn exit_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn enter_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn exit_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link RedshiftParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link RedshiftParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link RedshiftParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link RedshiftParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link RedshiftParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn enter_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn exit_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link RedshiftParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link RedshiftParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code redshiftExtract}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_redshiftExtract(&mut self, _ctx: &RedshiftExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code redshiftExtract}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_redshiftExtract(&mut self, _ctx: &RedshiftExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code firstValueFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_firstValueFunction(&mut self, _ctx: &FirstValueFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code firstValueFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_firstValueFunction(&mut self, _ctx: &FirstValueFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonValue}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValue(&mut self, _ctx: &JsonValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZonePrimary}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZonePrimary(&mut self, _ctx: &AtTimeZonePrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZonePrimary}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZonePrimary(&mut self, _ctx: &AtTimeZonePrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code convert}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_convert(&mut self, _ctx: &ConvertContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code convert}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_convert(&mut self, _ctx: &ConvertContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code substring}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code substring}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countStar}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countStar}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionParameterColumnReference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionParameterColumnReference(&mut self, _ctx: &FunctionParameterColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionParameterColumnReference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionParameterColumnReference(&mut self, _ctx: &FunctionParameterColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code normalize}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code normalize}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonObject}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonObject(&mut self, _ctx: &JsonObjectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonArray}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonArray(&mut self, _ctx: &JsonArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subscript}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subscript}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonExists}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonExists(&mut self, _ctx: &JsonExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonQuery}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonQuery(&mut self, _ctx: &JsonQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code approximateFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_approximateFunction(&mut self, _ctx: &ApproximateFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code approximateFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_approximateFunction(&mut self, _ctx: &ApproximateFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code variable}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code variable}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code position}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code position}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code listagg}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code listagg}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link RedshiftParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn enter_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn exit_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn enter_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn exit_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link RedshiftParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link RedshiftParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link RedshiftParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link RedshiftParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn enter_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn exit_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#namedParameter}.
 * @param ctx the parse tree
 */
fn enter_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#namedParameter}.
 * @param ctx the parse tree
 */
fn exit_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#field}.
 * @param ctx the parse tree
 */
fn enter_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#field}.
 * @param ctx the parse tree
 */
fn exit_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn enter_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonPathInvocation}.
 * @param ctx the parse tree
 */
fn exit_jsonPathInvocation(&mut self, _ctx: &JsonPathInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonValueExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonValueExpression(&mut self, _ctx: &JsonValueExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn enter_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonRepresentation}.
 * @param ctx the parse tree
 */
fn exit_jsonRepresentation(&mut self, _ctx: &JsonRepresentationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn enter_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonArgument}.
 * @param ctx the parse tree
 */
fn exit_jsonArgument(&mut self, _ctx: &JsonArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonExistsErrorBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonExistsErrorBehavior(&mut self, _ctx: &JsonExistsErrorBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonValueBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonValueBehavior(&mut self, _ctx: &JsonValueBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonQueryWrapperBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryWrapperBehavior(&mut self, _ctx: &JsonQueryWrapperBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn enter_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonQueryBehavior}.
 * @param ctx the parse tree
 */
fn exit_jsonQueryBehavior(&mut self, _ctx: &JsonQueryBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn enter_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#jsonObjectMember}.
 * @param ctx the parse tree
 */
fn exit_jsonObjectMember(&mut self, _ctx: &JsonObjectMemberContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#processingMode}.
 * @param ctx the parse tree
 */
fn enter_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#processingMode}.
 * @param ctx the parse tree
 */
fn exit_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn enter_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn exit_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dollarQuotedStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn enter_dollarQuotedStringLiteral(&mut self, _ctx: &DollarQuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dollarQuotedStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn exit_dollarQuotedStringLiteral(&mut self, _ctx: &DollarQuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn enter_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link RedshiftParser#string}.
 * @param ctx the parse tree
 */
fn exit_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#intervalField}.
 * @param ctx the parse tree
 */
fn enter_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#intervalField}.
 * @param ctx the parse tree
 */
fn exit_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#normalForm}.
 * @param ctx the parse tree
 */
fn enter_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#normalForm}.
 * @param ctx the parse tree
 */
fn exit_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn enter_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn exit_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateTimeWithTzType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_dateTimeWithTzType(&mut self, _ctx: &DateTimeWithTzTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateTimeWithTzType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_dateTimeWithTzType(&mut self, _ctx: &DateTimeWithTzTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code characterVarying}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_characterVarying(&mut self, _ctx: &CharacterVaryingContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code characterVarying}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_characterVarying(&mut self, _ctx: &CharacterVaryingContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryVarying}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_binaryVarying(&mut self, _ctx: &BinaryVaryingContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryVarying}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_binaryVarying(&mut self, _ctx: &BinaryVaryingContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn enter_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link RedshiftParser#type_}.
 * @param ctx the parse tree
 */
fn exit_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link RedshiftParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link RedshiftParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link RedshiftParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link RedshiftParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link RedshiftParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link RedshiftParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link RedshiftParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link RedshiftParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code serializable}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code serializable}
 * labeled alternative in {@link RedshiftParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link RedshiftParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link RedshiftParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pathExpression}.
 * @param ctx the parse tree
 */
fn enter_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pathExpression}.
 * @param ctx the parse tree
 */
fn exit_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link RedshiftParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code digitIdentifier}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_digitIdentifier(&mut self, _ctx: &DigitIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code digitIdentifier}
 * labeled alternative in {@link RedshiftParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_digitIdentifier(&mut self, _ctx: &DigitIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#pathComponent}.
 * @param ctx the parse tree
 */
fn enter_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#pathComponent}.
 * @param ctx the parse tree
 */
fn exit_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn enter_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn exit_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link RedshiftParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn enter_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn exit_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Enter a parse tree produced by {@link RedshiftParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link RedshiftParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : RedshiftListener<'input> }


