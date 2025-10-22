#![allow(nonstandard_style)]
// Generated from Databricks.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::databricksparser::*;

pub trait DatabricksListener<'input> : ParseTreeListener<'input,DatabricksParserContextType>{
/**
 * Enter a parse tree produced by {@link DatabricksParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn enter_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn exit_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTableAsSelect}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTableAsSelect(&mut self, _ctx: &CreateTableAsSelectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTable(&mut self, _ctx: &CreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTableLike}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTableLike(&mut self, _ctx: &CreateTableLikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTableLike}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTableLike(&mut self, _ctx: &CreateTableLikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dmlStatement}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dmlStatement(&mut self, _ctx: &DmlStatementContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dmlStatement}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dmlStatement(&mut self, _ctx: &DmlStatementContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFunction}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFunction(&mut self, _ctx: &CreateFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code set}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code set}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code drop}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code drop}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comment}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comment}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explain}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explain}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code show}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code show}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code reset}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code reset}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code alter}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code alter}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code optimize}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn enter_optimize(&mut self, _ctx: &OptimizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code optimize}
 * labeled alternative in {@link DatabricksParser#statement}.
 * @param ctx the parse tree
 */
fn exit_optimize(&mut self, _ctx: &OptimizeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableElements}.
 * @param ctx the parse tree
 */
fn enter_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableElements}.
 * @param ctx the parse tree
 */
fn exit_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#identifierReference}.
 * @param ctx the parse tree
 */
fn enter_identifierReference(&mut self, _ctx: &IdentifierReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#identifierReference}.
 * @param ctx the parse tree
 */
fn exit_identifierReference(&mut self, _ctx: &IdentifierReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#identifierCommentList}.
 * @param ctx the parse tree
 */
fn enter_identifierCommentList(&mut self, _ctx: &IdentifierCommentListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#identifierCommentList}.
 * @param ctx the parse tree
 */
fn exit_identifierCommentList(&mut self, _ctx: &IdentifierCommentListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#identifierComment}.
 * @param ctx the parse tree
 */
fn enter_identifierComment(&mut self, _ctx: &IdentifierCommentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#identifierComment}.
 * @param ctx the parse tree
 */
fn exit_identifierComment(&mut self, _ctx: &IdentifierCommentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#schemaBinding}.
 * @param ctx the parse tree
 */
fn enter_schemaBinding(&mut self, _ctx: &SchemaBindingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#schemaBinding}.
 * @param ctx the parse tree
 */
fn exit_schemaBinding(&mut self, _ctx: &SchemaBindingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#createTableClauses}.
 * @param ctx the parse tree
 */
fn enter_createTableClauses(&mut self, _ctx: &CreateTableClausesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#createTableClauses}.
 * @param ctx the parse tree
 */
fn exit_createTableClauses(&mut self, _ctx: &CreateTableClausesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableProvider}.
 * @param ctx the parse tree
 */
fn enter_tableProvider(&mut self, _ctx: &TableProviderContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableProvider}.
 * @param ctx the parse tree
 */
fn exit_tableProvider(&mut self, _ctx: &TableProviderContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionTransform}
 * labeled alternative in {@link DatabricksParser#partitionField}.
 * @param ctx the parse tree
 */
fn enter_partitionTransform(&mut self, _ctx: &PartitionTransformContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionTransform}
 * labeled alternative in {@link DatabricksParser#partitionField}.
 * @param ctx the parse tree
 */
fn exit_partitionTransform(&mut self, _ctx: &PartitionTransformContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionColumn}
 * labeled alternative in {@link DatabricksParser#partitionField}.
 * @param ctx the parse tree
 */
fn enter_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionColumn}
 * labeled alternative in {@link DatabricksParser#partitionField}.
 * @param ctx the parse tree
 */
fn exit_partitionColumn(&mut self, _ctx: &PartitionColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identityTransform}
 * labeled alternative in {@link DatabricksParser#transform}.
 * @param ctx the parse tree
 */
fn enter_identityTransform(&mut self, _ctx: &IdentityTransformContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identityTransform}
 * labeled alternative in {@link DatabricksParser#transform}.
 * @param ctx the parse tree
 */
fn exit_identityTransform(&mut self, _ctx: &IdentityTransformContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code applyTransform}
 * labeled alternative in {@link DatabricksParser#transform}.
 * @param ctx the parse tree
 */
fn enter_applyTransform(&mut self, _ctx: &ApplyTransformContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code applyTransform}
 * labeled alternative in {@link DatabricksParser#transform}.
 * @param ctx the parse tree
 */
fn exit_applyTransform(&mut self, _ctx: &ApplyTransformContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#transformArgument}.
 * @param ctx the parse tree
 */
fn enter_transformArgument(&mut self, _ctx: &TransformArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#transformArgument}.
 * @param ctx the parse tree
 */
fn exit_transformArgument(&mut self, _ctx: &TransformArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#colType}.
 * @param ctx the parse tree
 */
fn enter_colType(&mut self, _ctx: &ColTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#colType}.
 * @param ctx the parse tree
 */
fn exit_colType(&mut self, _ctx: &ColTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#skewSpec}.
 * @param ctx the parse tree
 */
fn enter_skewSpec(&mut self, _ctx: &SkewSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#skewSpec}.
 * @param ctx the parse tree
 */
fn exit_skewSpec(&mut self, _ctx: &SkewSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#clusterBySpec}.
 * @param ctx the parse tree
 */
fn enter_clusterBySpec(&mut self, _ctx: &ClusterBySpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#clusterBySpec}.
 * @param ctx the parse tree
 */
fn exit_clusterBySpec(&mut self, _ctx: &ClusterBySpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#bucketSpec}.
 * @param ctx the parse tree
 */
fn enter_bucketSpec(&mut self, _ctx: &BucketSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#bucketSpec}.
 * @param ctx the parse tree
 */
fn exit_bucketSpec(&mut self, _ctx: &BucketSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#constantList}.
 * @param ctx the parse tree
 */
fn enter_constantList(&mut self, _ctx: &ConstantListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#constantList}.
 * @param ctx the parse tree
 */
fn exit_constantList(&mut self, _ctx: &ConstantListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#nestedConstantList}.
 * @param ctx the parse tree
 */
fn enter_nestedConstantList(&mut self, _ctx: &NestedConstantListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#nestedConstantList}.
 * @param ctx the parse tree
 */
fn exit_nestedConstantList(&mut self, _ctx: &NestedConstantListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowFormatSerde}
 * labeled alternative in {@link DatabricksParser#rowFormat}.
 * @param ctx the parse tree
 */
fn enter_rowFormatSerde(&mut self, _ctx: &RowFormatSerdeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowFormatSerde}
 * labeled alternative in {@link DatabricksParser#rowFormat}.
 * @param ctx the parse tree
 */
fn exit_rowFormatSerde(&mut self, _ctx: &RowFormatSerdeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowFormatDelimited}
 * labeled alternative in {@link DatabricksParser#rowFormat}.
 * @param ctx the parse tree
 */
fn enter_rowFormatDelimited(&mut self, _ctx: &RowFormatDelimitedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowFormatDelimited}
 * labeled alternative in {@link DatabricksParser#rowFormat}.
 * @param ctx the parse tree
 */
fn exit_rowFormatDelimited(&mut self, _ctx: &RowFormatDelimitedContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn enter_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn exit_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableFileFormat}
 * labeled alternative in {@link DatabricksParser#fileFormat}.
 * @param ctx the parse tree
 */
fn enter_tableFileFormat(&mut self, _ctx: &TableFileFormatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableFileFormat}
 * labeled alternative in {@link DatabricksParser#fileFormat}.
 * @param ctx the parse tree
 */
fn exit_tableFileFormat(&mut self, _ctx: &TableFileFormatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code genericFileFormat}
 * labeled alternative in {@link DatabricksParser#fileFormat}.
 * @param ctx the parse tree
 */
fn enter_genericFileFormat(&mut self, _ctx: &GenericFileFormatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code genericFileFormat}
 * labeled alternative in {@link DatabricksParser#fileFormat}.
 * @param ctx the parse tree
 */
fn exit_genericFileFormat(&mut self, _ctx: &GenericFileFormatContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#storageHandler}.
 * @param ctx the parse tree
 */
fn enter_storageHandler(&mut self, _ctx: &StorageHandlerContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#storageHandler}.
 * @param ctx the parse tree
 */
fn exit_storageHandler(&mut self, _ctx: &StorageHandlerContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#locationSpec}.
 * @param ctx the parse tree
 */
fn enter_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#locationSpec}.
 * @param ctx the parse tree
 */
fn exit_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#literalType}.
 * @param ctx the parse tree
 */
fn enter_literalType(&mut self, _ctx: &LiteralTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#literalType}.
 * @param ctx the parse tree
 */
fn exit_literalType(&mut self, _ctx: &LiteralTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleInsertQuery}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn enter_singleInsertQuery(&mut self, _ctx: &SingleInsertQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleInsertQuery}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn exit_singleInsertQuery(&mut self, _ctx: &SingleInsertQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiInsertQuery}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn enter_multiInsertQuery(&mut self, _ctx: &MultiInsertQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiInsertQuery}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn exit_multiInsertQuery(&mut self, _ctx: &MultiInsertQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deleteFromTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn enter_deleteFromTable(&mut self, _ctx: &DeleteFromTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deleteFromTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn exit_deleteFromTable(&mut self, _ctx: &DeleteFromTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code updateTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn enter_updateTable(&mut self, _ctx: &UpdateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code updateTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn exit_updateTable(&mut self, _ctx: &UpdateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mergeIntoTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn enter_mergeIntoTable(&mut self, _ctx: &MergeIntoTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mergeIntoTable}
 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
 * @param ctx the parse tree
 */
fn exit_mergeIntoTable(&mut self, _ctx: &MergeIntoTableContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#ctes}.
 * @param ctx the parse tree
 */
fn enter_ctes(&mut self, _ctx: &CtesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#ctes}.
 * @param ctx the parse tree
 */
fn exit_ctes(&mut self, _ctx: &CtesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertOverwriteTable}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn enter_insertOverwriteTable(&mut self, _ctx: &InsertOverwriteTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertOverwriteTable}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn exit_insertOverwriteTable(&mut self, _ctx: &InsertOverwriteTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertIntoTable}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn enter_insertIntoTable(&mut self, _ctx: &InsertIntoTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertIntoTable}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn exit_insertIntoTable(&mut self, _ctx: &InsertIntoTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertIntoReplaceWhere}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn enter_insertIntoReplaceWhere(&mut self, _ctx: &InsertIntoReplaceWhereContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertIntoReplaceWhere}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn exit_insertIntoReplaceWhere(&mut self, _ctx: &InsertIntoReplaceWhereContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertOverwriteHiveDir}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn enter_insertOverwriteHiveDir(&mut self, _ctx: &InsertOverwriteHiveDirContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertOverwriteHiveDir}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn exit_insertOverwriteHiveDir(&mut self, _ctx: &InsertOverwriteHiveDirContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code insertOverwriteDir}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn enter_insertOverwriteDir(&mut self, _ctx: &InsertOverwriteDirContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code insertOverwriteDir}
 * labeled alternative in {@link DatabricksParser#insertInto}.
 * @param ctx the parse tree
 */
fn exit_insertOverwriteDir(&mut self, _ctx: &InsertOverwriteDirContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#multiInsertQueryBody}.
 * @param ctx the parse tree
 */
fn enter_multiInsertQueryBody(&mut self, _ctx: &MultiInsertQueryBodyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#multiInsertQueryBody}.
 * @param ctx the parse tree
 */
fn exit_multiInsertQueryBody(&mut self, _ctx: &MultiInsertQueryBodyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableAlias}.
 * @param ctx the parse tree
 */
fn enter_tableAlias(&mut self, _ctx: &TableAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableAlias}.
 * @param ctx the parse tree
 */
fn exit_tableAlias(&mut self, _ctx: &TableAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#whereClause}.
 * @param ctx the parse tree
 */
fn enter_whereClause(&mut self, _ctx: &WhereClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#whereClause}.
 * @param ctx the parse tree
 */
fn exit_whereClause(&mut self, _ctx: &WhereClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setClause}.
 * @param ctx the parse tree
 */
fn enter_setClause(&mut self, _ctx: &SetClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setClause}.
 * @param ctx the parse tree
 */
fn exit_setClause(&mut self, _ctx: &SetClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#matchedClause}.
 * @param ctx the parse tree
 */
fn enter_matchedClause(&mut self, _ctx: &MatchedClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#matchedClause}.
 * @param ctx the parse tree
 */
fn exit_matchedClause(&mut self, _ctx: &MatchedClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#notMatchedClause}.
 * @param ctx the parse tree
 */
fn enter_notMatchedClause(&mut self, _ctx: &NotMatchedClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#notMatchedClause}.
 * @param ctx the parse tree
 */
fn exit_notMatchedClause(&mut self, _ctx: &NotMatchedClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#notMatchedBySourceClause}.
 * @param ctx the parse tree
 */
fn enter_notMatchedBySourceClause(&mut self, _ctx: &NotMatchedBySourceClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#notMatchedBySourceClause}.
 * @param ctx the parse tree
 */
fn exit_notMatchedBySourceClause(&mut self, _ctx: &NotMatchedBySourceClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#optionsClause}.
 * @param ctx the parse tree
 */
fn enter_optionsClause(&mut self, _ctx: &OptionsClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#optionsClause}.
 * @param ctx the parse tree
 */
fn exit_optionsClause(&mut self, _ctx: &OptionsClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#partitionSpec}.
 * @param ctx the parse tree
 */
fn enter_partitionSpec(&mut self, _ctx: &PartitionSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#partitionSpec}.
 * @param ctx the parse tree
 */
fn exit_partitionSpec(&mut self, _ctx: &PartitionSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#lateralView}.
 * @param ctx the parse tree
 */
fn enter_lateralView(&mut self, _ctx: &LateralViewContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#lateralView}.
 * @param ctx the parse tree
 */
fn exit_lateralView(&mut self, _ctx: &LateralViewContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#fromStatementBody}.
 * @param ctx the parse tree
 */
fn enter_fromStatementBody(&mut self, _ctx: &FromStatementBodyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#fromStatementBody}.
 * @param ctx the parse tree
 */
fn exit_fromStatementBody(&mut self, _ctx: &FromStatementBodyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#queryOrganization}.
 * @param ctx the parse tree
 */
fn enter_queryOrganization(&mut self, _ctx: &QueryOrganizationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#queryOrganization}.
 * @param ctx the parse tree
 */
fn exit_queryOrganization(&mut self, _ctx: &QueryOrganizationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#assignmentList}.
 * @param ctx the parse tree
 */
fn enter_assignmentList(&mut self, _ctx: &AssignmentListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#assignmentList}.
 * @param ctx the parse tree
 */
fn exit_assignmentList(&mut self, _ctx: &AssignmentListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#assignment}.
 * @param ctx the parse tree
 */
fn enter_assignment(&mut self, _ctx: &AssignmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#assignment}.
 * @param ctx the parse tree
 */
fn exit_assignment(&mut self, _ctx: &AssignmentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#matchedAction}.
 * @param ctx the parse tree
 */
fn enter_matchedAction(&mut self, _ctx: &MatchedActionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#matchedAction}.
 * @param ctx the parse tree
 */
fn exit_matchedAction(&mut self, _ctx: &MatchedActionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#notMatchedAction}.
 * @param ctx the parse tree
 */
fn enter_notMatchedAction(&mut self, _ctx: &NotMatchedActionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#notMatchedAction}.
 * @param ctx the parse tree
 */
fn exit_notMatchedAction(&mut self, _ctx: &NotMatchedActionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#notMatchedBySourceAction}.
 * @param ctx the parse tree
 */
fn enter_notMatchedBySourceAction(&mut self, _ctx: &NotMatchedBySourceActionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#notMatchedBySourceAction}.
 * @param ctx the parse tree
 */
fn exit_notMatchedBySourceAction(&mut self, _ctx: &NotMatchedBySourceActionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#partitionVal}.
 * @param ctx the parse tree
 */
fn enter_partitionVal(&mut self, _ctx: &PartitionValContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#partitionVal}.
 * @param ctx the parse tree
 */
fn exit_partitionVal(&mut self, _ctx: &PartitionValContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#namedExpressionSeq}.
 * @param ctx the parse tree
 */
fn enter_namedExpressionSeq(&mut self, _ctx: &NamedExpressionSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#namedExpressionSeq}.
 * @param ctx the parse tree
 */
fn exit_namedExpressionSeq(&mut self, _ctx: &NamedExpressionSeqContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#namedExpression}.
 * @param ctx the parse tree
 */
fn enter_namedExpression(&mut self, _ctx: &NamedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#namedExpression}.
 * @param ctx the parse tree
 */
fn exit_namedExpression(&mut self, _ctx: &NamedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn enter_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn exit_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#transformClause}.
 * @param ctx the parse tree
 */
fn enter_transformClause(&mut self, _ctx: &TransformClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#transformClause}.
 * @param ctx the parse tree
 */
fn exit_transformClause(&mut self, _ctx: &TransformClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#selectClause}.
 * @param ctx the parse tree
 */
fn enter_selectClause(&mut self, _ctx: &SelectClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#selectClause}.
 * @param ctx the parse tree
 */
fn exit_selectClause(&mut self, _ctx: &SelectClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#havingClause}.
 * @param ctx the parse tree
 */
fn enter_havingClause(&mut self, _ctx: &HavingClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#havingClause}.
 * @param ctx the parse tree
 */
fn exit_havingClause(&mut self, _ctx: &HavingClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#multipartIdentifierList}.
 * @param ctx the parse tree
 */
fn enter_multipartIdentifierList(&mut self, _ctx: &MultipartIdentifierListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#multipartIdentifierList}.
 * @param ctx the parse tree
 */
fn exit_multipartIdentifierList(&mut self, _ctx: &MultipartIdentifierListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#expressionSeq}.
 * @param ctx the parse tree
 */
fn enter_expressionSeq(&mut self, _ctx: &ExpressionSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#expressionSeq}.
 * @param ctx the parse tree
 */
fn exit_expressionSeq(&mut self, _ctx: &ExpressionSeqContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#colTypeList}.
 * @param ctx the parse tree
 */
fn enter_colTypeList(&mut self, _ctx: &ColTypeListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#colTypeList}.
 * @param ctx the parse tree
 */
fn exit_colTypeList(&mut self, _ctx: &ColTypeListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#hint}.
 * @param ctx the parse tree
 */
fn enter_hint(&mut self, _ctx: &HintContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#hint}.
 * @param ctx the parse tree
 */
fn exit_hint(&mut self, _ctx: &HintContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#hintStatement}.
 * @param ctx the parse tree
 */
fn enter_hintStatement(&mut self, _ctx: &HintStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#hintStatement}.
 * @param ctx the parse tree
 */
fn exit_hintStatement(&mut self, _ctx: &HintStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn enter_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn exit_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnDefinitionForView}.
 * @param ctx the parse tree
 */
fn enter_columnDefinitionForView(&mut self, _ctx: &ColumnDefinitionForViewContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnDefinitionForView}.
 * @param ctx the parse tree
 */
fn exit_columnDefinitionForView(&mut self, _ctx: &ColumnDefinitionForViewContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#fieldDefinitions}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinitions(&mut self, _ctx: &FieldDefinitionsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#fieldDefinitions}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinitions(&mut self, _ctx: &FieldDefinitionsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnName}.
 * @param ctx the parse tree
 */
fn enter_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnName}.
 * @param ctx the parse tree
 */
fn exit_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn enter_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn exit_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#colDefinitionOption}.
 * @param ctx the parse tree
 */
fn enter_colDefinitionOption(&mut self, _ctx: &ColDefinitionOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#colDefinitionOption}.
 * @param ctx the parse tree
 */
fn exit_colDefinitionOption(&mut self, _ctx: &ColDefinitionOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#generationExpression}.
 * @param ctx the parse tree
 */
fn enter_generationExpression(&mut self, _ctx: &GenerationExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#generationExpression}.
 * @param ctx the parse tree
 */
fn exit_generationExpression(&mut self, _ctx: &GenerationExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#defaultExpression}.
 * @param ctx the parse tree
 */
fn enter_defaultExpression(&mut self, _ctx: &DefaultExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#defaultExpression}.
 * @param ctx the parse tree
 */
fn exit_defaultExpression(&mut self, _ctx: &DefaultExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn enter_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn exit_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnOption}.
 * @param ctx the parse tree
 */
fn enter_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnOption}.
 * @param ctx the parse tree
 */
fn exit_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link DatabricksParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link DatabricksParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link DatabricksParser#property}.
 * @param ctx the parse tree
 */
fn enter_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link DatabricksParser#property}.
 * @param ctx the parse tree
 */
fn exit_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link DatabricksParser#property}.
 * @param ctx the parse tree
 */
fn enter_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link DatabricksParser#property}.
 * @param ctx the parse tree
 */
fn exit_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#propertyKey}.
 * @param ctx the parse tree
 */
fn enter_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#propertyKey}.
 * @param ctx the parse tree
 */
fn exit_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link DatabricksParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#queryLimit}.
 * @param ctx the parse tree
 */
fn enter_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#queryLimit}.
 * @param ctx the parse tree
 */
fn exit_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryLimitTargetDatabricks}
 * labeled alternative in {@link DatabricksParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn enter_queryLimitTargetDatabricks(&mut self, _ctx: &QueryLimitTargetDatabricksContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryLimitTargetDatabricks}
 * labeled alternative in {@link DatabricksParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn exit_queryLimitTargetDatabricks(&mut self, _ctx: &QueryLimitTargetDatabricksContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#windowClause}.
 * @param ctx the parse tree
 */
fn enter_windowClause(&mut self, _ctx: &WindowClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#windowClause}.
 * @param ctx the parse tree
 */
fn exit_windowClause(&mut self, _ctx: &WindowClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setOperation}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setOperation}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setOperator}.
 * @param ctx the parse tree
 */
fn enter_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setOperator}.
 * @param ctx the parse tree
 */
fn exit_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn enter_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn exit_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn enter_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn exit_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#inlineTable}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#inlineTable}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault1}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault1(&mut self, _ctx: &InlineTableDefault1Context<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link DatabricksParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn enter_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn exit_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByWith}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByWith(&mut self, _ctx: &GroupByWithContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByWith}
 * labeled alternative in {@link DatabricksParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByWith(&mut self, _ctx: &GroupByWithContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpElementAnalytics}
 * labeled alternative in {@link DatabricksParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_grpElementAnalytics(&mut self, _ctx: &GrpElementAnalyticsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpElementAnalytics}
 * labeled alternative in {@link DatabricksParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_grpElementAnalytics(&mut self, _ctx: &GrpElementAnalyticsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpElementExpression}
 * labeled alternative in {@link DatabricksParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_grpElementExpression(&mut self, _ctx: &GrpElementExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpElementExpression}
 * labeled alternative in {@link DatabricksParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_grpElementExpression(&mut self, _ctx: &GrpElementExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpAnalyticsSugar}
 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
 * @param ctx the parse tree
 */
fn enter_grpAnalyticsSugar(&mut self, _ctx: &GrpAnalyticsSugarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpAnalyticsSugar}
 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
 * @param ctx the parse tree
 */
fn exit_grpAnalyticsSugar(&mut self, _ctx: &GrpAnalyticsSugarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpAnalyticsSets}
 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
 * @param ctx the parse tree
 */
fn enter_grpAnalyticsSets(&mut self, _ctx: &GrpAnalyticsSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpAnalyticsSets}
 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
 * @param ctx the parse tree
 */
fn exit_grpAnalyticsSets(&mut self, _ctx: &GrpAnalyticsSetsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpSetsElementAnalytics}
 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
 * @param ctx the parse tree
 */
fn enter_grpSetsElementAnalytics(&mut self, _ctx: &GrpSetsElementAnalyticsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpSetsElementAnalytics}
 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
 * @param ctx the parse tree
 */
fn exit_grpSetsElementAnalytics(&mut self, _ctx: &GrpSetsElementAnalyticsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grpSetsElementSet}
 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
 * @param ctx the parse tree
 */
fn enter_grpSetsElementSet(&mut self, _ctx: &GrpSetsElementSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grpSetsElementSet}
 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
 * @param ctx the parse tree
 */
fn exit_grpSetsElementSet(&mut self, _ctx: &GrpSetsElementSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn enter_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn exit_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#orderBy}.
 * @param ctx the parse tree
 */
fn enter_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#orderBy}.
 * @param ctx the parse tree
 */
fn exit_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn enter_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn exit_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link DatabricksParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link DatabricksParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link DatabricksParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link DatabricksParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code structItemSingle}
 * labeled alternative in {@link DatabricksParser#structItem}.
 * @param ctx the parse tree
 */
fn enter_structItemSingle(&mut self, _ctx: &StructItemSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code structItemSingle}
 * labeled alternative in {@link DatabricksParser#structItem}.
 * @param ctx the parse tree
 */
fn exit_structItemSingle(&mut self, _ctx: &StructItemSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code structItemMulti}
 * labeled alternative in {@link DatabricksParser#structItem}.
 * @param ctx the parse tree
 */
fn enter_structItemMulti(&mut self, _ctx: &StructItemMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code structItemMulti}
 * labeled alternative in {@link DatabricksParser#structItem}.
 * @param ctx the parse tree
 */
fn exit_structItemMulti(&mut self, _ctx: &StructItemMultiContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#multiSelect}.
 * @param ctx the parse tree
 */
fn enter_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#multiSelect}.
 * @param ctx the parse tree
 */
fn exit_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#selectStar}.
 * @param ctx the parse tree
 */
fn enter_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#selectStar}.
 * @param ctx the parse tree
 */
fn exit_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_sampledRelationTarget(&mut self, _ctx: &SampledRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn enter_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn exit_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#sample}.
 * @param ctx the parse tree
 */
fn enter_sample(&mut self, _ctx: &SampleContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#sample}.
 * @param ctx the parse tree
 */
fn exit_sample(&mut self, _ctx: &SampleContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn enter_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn exit_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code sampleByPercentile}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleByPercentile(&mut self, _ctx: &SampleByPercentileContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code sampleByPercentile}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleByPercentile(&mut self, _ctx: &SampleByPercentileContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code sampleByRows}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleByRows(&mut self, _ctx: &SampleByRowsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code sampleByRows}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleByRows(&mut self, _ctx: &SampleByRowsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code sampleByBucket}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleByBucket(&mut self, _ctx: &SampleByBucketContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code sampleByBucket}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleByBucket(&mut self, _ctx: &SampleByBucketContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code sampleByBytes}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleByBytes(&mut self, _ctx: &SampleByBytesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code sampleByBytes}
 * labeled alternative in {@link DatabricksParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleByBytes(&mut self, _ctx: &SampleByBytesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelationTarget(&mut self, _ctx: &PivotedRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#lateralViewRelation}.
 * @param ctx the parse tree
 */
fn enter_lateralViewRelation(&mut self, _ctx: &LateralViewRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#lateralViewRelation}.
 * @param ctx the parse tree
 */
fn exit_lateralViewRelation(&mut self, _ctx: &LateralViewRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lateralViewRelationTargetDefault}
 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_lateralViewRelationTargetDefault(&mut self, _ctx: &LateralViewRelationTargetDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lateralViewRelationTargetDefault}
 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_lateralViewRelationTargetDefault(&mut self, _ctx: &LateralViewRelationTargetDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lateralViewRelationTargetIncremental}
 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_lateralViewRelationTargetIncremental(&mut self, _ctx: &LateralViewRelationTargetIncrementalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lateralViewRelationTargetIncremental}
 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_lateralViewRelationTargetIncremental(&mut self, _ctx: &LateralViewRelationTargetIncrementalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#extensibleRelation}.
 * @param ctx the parse tree
 */
fn enter_extensibleRelation(&mut self, _ctx: &ExtensibleRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#extensibleRelation}.
 * @param ctx the parse tree
 */
fn exit_extensibleRelation(&mut self, _ctx: &ExtensibleRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extensibleRelationTargetIncremental}
 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_extensibleRelationTargetIncremental(&mut self, _ctx: &ExtensibleRelationTargetIncrementalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extensibleRelationTargetIncremental}
 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_extensibleRelationTargetIncremental(&mut self, _ctx: &ExtensibleRelationTargetIncrementalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extensibleRelationTargetDefault}
 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_extensibleRelationTargetDefault(&mut self, _ctx: &ExtensibleRelationTargetDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extensibleRelationTargetDefault}
 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_extensibleRelationTargetDefault(&mut self, _ctx: &ExtensibleRelationTargetDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationExtensionJoin}
 * labeled alternative in {@link DatabricksParser#relationExtension}.
 * @param ctx the parse tree
 */
fn enter_relationExtensionJoin(&mut self, _ctx: &RelationExtensionJoinContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationExtensionJoin}
 * labeled alternative in {@link DatabricksParser#relationExtension}.
 * @param ctx the parse tree
 */
fn exit_relationExtensionJoin(&mut self, _ctx: &RelationExtensionJoinContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationExtensionPivot}
 * labeled alternative in {@link DatabricksParser#relationExtension}.
 * @param ctx the parse tree
 */
fn enter_relationExtensionPivot(&mut self, _ctx: &RelationExtensionPivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationExtensionPivot}
 * labeled alternative in {@link DatabricksParser#relationExtension}.
 * @param ctx the parse tree
 */
fn exit_relationExtensionPivot(&mut self, _ctx: &RelationExtensionPivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelationDefault}
 * labeled alternative in {@link DatabricksParser#joinRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelationDefault(&mut self, _ctx: &JoinRelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelationDefault}
 * labeled alternative in {@link DatabricksParser#joinRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelationDefault(&mut self, _ctx: &JoinRelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelationNatural}
 * labeled alternative in {@link DatabricksParser#joinRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelationNatural(&mut self, _ctx: &JoinRelationNaturalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelationNatural}
 * labeled alternative in {@link DatabricksParser#joinRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelationNatural(&mut self, _ctx: &JoinRelationNaturalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn enter_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn exit_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn enter_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn exit_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntoNamedExpression}
 * labeled alternative in {@link DatabricksParser#pivotInto}.
 * @param ctx the parse tree
 */
fn enter_pivotIntoNamedExpression(&mut self, _ctx: &PivotIntoNamedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntoNamedExpression}
 * labeled alternative in {@link DatabricksParser#pivotInto}.
 * @param ctx the parse tree
 */
fn exit_pivotIntoNamedExpression(&mut self, _ctx: &PivotIntoNamedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn enter_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn exit_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#unpivotAlias}.
 * @param ctx the parse tree
 */
fn enter_unpivotAlias(&mut self, _ctx: &UnpivotAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#unpivotAlias}.
 * @param ctx the parse tree
 */
fn exit_unpivotAlias(&mut self, _ctx: &UnpivotAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#multiColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_multiColumnUnpivot(&mut self, _ctx: &MultiColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#multiColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_multiColumnUnpivot(&mut self, _ctx: &MultiColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#valueColumnSet}.
 * @param ctx the parse tree
 */
fn enter_valueColumnSet(&mut self, _ctx: &ValueColumnSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#valueColumnSet}.
 * @param ctx the parse tree
 */
fn exit_valueColumnSet(&mut self, _ctx: &ValueColumnSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#unpivotColumnSet}.
 * @param ctx the parse tree
 */
fn enter_unpivotColumnSet(&mut self, _ctx: &UnpivotColumnSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#unpivotColumnSet}.
 * @param ctx the parse tree
 */
fn exit_unpivotColumnSet(&mut self, _ctx: &UnpivotColumnSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnSetsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnSetsToUnpivot(&mut self, _ctx: &ColumnSetsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnSetsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnSetsToUnpivot(&mut self, _ctx: &ColumnSetsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiColumnUnpivotDefault}
 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_multiColumnUnpivotDefault(&mut self, _ctx: &MultiColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiColumnUnpivotDefault}
 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_multiColumnUnpivotDefault(&mut self, _ctx: &MultiColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link DatabricksParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link DatabricksParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivot}
 * labeled alternative in {@link DatabricksParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivot}
 * labeled alternative in {@link DatabricksParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link DatabricksParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link DatabricksParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code streamTableTarget}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_streamTableTarget(&mut self, _ctx: &StreamTableTargetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code streamTableTarget}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_streamTableTarget(&mut self, _ctx: &StreamTableTargetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code sampledRelationDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_sampledRelationDefault(&mut self, _ctx: &SampledRelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code sampledRelationDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_sampledRelationDefault(&mut self, _ctx: &SampledRelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault(&mut self, _ctx: &InlineTableDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault(&mut self, _ctx: &InlineTableDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionTableDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_functionTableDefault(&mut self, _ctx: &FunctionTableDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionTableDefault}
 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_functionTableDefault(&mut self, _ctx: &FunctionTableDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#temporalClause}.
 * @param ctx the parse tree
 */
fn enter_temporalClause(&mut self, _ctx: &TemporalClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#temporalClause}.
 * @param ctx the parse tree
 */
fn exit_temporalClause(&mut self, _ctx: &TemporalClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#version}.
 * @param ctx the parse tree
 */
fn enter_version(&mut self, _ctx: &VersionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#version}.
 * @param ctx the parse tree
 */
fn exit_version(&mut self, _ctx: &VersionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link DatabricksParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link DatabricksParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link DatabricksParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link DatabricksParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn enter_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn exit_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code regexp}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_regexp(&mut self, _ctx: &RegexpContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code regexp}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_regexp(&mut self, _ctx: &RegexpContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedLike}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedLike(&mut self, _ctx: &QuantifiedLikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedLike}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedLike(&mut self, _ctx: &QuantifiedLikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truePredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_truePredicate(&mut self, _ctx: &TruePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code falsePredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_falsePredicate(&mut self, _ctx: &FalsePredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link DatabricksParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link DatabricksParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code structConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_structConstructor(&mut self, _ctx: &StructConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code structConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_structConstructor(&mut self, _ctx: &StructConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code anyValue}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_anyValue(&mut self, _ctx: &AnyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code anyValue}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_anyValue(&mut self, _ctx: &AnyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decode}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_decode(&mut self, _ctx: &DecodeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decode}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_decode(&mut self, _ctx: &DecodeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code substring}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code substring}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_substring(&mut self, _ctx: &SubstringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countStar}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countStar}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedStruct}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_namedStruct(&mut self, _ctx: &NamedStructContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedStruct}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_namedStruct(&mut self, _ctx: &NamedStructContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code array}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code array}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tryCastOperator}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_tryCastOperator(&mut self, _ctx: &TryCastOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tryCastOperator}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_tryCastOperator(&mut self, _ctx: &TryCastOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arraysZip}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arraysZip(&mut self, _ctx: &ArraysZipContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arraysZip}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arraysZip(&mut self, _ctx: &ArraysZipContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentLike}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_currentLike(&mut self, _ctx: &CurrentLikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentLike}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_currentLike(&mut self, _ctx: &CurrentLikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code last}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_last(&mut self, _ctx: &LastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code last}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_last(&mut self, _ctx: &LastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code overlay}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_overlay(&mut self, _ctx: &OverlayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code overlay}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_overlay(&mut self, _ctx: &OverlayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subscript}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subscript}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subscript(&mut self, _ctx: &SubscriptContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code collate}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_collate(&mut self, _ctx: &CollateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code collate}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_collate(&mut self, _ctx: &CollateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code jsonExtract}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_jsonExtract(&mut self, _ctx: &JsonExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code jsonExtract}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_jsonExtract(&mut self, _ctx: &JsonExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code constantDefault}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_constantDefault(&mut self, _ctx: &ConstantDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code constantDefault}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_constantDefault(&mut self, _ctx: &ConstantDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extract}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extract}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayConstructor}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arrayConstructor(&mut self, _ctx: &ArrayConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code variable}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code variable}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fromJson}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_fromJson(&mut self, _ctx: &FromJsonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fromJson}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_fromJson(&mut self, _ctx: &FromJsonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code position}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code position}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code listagg}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code listagg}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mapFromEntries}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_mapFromEntries(&mut self, _ctx: &MapFromEntriesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mapFromEntries}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_mapFromEntries(&mut self, _ctx: &MapFromEntriesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code modeFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_modeFunction(&mut self, _ctx: &ModeFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code modeFunction}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_modeFunction(&mut self, _ctx: &ModeFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code first}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_first(&mut self, _ctx: &FirstContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code first}
 * labeled alternative in {@link DatabricksParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_first(&mut self, _ctx: &FirstContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn enter_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn exit_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn enter_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn exit_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link DatabricksParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn enter_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn exit_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code posParameterLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_posParameterLiteral(&mut self, _ctx: &PosParameterLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code posParameterLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_posParameterLiteral(&mut self, _ctx: &PosParameterLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedParameterLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_namedParameterLiteral(&mut self, _ctx: &NamedParameterLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedParameterLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_namedParameterLiteral(&mut self, _ctx: &NamedParameterLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringConcatination}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn enter_stringConcatination(&mut self, _ctx: &StringConcatinationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringConcatination}
 * labeled alternative in {@link DatabricksParser#constant}.
 * @param ctx the parse tree
 */
fn exit_stringConcatination(&mut self, _ctx: &StringConcatinationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#jsonPath}.
 * @param ctx the parse tree
 */
fn enter_jsonPath(&mut self, _ctx: &JsonPathContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#jsonPath}.
 * @param ctx the parse tree
 */
fn exit_jsonPath(&mut self, _ctx: &JsonPathContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#jsonPathElement1}.
 * @param ctx the parse tree
 */
fn enter_jsonPathElement1(&mut self, _ctx: &JsonPathElement1Context<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#jsonPathElement1}.
 * @param ctx the parse tree
 */
fn exit_jsonPathElement1(&mut self, _ctx: &JsonPathElement1Context<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#jsonPathElement2}.
 * @param ctx the parse tree
 */
fn enter_jsonPathElement2(&mut self, _ctx: &JsonPathElement2Context<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#jsonPathElement2}.
 * @param ctx the parse tree
 */
fn exit_jsonPathElement2(&mut self, _ctx: &JsonPathElement2Context<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#field}.
 * @param ctx the parse tree
 */
fn enter_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#field}.
 * @param ctx the parse tree
 */
fn exit_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link DatabricksParser#string}.
 * @param ctx the parse tree
 */
fn enter_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link DatabricksParser#string}.
 * @param ctx the parse tree
 */
fn exit_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleQuotedStringLiteral}
 * labeled alternative in {@link DatabricksParser#string}.
 * @param ctx the parse tree
 */
fn enter_doubleQuotedStringLiteral(&mut self, _ctx: &DoubleQuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleQuotedStringLiteral}
 * labeled alternative in {@link DatabricksParser#string}.
 * @param ctx the parse tree
 */
fn exit_doubleQuotedStringLiteral(&mut self, _ctx: &DoubleQuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#standaloneInterval}.
 * @param ctx the parse tree
 */
fn enter_standaloneInterval(&mut self, _ctx: &StandaloneIntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#standaloneInterval}.
 * @param ctx the parse tree
 */
fn exit_standaloneInterval(&mut self, _ctx: &StandaloneIntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#intervalValue}.
 * @param ctx the parse tree
 */
fn enter_intervalValue(&mut self, _ctx: &IntervalValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#intervalValue}.
 * @param ctx the parse tree
 */
fn exit_intervalValue(&mut self, _ctx: &IntervalValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#intervalValueField}.
 * @param ctx the parse tree
 */
fn enter_intervalValueField(&mut self, _ctx: &IntervalValueFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#intervalValueField}.
 * @param ctx the parse tree
 */
fn exit_intervalValueField(&mut self, _ctx: &IntervalValueFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#intervalTypeField}.
 * @param ctx the parse tree
 */
fn enter_intervalTypeField(&mut self, _ctx: &IntervalTypeFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#intervalTypeField}.
 * @param ctx the parse tree
 */
fn exit_intervalTypeField(&mut self, _ctx: &IntervalTypeFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn enter_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn exit_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#collateClause}.
 * @param ctx the parse tree
 */
fn enter_collateClause(&mut self, _ctx: &CollateClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#collateClause}.
 * @param ctx the parse tree
 */
fn exit_collateClause(&mut self, _ctx: &CollateClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link DatabricksParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link DatabricksParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link DatabricksParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link DatabricksParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_rowType(&mut self, _ctx: &RowTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_intervalType(&mut self, _ctx: &IntervalTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyArrayType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyArrayType(&mut self, _ctx: &LegacyArrayTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link DatabricksParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#commentSpec}.
 * @param ctx the parse tree
 */
fn enter_commentSpec(&mut self, _ctx: &CommentSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#commentSpec}.
 * @param ctx the parse tree
 */
fn exit_commentSpec(&mut self, _ctx: &CommentSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link DatabricksParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link DatabricksParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link DatabricksParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pathExpression}.
 * @param ctx the parse tree
 */
fn enter_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pathExpression}.
 * @param ctx the parse tree
 */
fn exit_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link DatabricksParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code strictIdentifierDefault}
 * labeled alternative in {@link DatabricksParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_strictIdentifierDefault(&mut self, _ctx: &StrictIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code strictIdentifierDefault}
 * labeled alternative in {@link DatabricksParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_strictIdentifierDefault(&mut self, _ctx: &StrictIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code strictNonReservedIdentifier}
 * labeled alternative in {@link DatabricksParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_strictNonReservedIdentifier(&mut self, _ctx: &StrictNonReservedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code strictNonReservedIdentifier}
 * labeled alternative in {@link DatabricksParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_strictNonReservedIdentifier(&mut self, _ctx: &StrictNonReservedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn enter_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code backQuotedIdentifier}
 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn exit_backQuotedIdentifier(&mut self, _ctx: &BackQuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#pathComponent}.
 * @param ctx the parse tree
 */
fn enter_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#pathComponent}.
 * @param ctx the parse tree
 */
fn exit_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn enter_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn exit_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#identifierList}.
 * @param ctx the parse tree
 */
fn enter_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#identifierList}.
 * @param ctx the parse tree
 */
fn exit_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn enter_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn exit_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exponentLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_exponentLiteral(&mut self, _ctx: &ExponentLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exponentLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_exponentLiteral(&mut self, _ctx: &ExponentLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_bigIntLiteral(&mut self, _ctx: &BigIntLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_bigIntLiteral(&mut self, _ctx: &BigIntLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code smallIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_smallIntLiteral(&mut self, _ctx: &SmallIntLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code smallIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_smallIntLiteral(&mut self, _ctx: &SmallIntLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tinyIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_tinyIntLiteral(&mut self, _ctx: &TinyIntLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tinyIntLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_tinyIntLiteral(&mut self, _ctx: &TinyIntLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code floatLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_floatLiteral(&mut self, _ctx: &FloatLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code floatLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_floatLiteral(&mut self, _ctx: &FloatLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bigDecimalLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn enter_bigDecimalLiteral(&mut self, _ctx: &BigDecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bigDecimalLiteral}
 * labeled alternative in {@link DatabricksParser#number}.
 * @param ctx the parse tree
 */
fn exit_bigDecimalLiteral(&mut self, _ctx: &BigDecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentStruct}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentStruct(&mut self, _ctx: &PrestoFunctionArgumentStructContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentStruct}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentStruct(&mut self, _ctx: &PrestoFunctionArgumentStructContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentMap}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentMap(&mut self, _ctx: &PrestoFunctionArgumentMapContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentMap}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentMap(&mut self, _ctx: &PrestoFunctionArgumentMapContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentArray}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentArray(&mut self, _ctx: &PrestoFunctionArgumentArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentArray}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentArray(&mut self, _ctx: &PrestoFunctionArgumentArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentLambda}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentLambda(&mut self, _ctx: &PrestoFunctionArgumentLambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentLambda}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentLambda(&mut self, _ctx: &PrestoFunctionArgumentLambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentInteger}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentInteger(&mut self, _ctx: &PrestoFunctionArgumentIntegerContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentInteger}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentInteger(&mut self, _ctx: &PrestoFunctionArgumentIntegerContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prestoFunctionArgumentDefault}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_prestoFunctionArgumentDefault(&mut self, _ctx: &PrestoFunctionArgumentDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prestoFunctionArgumentDefault}
 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_prestoFunctionArgumentDefault(&mut self, _ctx: &PrestoFunctionArgumentDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#prestoShowFunctionRowField}.
 * @param ctx the parse tree
 */
fn enter_prestoShowFunctionRowField(&mut self, _ctx: &PrestoShowFunctionRowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#prestoShowFunctionRowField}.
 * @param ctx the parse tree
 */
fn exit_prestoShowFunctionRowField(&mut self, _ctx: &PrestoShowFunctionRowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#prestoShowFunctionTypes}.
 * @param ctx the parse tree
 */
fn enter_prestoShowFunctionTypes(&mut self, _ctx: &PrestoShowFunctionTypesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#prestoShowFunctionTypes}.
 * @param ctx the parse tree
 */
fn exit_prestoShowFunctionTypes(&mut self, _ctx: &PrestoShowFunctionTypesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn enter_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn exit_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Enter a parse tree produced by {@link DatabricksParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link DatabricksParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : DatabricksListener<'input> }


