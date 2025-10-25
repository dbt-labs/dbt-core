#![allow(nonstandard_style)]
// Generated from Bigquery.g4 by ANTLR 4.8
use antlr_rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::bigqueryparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link BigqueryParser}.
 */
pub trait BigqueryVisitor<'input>: ParseTreeVisitor<'input,BigqueryParserContextType>{
	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#statementBlock}.
	 * @param ctx the parse tree
	 */
	fn visit_statementBlock(&mut self, ctx: &StatementBlockContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_use(&mut self, ctx: &UseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateExternalTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryCreateExternalTable(&mut self, ctx: &BigqueryCreateExternalTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryCreateTable(&mut self, ctx: &BigqueryCreateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSnapshotTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSnapshotTable(&mut self, ctx: &CreateSnapshotTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertInto}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryCreateMaterializedView(&mut self, ctx: &BigqueryCreateMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryCreateView(&mut self, ctx: &BigqueryCreateViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_merge(&mut self, ctx: &MergeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSqlFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createJSFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRemoteFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRemoteFunction(&mut self, ctx: &CreateRemoteFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateModel}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryCreateModel(&mut self, ctx: &BigqueryCreateModelContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code declare}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_declare(&mut self, ctx: &DeclareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code executeImmediate}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_executeImmediate(&mut self, ctx: &ExecuteImmediateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code begin}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_begin(&mut self, ctx: &BeginContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code beginException}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_beginException(&mut self, ctx: &BeginExceptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code case}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_case(&mut self, ctx: &CaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code if}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_if(&mut self, ctx: &IfContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code loop}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_loop(&mut self, ctx: &LoopContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code repeat}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_repeat(&mut self, ctx: &RepeatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code while}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_while(&mut self, ctx: &WhileContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code break}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_break(&mut self, ctx: &BreakContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code leave}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_leave(&mut self, ctx: &LeaveContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code continue}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_continue(&mut self, ctx: &ContinueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code forIn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_forIn(&mut self, ctx: &ForInContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code raise}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_raise(&mut self, ctx: &RaiseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code return}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_return(&mut self, ctx: &ReturnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code beginTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_beginTransaction(&mut self, ctx: &BeginTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code commitTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_commitTransaction(&mut self, ctx: &CommitTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollbackTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollbackTransaction(&mut self, ctx: &RollbackTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_set(&mut self, ctx: &SetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_drop(&mut self, ctx: &DropContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_delete(&mut self, ctx: &DeleteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_comment(&mut self, ctx: &CommentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_call(&mut self, ctx: &CallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_grant(&mut self, ctx: &GrantContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deny(&mut self, ctx: &DenyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_show(&mut self, ctx: &ShowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_reset(&mut self, ctx: &ResetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_commit(&mut self, ctx: &CommitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_update(&mut self, ctx: &UpdateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableElements}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#connectionSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_connectionSpec(&mut self, ctx: &ConnectionSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#query}.
	 * @param ctx the parse tree
	 */
	fn visit_query(&mut self, ctx: &QueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#with}.
	 * @param ctx the parse tree
	 */
	fn visit_with(&mut self, ctx: &WithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableElement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnName}.
	 * @param ctx the parse tree
	 */
	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnOption}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaStruct}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaStruct(&mut self, ctx: &ColumnSchemaStructContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaArray}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaArray(&mut self, ctx: &ColumnSchemaArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#fieldList}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#arrayElementSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_arrayElementSchema(&mut self, ctx: &ArrayElementSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#structDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_structDefinition(&mut self, ctx: &StructDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#properties}.
	 * @param ctx the parse tree
	 */
	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link BigqueryParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link BigqueryParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryLimit}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDefault}
	 * labeled alternative in {@link BigqueryParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperation}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_table(&mut self, ctx: &TableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sortItem}.
	 * @param ctx the parse tree
	 */
	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#replaceDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link BigqueryParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link BigqueryParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollup(&mut self, ctx: &RollupContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_cube(&mut self, ctx: &CubeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#orderBy}.
	 * @param ctx the parse tree
	 */
	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link BigqueryParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link BigqueryParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multiSelect}.
	 * @param ctx the parse tree
	 */
	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#selectStar}.
	 * @param ctx the parse tree
	 */
	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#relation}.
	 * @param ctx the parse tree
	 */
	fn visit_relation(&mut self, ctx: &RelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link BigqueryParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link BigqueryParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#joinType}.
	 * @param ctx the parse tree
	 */
	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntoNamedExpression}
	 * labeled alternative in {@link BigqueryParser#pivotInto}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multiColumnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#valueColumnSet}.
	 * @param ctx the parse tree
	 */
	fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnSetsToUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multiColumnUnpivotDefault}
	 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link BigqueryParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link BigqueryParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_pivot(&mut self, ctx: &PivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link BigqueryParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#partitionColumn}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#partitionColumns}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionColumns(&mut self, ctx: &PartitionColumnsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code aliased}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_aliased(&mut self, ctx: &AliasedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_unnest(&mut self, ctx: &UnnestContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link BigqueryParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#descriptorField}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_or(&mut self, ctx: &OrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_and(&mut self, ctx: &AndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inList(&mut self, ctx: &InListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inUnnest}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inUnnest(&mut self, ctx: &InUnnestContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedLike}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_like(&mut self, ctx: &LikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truePredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code falsePredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dateDiff}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dateDiff(&mut self, ctx: &DateDiffContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code structConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryArrayConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryArrayConstructor(&mut self, ctx: &BigqueryArrayConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_substring(&mut self, ctx: &SubstringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_cast(&mut self, ctx: &CastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_trim(&mut self, ctx: &TrimContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_array(&mut self, ctx: &ArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code coalesce}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_coalesce(&mut self, ctx: &CoalesceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arraySubquery}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arraySubquery(&mut self, ctx: &ArraySubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryExtract}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryExtract(&mut self, ctx: &BigqueryExtractContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_measure(&mut self, ctx: &MeasureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_variable(&mut self, ctx: &VariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_exists(&mut self, ctx: &ExistsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link BigqueryParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link BigqueryParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dateDiffCall}.
	 * @param ctx the parse tree
	 */
	fn visit_dateDiffCall(&mut self, ctx: &DateDiffCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#datePart}.
	 * @param ctx the parse tree
	 */
	fn visit_datePart(&mut self, ctx: &DatePartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionName}.
	 * @param ctx the parse tree
	 */
	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#havingArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_havingArgument(&mut self, ctx: &HavingArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#limitArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_limitArgument(&mut self, ctx: &LimitArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#field}.
	 * @param ctx the parse tree
	 */
	fn visit_field(&mut self, ctx: &FieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#processingMode}.
	 * @param ctx the parse tree
	 */
	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedStringLiteral(&mut self, ctx: &QuotedStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tripleQuotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_tripleQuotedStringLiteral(&mut self, ctx: &TripleQuotedStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rawStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_rawStringLiteral(&mut self, ctx: &RawStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rawTripleQuotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_rawTripleQuotedStringLiteral(&mut self, ctx: &RawTripleQuotedStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#interval}.
	 * @param ctx the parse tree
	 */
	fn visit_interval(&mut self, ctx: &IntervalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#intervalField}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#normalForm}.
	 * @param ctx the parse tree
	 */
	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link BigqueryParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link BigqueryParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigqueryType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_bigqueryType(&mut self, ctx: &BigqueryTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyStructType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rowField}.
	 * @param ctx the parse tree
	 */
	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#whenClause}.
	 * @param ctx the parse tree
	 */
	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#filter}.
	 * @param ctx the parse tree
	 */
	fn visit_filter(&mut self, ctx: &FilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mergeCaseMatched}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeCaseMatched(&mut self, ctx: &MergeCaseMatchedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mergeCaseNotMatched}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeCaseNotMatched(&mut self, ctx: &MergeCaseNotMatchedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mergeCaseNotMatchedBySource}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeCaseNotMatchedBySource(&mut self, ctx: &MergeCaseNotMatchedBySourceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#mergeUpdateClause}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeUpdateClause(&mut self, ctx: &MergeUpdateClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#mergeInsertClause}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeInsertClause(&mut self, ctx: &MergeInsertClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#over}.
	 * @param ctx the parse tree
	 */
	fn visit_over(&mut self, ctx: &OverContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#frameExtent}.
	 * @param ctx the parse tree
	 */
	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link BigqueryParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link BigqueryParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#privilege}.
	 * @param ctx the parse tree
	 */
	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link BigqueryParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nonquotedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dashedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#maybeDashedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dashedPathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dashedPathExpression(&mut self, ctx: &DashedPathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#maybeDashedPathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_maybeDashedPathExpression(&mut self, ctx: &MaybeDashedPathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rangeType}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link BigqueryParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link BigqueryParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pathComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#identifierList}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code hexadecimalLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_hexadecimalLiteral(&mut self, ctx: &HexadecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) { self.visit_children(ctx) }

}

pub trait BigqueryVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= BigqueryParserContextType>{
	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#singleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneType}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#statementBlock}.
	 * @param ctx the parse tree
	 */
		fn visit_statementBlock(&mut self, ctx: &StatementBlockContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_use(&mut self, ctx: &UseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateExternalTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryCreateExternalTable(&mut self, ctx: &BigqueryCreateExternalTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryCreateTable(&mut self, ctx: &BigqueryCreateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSnapshotTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSnapshotTable(&mut self, ctx: &CreateSnapshotTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertInto}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryCreateMaterializedView(&mut self, ctx: &BigqueryCreateMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryCreateView(&mut self, ctx: &BigqueryCreateViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_merge(&mut self, ctx: &MergeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSqlFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createJSFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRemoteFunction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRemoteFunction(&mut self, ctx: &CreateRemoteFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryCreateModel}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryCreateModel(&mut self, ctx: &BigqueryCreateModelContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code declare}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_declare(&mut self, ctx: &DeclareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code executeImmediate}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_executeImmediate(&mut self, ctx: &ExecuteImmediateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code begin}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_begin(&mut self, ctx: &BeginContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code beginException}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_beginException(&mut self, ctx: &BeginExceptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code case}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_case(&mut self, ctx: &CaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code if}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_if(&mut self, ctx: &IfContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code loop}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_loop(&mut self, ctx: &LoopContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code repeat}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_repeat(&mut self, ctx: &RepeatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code while}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_while(&mut self, ctx: &WhileContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code break}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_break(&mut self, ctx: &BreakContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code leave}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_leave(&mut self, ctx: &LeaveContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code continue}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_continue(&mut self, ctx: &ContinueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code forIn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_forIn(&mut self, ctx: &ForInContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code raise}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_raise(&mut self, ctx: &RaiseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code return}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_return(&mut self, ctx: &ReturnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code beginTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_beginTransaction(&mut self, ctx: &BeginTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code commitTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_commitTransaction(&mut self, ctx: &CommitTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollbackTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollbackTransaction(&mut self, ctx: &RollbackTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_set(&mut self, ctx: &SetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_drop(&mut self, ctx: &DropContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_delete(&mut self, ctx: &DeleteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_comment(&mut self, ctx: &CommentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_call(&mut self, ctx: &CallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_grant(&mut self, ctx: &GrantContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deny(&mut self, ctx: &DenyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_show(&mut self, ctx: &ShowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_reset(&mut self, ctx: &ResetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_commit(&mut self, ctx: &CommitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link BigqueryParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_update(&mut self, ctx: &UpdateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableElements}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#locationSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#connectionSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_connectionSpec(&mut self, ctx: &ConnectionSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#query}.
	 * @param ctx the parse tree
	 */
		fn visit_query(&mut self, ctx: &QueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#with}.
	 * @param ctx the parse tree
	 */
		fn visit_with(&mut self, ctx: &WithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableElement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnName}.
	 * @param ctx the parse tree
	 */
		fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnOption}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaStruct}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaStruct(&mut self, ctx: &ColumnSchemaStructContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaArray}
	 * labeled alternative in {@link BigqueryParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaArray(&mut self, ctx: &ColumnSchemaArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#fieldList}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#arrayElementSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_arrayElementSchema(&mut self, ctx: &ArrayElementSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#structDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_structDefinition(&mut self, ctx: &StructDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#properties}.
	 * @param ctx the parse tree
	 */
		fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link BigqueryParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link BigqueryParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#propertyKey}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link BigqueryParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryLimit}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDefault}
	 * labeled alternative in {@link BigqueryParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperation}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#inlineTable}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_table(&mut self, ctx: &TableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link BigqueryParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sortItem}.
	 * @param ctx the parse tree
	 */
		fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#querySpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#replaceDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
		fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link BigqueryParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link BigqueryParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollup(&mut self, ctx: &RollupContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_cube(&mut self, ctx: &CubeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link BigqueryParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#groupingSet}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#orderBy}.
	 * @param ctx the parse tree
	 */
		fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link BigqueryParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link BigqueryParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multiSelect}.
	 * @param ctx the parse tree
	 */
		fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#selectStar}.
	 * @param ctx the parse tree
	 */
		fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#relation}.
	 * @param ctx the parse tree
	 */
		fn visit_relation(&mut self, ctx: &RelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link BigqueryParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link BigqueryParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#joinType}.
	 * @param ctx the parse tree
	 */
		fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
		fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntoNamedExpression}
	 * labeled alternative in {@link BigqueryParser#pivotInto}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#multiColumnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#valueColumnSet}.
	 * @param ctx the parse tree
	 */
		fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnSetsToUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multiColumnUnpivotDefault}
	 * labeled alternative in {@link BigqueryParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link BigqueryParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link BigqueryParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_pivot(&mut self, ctx: &PivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link BigqueryParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link BigqueryParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#columnAliases}.
	 * @param ctx the parse tree
	 */
		fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#partitionColumn}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#partitionColumns}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionColumns(&mut self, ctx: &PartitionColumnsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code aliased}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_aliased(&mut self, ctx: &AliasedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_unnest(&mut self, ctx: &UnnestContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link BigqueryParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link BigqueryParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#tableArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link BigqueryParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#descriptorField}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
		fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_or(&mut self, ctx: &OrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link BigqueryParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_and(&mut self, ctx: &AndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_between(&mut self, ctx: &BetweenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inList(&mut self, ctx: &InListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inUnnest}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inUnnest(&mut self, ctx: &InUnnestContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedLike}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_like(&mut self, ctx: &LikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truePredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code falsePredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link BigqueryParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link BigqueryParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dateDiff}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dateDiff(&mut self, ctx: &DateDiffContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code structConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryArrayConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryArrayConstructor(&mut self, ctx: &BigqueryArrayConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_substring(&mut self, ctx: &SubstringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_cast(&mut self, ctx: &CastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_trim(&mut self, ctx: &TrimContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_array(&mut self, ctx: &ArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code coalesce}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_coalesce(&mut self, ctx: &CoalesceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arraySubquery}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arraySubquery(&mut self, ctx: &ArraySubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryExtract}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryExtract(&mut self, ctx: &BigqueryExtractContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_measure(&mut self, ctx: &MeasureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_variable(&mut self, ctx: &VariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_exists(&mut self, ctx: &ExistsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link BigqueryParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link BigqueryParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link BigqueryParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
		fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dateDiffCall}.
	 * @param ctx the parse tree
	 */
		fn visit_dateDiffCall(&mut self, ctx: &DateDiffCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#datePart}.
	 * @param ctx the parse tree
	 */
		fn visit_datePart(&mut self, ctx: &DatePartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#functionName}.
	 * @param ctx the parse tree
	 */
		fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#havingArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_havingArgument(&mut self, ctx: &HavingArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#limitArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_limitArgument(&mut self, ctx: &LimitArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#namedParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#field}.
	 * @param ctx the parse tree
	 */
		fn visit_field(&mut self, ctx: &FieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#processingMode}.
	 * @param ctx the parse tree
	 */
		fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
		fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedStringLiteral(&mut self, ctx: &QuotedStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tripleQuotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_tripleQuotedStringLiteral(&mut self, ctx: &TripleQuotedStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rawStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_rawStringLiteral(&mut self, ctx: &RawStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rawTripleQuotedStringLiteral}
	 * labeled alternative in {@link BigqueryParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_rawTripleQuotedStringLiteral(&mut self, ctx: &RawTripleQuotedStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#booleanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#interval}.
	 * @param ctx the parse tree
	 */
		fn visit_interval(&mut self, ctx: &IntervalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#intervalField}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#normalForm}.
	 * @param ctx the parse tree
	 */
		fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link BigqueryParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link BigqueryParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigqueryType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_bigqueryType(&mut self, ctx: &BigqueryTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyStructType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link BigqueryParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rowField}.
	 * @param ctx the parse tree
	 */
		fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#typeParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#whenClause}.
	 * @param ctx the parse tree
	 */
		fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#filter}.
	 * @param ctx the parse tree
	 */
		fn visit_filter(&mut self, ctx: &FilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mergeCaseMatched}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeCaseMatched(&mut self, ctx: &MergeCaseMatchedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mergeCaseNotMatched}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeCaseNotMatched(&mut self, ctx: &MergeCaseNotMatchedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mergeCaseNotMatchedBySource}
	 * labeled alternative in {@link BigqueryParser#mergeCase}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeCaseNotMatchedBySource(&mut self, ctx: &MergeCaseNotMatchedBySourceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#mergeUpdateClause}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeUpdateClause(&mut self, ctx: &MergeUpdateClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#mergeInsertClause}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeInsertClause(&mut self, ctx: &MergeInsertClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#over}.
	 * @param ctx the parse tree
	 */
		fn visit_over(&mut self, ctx: &OverContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#windowFrame}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#frameExtent}.
	 * @param ctx the parse tree
	 */
		fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link BigqueryParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link BigqueryParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link BigqueryParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link BigqueryParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link BigqueryParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link BigqueryParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link BigqueryParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#privilege}.
	 * @param ctx the parse tree
	 */
		fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link BigqueryParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nonquotedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dashedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#maybeDashedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#dashedPathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dashedPathExpression(&mut self, ctx: &DashedPathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#maybeDashedPathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_maybeDashedPathExpression(&mut self, ctx: &MaybeDashedPathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#rangeType}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link BigqueryParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link BigqueryParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link BigqueryParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#pathComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#identifierList}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code hexadecimalLiteral}
	 * labeled alternative in {@link BigqueryParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_hexadecimalLiteral(&mut self, ctx: &HexadecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link BigqueryParser#nonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> BigqueryVisitor<'input> for T
where
	T: BigqueryVisitorCompat<'input>
{
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_multipleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_singleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_standaloneExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_standaloneQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_standaloneType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statementBlock(&mut self, ctx: &StatementBlockContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_statementBlock(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_statementDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_use(&mut self, ctx: &UseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_use(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dropSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_renameSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setSchemaAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dropTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dropView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryCreateExternalTable(&mut self, ctx: &BigqueryCreateExternalTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryCreateExternalTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryCreateTable(&mut self, ctx: &BigqueryCreateTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryCreateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSnapshotTable(&mut self, ctx: &CreateSnapshotTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createSnapshotTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_insertInto(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryCreateMaterializedView(&mut self, ctx: &BigqueryCreateMaterializedViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryCreateMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryCreateView(&mut self, ctx: &BigqueryCreateViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryCreateView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_showColumns(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_merge(&mut self, ctx: &MergeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_merge(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createSqlFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createJSFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRemoteFunction(&mut self, ctx: &CreateRemoteFunctionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createRemoteFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryCreateModel(&mut self, ctx: &BigqueryCreateModelContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryCreateModel(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_declare(&mut self, ctx: &DeclareContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_declare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_executeImmediate(&mut self, ctx: &ExecuteImmediateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_executeImmediate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_begin(&mut self, ctx: &BeginContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_begin(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_beginException(&mut self, ctx: &BeginExceptionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_beginException(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_case(&mut self, ctx: &CaseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_case(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_if(&mut self, ctx: &IfContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_if(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_loop(&mut self, ctx: &LoopContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_loop(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_repeat(&mut self, ctx: &RepeatContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_repeat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_while(&mut self, ctx: &WhileContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_while(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_break(&mut self, ctx: &BreakContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_break(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_leave(&mut self, ctx: &LeaveContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_leave(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_continue(&mut self, ctx: &ContinueContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_continue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_forIn(&mut self, ctx: &ForInContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_forIn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_raise(&mut self, ctx: &RaiseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_raise(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_return(&mut self, ctx: &ReturnContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_return(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_beginTransaction(&mut self, ctx: &BeginTransactionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_beginTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commitTransaction(&mut self, ctx: &CommitTransactionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_commitTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollbackTransaction(&mut self, ctx: &RollbackTransactionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rollbackTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_set(&mut self, ctx: &SetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_set(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_drop(&mut self, ctx: &DropContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_drop(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delete(&mut self, ctx: &DeleteContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_delete(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_truncateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comment(&mut self, ctx: &CommentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_comment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_renameTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_addColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_renameColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dropColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setColumnType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setTableAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setTableProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableExecute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_analyze(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_refreshMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_renameMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setMaterializedViewProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_renameView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setViewAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_call(&mut self, ctx: &CallContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_call(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_createRole(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grant(&mut self, ctx: &GrantContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_grant(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_revoke(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deny(&mut self, ctx: &DenyContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_deny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_show(&mut self, ctx: &ShowContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_show(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_reset(&mut self, ctx: &ResetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_reset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_startTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commit(&mut self, ctx: &CommitContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_commit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rollback(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_prepare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_deallocate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_execute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_describeInput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_describeOutput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_update(&mut self, ctx: &UpdateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_update(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableElements(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_namedExpressionSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_namedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unpivotNullClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_locationSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_connectionSpec(&mut self, ctx: &ConnectionSpecContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_connectionSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_query(&mut self, ctx: &QueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_query(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_with(&mut self, ctx: &WithContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_with(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_fieldDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnNameComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnSchemaWithMetadata(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnOptionList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnSchemaSimpleType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaStruct(&mut self, ctx: &ColumnSchemaStructContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnSchemaStruct(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaArray(&mut self, ctx: &ColumnSchemaArrayContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnSchemaArray(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_fieldList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arrayElementSchema(&mut self, ctx: &ArrayElementSchemaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_arrayElementSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structDefinition(&mut self, ctx: &StructDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_structDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_properties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_propertyAssignments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nestedProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_defaultProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_propertyKey(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_defaultPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_identifierPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_expressionPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryNoWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryLimit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryLimitTargetDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_limitRowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setOperation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setOperationIntersect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setIntersectOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_setQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_inlineTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryPrimaryDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_table(&mut self, ctx: &TableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_table(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_inlineTableDefault1(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_subquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_sortItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_querySpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_replaceDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_querySelectItems(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_aggregationClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_groupByAll(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_groupByDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollup(&mut self, ctx: &RollupContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rollup(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cube(&mut self, ctx: &CubeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_cube(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_multipleGroupingSets(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_singleGroupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_groupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_windowDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_windowSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_windowSpecificationPartitionBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_orderBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_namedQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_selectItemAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_selectSingle(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_selectMulti(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_multiSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_selectStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relation(&mut self, ctx: &RelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_relation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_relationDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_joinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_noJoinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_joinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_joinCriteria(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_sampledRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_sampledRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_sampleOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_sampleMethod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_trimsSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_listAggOverflowBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_listaggCountIndication(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_variableDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotedRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotAggregates(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotIntoNamedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotAsAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_singleColumnUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnsToUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unpivotAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_multiColumnUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_valueColumnSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unpivotColumnSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnSetsToUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_singleColumnUnpivotDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_multiColumnUnpivotDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivotIntosDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivot(&mut self, ctx: &PivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_subqueryRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_aliasedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnAliases(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_partitionColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionColumns(&mut self, ctx: &PartitionColumnsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_partitionColumns(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliased(&mut self, ctx: &AliasedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_aliased(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unnest(&mut self, ctx: &UnnestContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unnest(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableFunctionInvocation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_defaultTableFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableFunctionArgumentCopartition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableFunctionArgumentName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableFunctionArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableArgumentTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tableArgumentQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_descriptorArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_descriptorField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_copartitionTables(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_expression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_logicalNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_predicated(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_or(&mut self, ctx: &OrContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_or(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_and(&mut self, ctx: &AndContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_and(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_comparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_quantifiedComparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_between(&mut self, ctx: &BetweenContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_between(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inList(&mut self, ctx: &InListContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_inList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inUnnest(&mut self, ctx: &InUnnestContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_inUnnest(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_inSubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_quantifiedLike(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_like(&mut self, ctx: &LikeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_like(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nullPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_distinctFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_truePredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_falsePredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unknownPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_valueExpressionDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_concatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_arithmeticBinary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_arithmeticUnary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateDiff(&mut self, ctx: &DateDiffContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dateDiff(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dereference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_structConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_typeConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryArrayConstructor(&mut self, ctx: &BigqueryArrayConstructorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryArrayConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_substring(&mut self, ctx: &SubstringContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_substring(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_countStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cast(&mut self, ctx: &CastContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_cast(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_lambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_parenthesizedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trim(&mut self, ctx: &TrimContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_trim(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_array(&mut self, ctx: &ArrayContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_array(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_normalize(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_intervalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_numericLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_booleanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_simpleCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_columnReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nullLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rowConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_subscript(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_coalesce(&mut self, ctx: &CoalesceContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_coalesce(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arraySubquery(&mut self, ctx: &ArraySubqueryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_arraySubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_subqueryExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_binaryLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryExtract(&mut self, ctx: &BigqueryExtractContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryExtract(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measure(&mut self, ctx: &MeasureContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_measure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variable(&mut self, ctx: &VariableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_variable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exists(&mut self, ctx: &ExistsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_exists(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_searchedCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionCallHead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionCallTail(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_positionalArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_namedArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionExtraArguments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateDiffCall(&mut self, ctx: &DateDiffCallContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dateDiffCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_datePart(&mut self, ctx: &DatePartContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_datePart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_havingArgument(&mut self, ctx: &HavingArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_havingArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_limitArgument(&mut self, ctx: &LimitArgumentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_limitArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_namedParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_field(&mut self, ctx: &FieldContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_field(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_processingMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nullTreatment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedStringLiteral(&mut self, ctx: &QuotedStringLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_quotedStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tripleQuotedStringLiteral(&mut self, ctx: &TripleQuotedStringLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_tripleQuotedStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rawStringLiteral(&mut self, ctx: &RawStringLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rawStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rawTripleQuotedStringLiteral(&mut self, ctx: &RawTripleQuotedStringLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rawTripleQuotedStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_comparisonOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_comparisonQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_booleanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_interval(&mut self, ctx: &IntervalContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_interval(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_intervalField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_normalForm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_typeIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_typeNotNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_typeNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_intervalType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_functionSignatureGenericType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_legacyArrayType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigqueryType(&mut self, ctx: &BigqueryTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_bigqueryType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_legacyStructType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_primitiveType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_typeParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_whenClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_filter(&mut self, ctx: &FilterContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_filter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeCaseMatched(&mut self, ctx: &MergeCaseMatchedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_mergeCaseMatched(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeCaseNotMatched(&mut self, ctx: &MergeCaseNotMatchedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_mergeCaseNotMatched(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeCaseNotMatchedBySource(&mut self, ctx: &MergeCaseNotMatchedBySourceContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_mergeCaseNotMatchedBySource(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeUpdateClause(&mut self, ctx: &MergeUpdateClauseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_mergeUpdateClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeInsertClause(&mut self, ctx: &MergeInsertClauseContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_mergeInsertClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_over(&mut self, ctx: &OverContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_over(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_windowFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_frameExtent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unboundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_currentRowBound(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_boundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_quantifiedPrimary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_patternConcatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_patternAlternation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_patternVariable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_emptyPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_patternPermutation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_groupedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_partitionStartAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_partitionEndAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_excludedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_zeroOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_oneOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_zeroOrOneQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rangeQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_isolationLevel(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_transactionAccessMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_readUncommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_readCommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_repeatableRead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_serializable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_privilege(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_qualifiedNameDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nonquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dashedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_maybeDashedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dashedPathExpression(&mut self, ctx: &DashedPathExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_dashedPathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_maybeDashedPathExpression(&mut self, ctx: &MaybeDashedPathExpressionContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_maybeDashedPathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_queryPeriod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rangeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unspecifiedPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_userPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_rolePrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_unquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_backQuotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_pathComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_standaloneIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_identifierList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_identifierSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_decimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_doubleLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_integerLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_hexadecimalLiteral(&mut self, ctx: &HexadecimalLiteralContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_hexadecimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>){
		let result = <Self as BigqueryVisitorCompat>::visit_nonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}