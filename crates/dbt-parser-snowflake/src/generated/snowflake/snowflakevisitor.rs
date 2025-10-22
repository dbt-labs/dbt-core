#![allow(nonstandard_style)]
// Generated from Snowflake.g4 by ANTLR 4.8
use antlr_rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::snowflakeparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SnowflakeParser}.
 */
pub trait SnowflakeVisitor<'input>: ParseTreeVisitor<'input,SnowflakeParserContextType>{
	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_use(&mut self, ctx: &UseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createExternalTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createExternalTable(&mut self, ctx: &CreateExternalTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTableAsSelect}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeCreateTableAsSelect(&mut self, ctx: &SnowflakeCreateTableAsSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTableClone}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTableClone(&mut self, ctx: &CreateTableCloneContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createDynamicTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createDynamicTable(&mut self, ctx: &CreateDynamicTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createEventTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createEventTable(&mut self, ctx: &CreateEventTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createIcebergTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createIcebergTable(&mut self, ctx: &CreateIcebergTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRecursiveTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeCreateTable(&mut self, ctx: &SnowflakeCreateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTableUsingTemplate}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeCreateTableUsingTemplate(&mut self, ctx: &SnowflakeCreateTableUsingTemplateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeInsertInto}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeInsertInto(&mut self, ctx: &SnowflakeInsertIntoContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_set(&mut self, ctx: &SetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createStage}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createStage(&mut self, ctx: &CreateStageContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createJavaFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createJavaFunction(&mut self, ctx: &CreateJavaFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createJarFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createJarFunction(&mut self, ctx: &CreateJarFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createJSFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createPythonFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createPythonFunction(&mut self, ctx: &CreatePythonFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createModuleFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createModuleFunction(&mut self, ctx: &CreateModuleFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createScalaFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createScalaFunction(&mut self, ctx: &CreateScalaFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createScalaJarFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createScalaJarFunction(&mut self, ctx: &CreateScalaJarFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSqlFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createPythonProcedure}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createPythonProcedure(&mut self, ctx: &CreatePythonProcedureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createAnonymousPythonProcedure}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createAnonymousPythonProcedure(&mut self, ctx: &CreateAnonymousPythonProcedureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unset}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_unset(&mut self, ctx: &UnsetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_merge(&mut self, ctx: &MergeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code alter}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_alter(&mut self, ctx: &AlterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code begin}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_begin(&mut self, ctx: &BeginContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createFoo}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_drop(&mut self, ctx: &DropContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_delete(&mut self, ctx: &DeleteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_comment(&mut self, ctx: &CommentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_call(&mut self, ctx: &CallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_grant(&mut self, ctx: &GrantContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deny(&mut self, ctx: &DenyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_explain(&mut self, ctx: &ExplainContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_show(&mut self, ctx: &ShowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_reset(&mut self, ctx: &ResetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_commit(&mut self, ctx: &CommitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_update(&mut self, ctx: &UpdateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableElements}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeCreateTableClauses}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeCreateTableClauses(&mut self, ctx: &SnowflakeCreateTableClausesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
	fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#constraintProperties}.
	 * @param ctx the parse tree
	 */
	fn visit_constraintProperties(&mut self, ctx: &ConstraintPropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeValueRow}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeValueRow(&mut self, ctx: &SnowflakeValueRowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeValueItem}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeValueItem(&mut self, ctx: &SnowflakeValueItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeCreateExternalTableClauses}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeCreateExternalTableClauses(&mut self, ctx: &SnowflakeCreateExternalTableClausesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#partitionedByNameSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionedByNameSpec(&mut self, ctx: &PartitionedByNameSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#compressionSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_compressionSpec(&mut self, ctx: &CompressionSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#headerRowSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_headerRowSpec(&mut self, ctx: &HeaderRowSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#delimiterSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_delimiterSpec(&mut self, ctx: &DelimiterSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#query}.
	 * @param ctx the parse tree
	 */
	fn visit_query(&mut self, ctx: &QueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#with}.
	 * @param ctx the parse tree
	 */
	fn visit_with(&mut self, ctx: &WithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableElement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#inlineConstraint}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineConstraint(&mut self, ctx: &InlineConstraintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnDefinitionForView}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#externalColumnDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_externalColumnDefinition(&mut self, ctx: &ExternalColumnDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnName}.
	 * @param ctx the parse tree
	 */
	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnOption}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link SnowflakeParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#properties}.
	 * @param ctx the parse tree
	 */
	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link SnowflakeParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link SnowflakeParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryLimit}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
	 * labeled alternative in {@link SnowflakeParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimitTargetRedshiftSnowflake(&mut self, ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowOrRows}.
	 * @param ctx the parse tree
	 */
	fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperation}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_table(&mut self, ctx: &TableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sortItem}.
	 * @param ctx the parse tree
	 */
	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#connectByItem}.
	 * @param ctx the parse tree
	 */
	fn visit_connectByItem(&mut self, ctx: &ConnectByItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#connectBy}.
	 * @param ctx the parse tree
	 */
	fn visit_connectBy(&mut self, ctx: &ConnectByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#replaceDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link SnowflakeParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link SnowflakeParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollup(&mut self, ctx: &RollupContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_cube(&mut self, ctx: &CubeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#orderBy}.
	 * @param ctx the parse tree
	 */
	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SnowflakeParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link SnowflakeParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#multiSelect}.
	 * @param ctx the parse tree
	 */
	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#selectStar}.
	 * @param ctx the parse tree
	 */
	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#relation}.
	 * @param ctx the parse tree
	 */
	fn visit_relation(&mut self, ctx: &RelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code asofJoinRelation}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_asofJoinRelation(&mut self, ctx: &AsofJoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#joinType}.
	 * @param ctx the parse tree
	 */
	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code aliased2}
	 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_aliased2(&mut self, ctx: &Aliased2Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault}
	 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#samplePercentage}.
	 * @param ctx the parse tree
	 */
	fn visit_samplePercentage(&mut self, ctx: &SamplePercentageContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleCount}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleCount(&mut self, ctx: &SampleCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleSeed}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleSeed(&mut self, ctx: &SampleSeedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#patternRecognitionTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#patternRecognition}.
	 * @param ctx the parse tree
	 */
	fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#measureDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowsPerMatch}.
	 * @param ctx the parse tree
	 */
	fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#emptyMatchHandling}.
	 * @param ctx the parse tree
	 */
	fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#skipTo}.
	 * @param ctx the parse tree
	 */
	fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#subsetDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#atBefore}.
	 * @param ctx the parse tree
	 */
	fn visit_atBefore(&mut self, ctx: &AtBeforeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code aliased}
	 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_aliased(&mut self, ctx: &AliasedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code directory}
	 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_directory(&mut self, ctx: &DirectoryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#changesRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_changesRelation(&mut self, ctx: &ChangesRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pattern}
	 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_pattern(&mut self, ctx: &PatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelation2}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelation2(&mut self, ctx: &AliasedRelation2Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntoDefault}
	 * labeled alternative in {@link SnowflakeParser#pivotInto}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntoDefault(&mut self, ctx: &PivotIntoDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link SnowflakeParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntosAny}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntosAny(&mut self, ctx: &PivotIntosAnyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntosQuery}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntosQuery(&mut self, ctx: &PivotIntosQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_pivot(&mut self, ctx: &PivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lateral}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_lateral(&mut self, ctx: &LateralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code validate}
	 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_validate(&mut self, ctx: &ValidateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#descriptorField}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultBooleanExpression}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_or(&mut self, ctx: &OrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_and(&mut self, ctx: &AndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonComparisonExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inList(&mut self, ctx: &InListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code likeAny}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_likeAny(&mut self, ctx: &LikeAnyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_collate(&mut self, ctx: &CollateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code regexp}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_regexp(&mut self, ctx: &RegexpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rlike}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_rlike(&mut self, ctx: &RlikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_like(&mut self, ctx: &LikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code similarTo}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mod}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_mod(&mut self, ctx: &ModContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code firstValueFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_firstValueFunction(&mut self, ctx: &FirstValueFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnReferenceByPosition}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_columnReferenceByPosition(&mut self, ctx: &ColumnReferenceByPositionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueDereference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueDereference(&mut self, ctx: &ValueDereferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierExpression(&mut self, ctx: &IdentifierExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arrayAggFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arrayAggFunction(&mut self, ctx: &ArrayAggFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decode}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_decode(&mut self, ctx: &DecodeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dereferenceByPosition}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dereferenceByPosition(&mut self, ctx: &DereferenceByPositionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileContFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_cast(&mut self, ctx: &CastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code minhash}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_minhash(&mut self, ctx: &MinhashContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_trim(&mut self, ctx: &TrimContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_array(&mut self, ctx: &ArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code castOperator}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code objectLiteral}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_objectLiteral(&mut self, ctx: &ObjectLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_extract(&mut self, ctx: &ExtractContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_measure(&mut self, ctx: &MeasureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_variable(&mut self, ctx: &VariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_exists(&mut self, ctx: &ExistsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileDiscFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_position(&mut self, ctx: &PositionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code filesNamedFunctionArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_filesNamedFunctionArgument(&mut self, ctx: &FilesNamedFunctionArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multiArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKey}.
	 * @param ctx the parse tree
	 */
	fn visit_dereferenceKey(&mut self, ctx: &DereferenceKeyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKeyElement}.
	 * @param ctx the parse tree
	 */
	fn visit_dereferenceKeyElement(&mut self, ctx: &DereferenceKeyElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKeyText}.
	 * @param ctx the parse tree
	 */
	fn visit_dereferenceKeyText(&mut self, ctx: &DereferenceKeyTextContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionName}.
	 * @param ctx the parse tree
	 */
	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#namedParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#stageFileSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_stageFileSpec(&mut self, ctx: &StageFileSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#field}.
	 * @param ctx the parse tree
	 */
	fn visit_field(&mut self, ctx: &FieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#processingMode}.
	 * @param ctx the parse tree
	 */
	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dollarQuotedStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_dollarQuotedStringLiteral(&mut self, ctx: &DollarQuotedStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#interval}.
	 * @param ctx the parse tree
	 */
	fn visit_interval(&mut self, ctx: &IntervalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#intervalField}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#normalForm}.
	 * @param ctx the parse tree
	 */
	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link SnowflakeParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link SnowflakeParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doublePrecisionType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code characterVarying}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_characterVarying(&mut self, ctx: &CharacterVaryingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dateTimeType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyStructType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code structuredObjectType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_structuredObjectType(&mut self, ctx: &StructuredObjectTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowField}.
	 * @param ctx the parse tree
	 */
	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#whenClause}.
	 * @param ctx the parse tree
	 */
	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#filter}.
	 * @param ctx the parse tree
	 */
	fn visit_filter(&mut self, ctx: &FilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#over}.
	 * @param ctx the parse tree
	 */
	fn visit_over(&mut self, ctx: &OverContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#frameExtent}.
	 * @param ctx the parse tree
	 */
	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link SnowflakeParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link SnowflakeParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#privilege}.
	 * @param ctx the parse tree
	 */
	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierFunction}
	 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierFunction(&mut self, ctx: &IdentifierFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonquotedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dashedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#maybeDashedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rangeType}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code strictIdentifierDefault}
	 * labeled alternative in {@link SnowflakeParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code strictNonReservedIdentifier}
	 * labeled alternative in {@link SnowflakeParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pathComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#identifierList}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentAny}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentAny(&mut self, ctx: &SnowflakeFunctionArgumentAnyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentArray}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentArray(&mut self, ctx: &SnowflakeFunctionArgumentArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentObject}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentObject(&mut self, ctx: &SnowflakeFunctionArgumentObjectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentMap}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentMap(&mut self, ctx: &SnowflakeFunctionArgumentMapContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentVector}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentVector(&mut self, ctx: &SnowflakeFunctionArgumentVectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentFloat}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentFloat(&mut self, ctx: &SnowflakeFunctionArgumentFloatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentDefault}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeFunctionArgumentDefault(&mut self, ctx: &SnowflakeFunctionArgumentDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArguments}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeShowFunctionArguments(&mut self, ctx: &SnowflakeShowFunctionArgumentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArgumentsList}.
	 * @param ctx the parse tree
	 */
	fn visit_snowflakeShowFunctionArgumentsList(&mut self, ctx: &SnowflakeShowFunctionArgumentsListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) { self.visit_children(ctx) }

}

pub trait SnowflakeVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= SnowflakeParserContextType>{
	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#singleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneType}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_use(&mut self, ctx: &UseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createExternalTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createExternalTable(&mut self, ctx: &CreateExternalTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTableAsSelect}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeCreateTableAsSelect(&mut self, ctx: &SnowflakeCreateTableAsSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTableClone}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTableClone(&mut self, ctx: &CreateTableCloneContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createDynamicTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createDynamicTable(&mut self, ctx: &CreateDynamicTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createEventTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createEventTable(&mut self, ctx: &CreateEventTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createIcebergTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createIcebergTable(&mut self, ctx: &CreateIcebergTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRecursiveTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeCreateTable(&mut self, ctx: &SnowflakeCreateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeCreateTableUsingTemplate}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeCreateTableUsingTemplate(&mut self, ctx: &SnowflakeCreateTableUsingTemplateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeInsertInto}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeInsertInto(&mut self, ctx: &SnowflakeInsertIntoContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_set(&mut self, ctx: &SetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createStage}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createStage(&mut self, ctx: &CreateStageContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createJavaFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createJavaFunction(&mut self, ctx: &CreateJavaFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createJarFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createJarFunction(&mut self, ctx: &CreateJarFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createJSFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createPythonFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createPythonFunction(&mut self, ctx: &CreatePythonFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createModuleFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createModuleFunction(&mut self, ctx: &CreateModuleFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createScalaFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createScalaFunction(&mut self, ctx: &CreateScalaFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createScalaJarFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createScalaJarFunction(&mut self, ctx: &CreateScalaJarFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSqlFunction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createPythonProcedure}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createPythonProcedure(&mut self, ctx: &CreatePythonProcedureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createAnonymousPythonProcedure}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createAnonymousPythonProcedure(&mut self, ctx: &CreateAnonymousPythonProcedureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unset}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_unset(&mut self, ctx: &UnsetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_merge(&mut self, ctx: &MergeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code alter}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_alter(&mut self, ctx: &AlterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code begin}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_begin(&mut self, ctx: &BeginContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createFoo}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_drop(&mut self, ctx: &DropContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_delete(&mut self, ctx: &DeleteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_comment(&mut self, ctx: &CommentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_call(&mut self, ctx: &CallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_grant(&mut self, ctx: &GrantContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deny(&mut self, ctx: &DenyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_explain(&mut self, ctx: &ExplainContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_show(&mut self, ctx: &ShowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_reset(&mut self, ctx: &ResetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_commit(&mut self, ctx: &CommitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link SnowflakeParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_update(&mut self, ctx: &UpdateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableElements}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeCreateTableClauses}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeCreateTableClauses(&mut self, ctx: &SnowflakeCreateTableClausesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
		fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#constraintProperties}.
	 * @param ctx the parse tree
	 */
		fn visit_constraintProperties(&mut self, ctx: &ConstraintPropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeValueRow}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeValueRow(&mut self, ctx: &SnowflakeValueRowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeValueItem}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeValueItem(&mut self, ctx: &SnowflakeValueItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeCreateExternalTableClauses}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeCreateExternalTableClauses(&mut self, ctx: &SnowflakeCreateExternalTableClausesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#locationSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#partitionedByNameSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionedByNameSpec(&mut self, ctx: &PartitionedByNameSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#compressionSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_compressionSpec(&mut self, ctx: &CompressionSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#headerRowSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_headerRowSpec(&mut self, ctx: &HeaderRowSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#delimiterSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_delimiterSpec(&mut self, ctx: &DelimiterSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#query}.
	 * @param ctx the parse tree
	 */
		fn visit_query(&mut self, ctx: &QueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#with}.
	 * @param ctx the parse tree
	 */
		fn visit_with(&mut self, ctx: &WithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableElement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#inlineConstraint}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineConstraint(&mut self, ctx: &InlineConstraintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnDefinitionForView}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#externalColumnDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_externalColumnDefinition(&mut self, ctx: &ExternalColumnDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnName}.
	 * @param ctx the parse tree
	 */
		fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnOption}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link SnowflakeParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#properties}.
	 * @param ctx the parse tree
	 */
		fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link SnowflakeParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link SnowflakeParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#propertyKey}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link SnowflakeParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryLimit}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
	 * labeled alternative in {@link SnowflakeParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimitTargetRedshiftSnowflake(&mut self, ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowOrRows}.
	 * @param ctx the parse tree
	 */
		fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperation}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#inlineTable}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_table(&mut self, ctx: &TableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sortItem}.
	 * @param ctx the parse tree
	 */
		fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#connectByItem}.
	 * @param ctx the parse tree
	 */
		fn visit_connectByItem(&mut self, ctx: &ConnectByItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#querySpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#connectBy}.
	 * @param ctx the parse tree
	 */
		fn visit_connectBy(&mut self, ctx: &ConnectByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#replaceDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
		fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link SnowflakeParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link SnowflakeParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollup(&mut self, ctx: &RollupContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_cube(&mut self, ctx: &CubeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link SnowflakeParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#groupingSet}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#orderBy}.
	 * @param ctx the parse tree
	 */
		fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#namedQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link SnowflakeParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link SnowflakeParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#multiSelect}.
	 * @param ctx the parse tree
	 */
		fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#selectStar}.
	 * @param ctx the parse tree
	 */
		fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#relation}.
	 * @param ctx the parse tree
	 */
		fn visit_relation(&mut self, ctx: &RelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code asofJoinRelation}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_asofJoinRelation(&mut self, ctx: &AsofJoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#joinType}.
	 * @param ctx the parse tree
	 */
		fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code aliased2}
	 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_aliased2(&mut self, ctx: &Aliased2Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault}
	 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#samplePercentage}.
	 * @param ctx the parse tree
	 */
		fn visit_samplePercentage(&mut self, ctx: &SamplePercentageContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleCount}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleCount(&mut self, ctx: &SampleCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#sampleSeed}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleSeed(&mut self, ctx: &SampleSeedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
		fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#patternRecognitionTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#patternRecognition}.
	 * @param ctx the parse tree
	 */
		fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#measureDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowsPerMatch}.
	 * @param ctx the parse tree
	 */
		fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#emptyMatchHandling}.
	 * @param ctx the parse tree
	 */
		fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#skipTo}.
	 * @param ctx the parse tree
	 */
		fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#subsetDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#atBefore}.
	 * @param ctx the parse tree
	 */
		fn visit_atBefore(&mut self, ctx: &AtBeforeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code aliased}
	 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_aliased(&mut self, ctx: &AliasedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code directory}
	 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_directory(&mut self, ctx: &DirectoryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#changesRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_changesRelation(&mut self, ctx: &ChangesRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pattern}
	 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_pattern(&mut self, ctx: &PatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelation2}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelation2(&mut self, ctx: &AliasedRelation2Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntoDefault}
	 * labeled alternative in {@link SnowflakeParser#pivotInto}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntoDefault(&mut self, ctx: &PivotIntoDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link SnowflakeParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntosAny}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntosAny(&mut self, ctx: &PivotIntosAnyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntosQuery}
	 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntosQuery(&mut self, ctx: &PivotIntosQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_pivot(&mut self, ctx: &PivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#columnAliases}.
	 * @param ctx the parse tree
	 */
		fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lateral}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_lateral(&mut self, ctx: &LateralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code validate}
	 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_validate(&mut self, ctx: &ValidateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#tableArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#descriptorField}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
		fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultBooleanExpression}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_or(&mut self, ctx: &OrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_and(&mut self, ctx: &AndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
		fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonComparisonExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_between(&mut self, ctx: &BetweenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inList(&mut self, ctx: &InListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code likeAny}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_likeAny(&mut self, ctx: &LikeAnyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_collate(&mut self, ctx: &CollateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code regexp}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_regexp(&mut self, ctx: &RegexpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rlike}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_rlike(&mut self, ctx: &RlikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_like(&mut self, ctx: &LikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code similarTo}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link SnowflakeParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link SnowflakeParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mod}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_mod(&mut self, ctx: &ModContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code firstValueFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_firstValueFunction(&mut self, ctx: &FirstValueFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnReferenceByPosition}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_columnReferenceByPosition(&mut self, ctx: &ColumnReferenceByPositionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueDereference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueDereference(&mut self, ctx: &ValueDereferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierExpression(&mut self, ctx: &IdentifierExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arrayAggFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arrayAggFunction(&mut self, ctx: &ArrayAggFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decode}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_decode(&mut self, ctx: &DecodeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dereferenceByPosition}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dereferenceByPosition(&mut self, ctx: &DereferenceByPositionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileContFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_cast(&mut self, ctx: &CastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code minhash}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_minhash(&mut self, ctx: &MinhashContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_trim(&mut self, ctx: &TrimContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_array(&mut self, ctx: &ArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code castOperator}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code objectLiteral}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_objectLiteral(&mut self, ctx: &ObjectLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_extract(&mut self, ctx: &ExtractContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_measure(&mut self, ctx: &MeasureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_variable(&mut self, ctx: &VariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_exists(&mut self, ctx: &ExistsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileDiscFunction}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_position(&mut self, ctx: &PositionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code filesNamedFunctionArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_filesNamedFunctionArgument(&mut self, ctx: &FilesNamedFunctionArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multiArgument}
	 * labeled alternative in {@link SnowflakeParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
		fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SnowflakeParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKey}.
	 * @param ctx the parse tree
	 */
		fn visit_dereferenceKey(&mut self, ctx: &DereferenceKeyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKeyElement}.
	 * @param ctx the parse tree
	 */
		fn visit_dereferenceKeyElement(&mut self, ctx: &DereferenceKeyElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dereferenceKeyText}.
	 * @param ctx the parse tree
	 */
		fn visit_dereferenceKeyText(&mut self, ctx: &DereferenceKeyTextContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#functionName}.
	 * @param ctx the parse tree
	 */
		fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#namedParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#stageFileSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_stageFileSpec(&mut self, ctx: &StageFileSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#field}.
	 * @param ctx the parse tree
	 */
		fn visit_field(&mut self, ctx: &FieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#processingMode}.
	 * @param ctx the parse tree
	 */
		fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
		fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dollarQuotedStringLiteral}
	 * labeled alternative in {@link SnowflakeParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_dollarQuotedStringLiteral(&mut self, ctx: &DollarQuotedStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
		fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#booleanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#interval}.
	 * @param ctx the parse tree
	 */
		fn visit_interval(&mut self, ctx: &IntervalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#intervalField}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#normalForm}.
	 * @param ctx the parse tree
	 */
		fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link SnowflakeParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link SnowflakeParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doublePrecisionType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code characterVarying}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_characterVarying(&mut self, ctx: &CharacterVaryingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dateTimeType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyStructType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code structuredObjectType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_structuredObjectType(&mut self, ctx: &StructuredObjectTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rowField}.
	 * @param ctx the parse tree
	 */
		fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#typeParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#whenClause}.
	 * @param ctx the parse tree
	 */
		fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#filter}.
	 * @param ctx the parse tree
	 */
		fn visit_filter(&mut self, ctx: &FilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#over}.
	 * @param ctx the parse tree
	 */
		fn visit_over(&mut self, ctx: &OverContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#windowFrame}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#frameExtent}.
	 * @param ctx the parse tree
	 */
		fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link SnowflakeParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link SnowflakeParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link SnowflakeParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link SnowflakeParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#privilege}.
	 * @param ctx the parse tree
	 */
		fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierFunction}
	 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierFunction(&mut self, ctx: &IdentifierFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonquotedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#dashedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#maybeDashedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#rangeType}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link SnowflakeParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code strictIdentifierDefault}
	 * labeled alternative in {@link SnowflakeParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code strictNonReservedIdentifier}
	 * labeled alternative in {@link SnowflakeParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#pathComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#identifierList}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SnowflakeParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentAny}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentAny(&mut self, ctx: &SnowflakeFunctionArgumentAnyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentArray}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentArray(&mut self, ctx: &SnowflakeFunctionArgumentArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentObject}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentObject(&mut self, ctx: &SnowflakeFunctionArgumentObjectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentMap}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentMap(&mut self, ctx: &SnowflakeFunctionArgumentMapContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentVector}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentVector(&mut self, ctx: &SnowflakeFunctionArgumentVectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentFloat}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentFloat(&mut self, ctx: &SnowflakeFunctionArgumentFloatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code snowflakeFunctionArgumentDefault}
	 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeFunctionArgumentDefault(&mut self, ctx: &SnowflakeFunctionArgumentDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArguments}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeShowFunctionArguments(&mut self, ctx: &SnowflakeShowFunctionArgumentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArgumentsList}.
	 * @param ctx the parse tree
	 */
		fn visit_snowflakeShowFunctionArgumentsList(&mut self, ctx: &SnowflakeShowFunctionArgumentsListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link SnowflakeParser#nonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> SnowflakeVisitor<'input> for T
where
	T: SnowflakeVisitorCompat<'input>
{
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_multipleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_singleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_standaloneExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_standaloneQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_standaloneType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_statementDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_use(&mut self, ctx: &UseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_use(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dropSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_renameSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setSchemaAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dropTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dropView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createExternalTable(&mut self, ctx: &CreateExternalTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createExternalTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeCreateTableAsSelect(&mut self, ctx: &SnowflakeCreateTableAsSelectContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeCreateTableAsSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTableClone(&mut self, ctx: &CreateTableCloneContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createTableClone(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createDynamicTable(&mut self, ctx: &CreateDynamicTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createDynamicTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createEventTable(&mut self, ctx: &CreateEventTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createEventTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createIcebergTable(&mut self, ctx: &CreateIcebergTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createIcebergTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createRecursiveTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeCreateTable(&mut self, ctx: &SnowflakeCreateTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeCreateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeCreateTableUsingTemplate(&mut self, ctx: &SnowflakeCreateTableUsingTemplateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeCreateTableUsingTemplate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeInsertInto(&mut self, ctx: &SnowflakeInsertIntoContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeInsertInto(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_set(&mut self, ctx: &SetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_set(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_showColumns(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createStage(&mut self, ctx: &CreateStageContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createStage(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createJavaFunction(&mut self, ctx: &CreateJavaFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createJavaFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createJarFunction(&mut self, ctx: &CreateJarFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createJarFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createJSFunction(&mut self, ctx: &CreateJSFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createJSFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createPythonFunction(&mut self, ctx: &CreatePythonFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createPythonFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createModuleFunction(&mut self, ctx: &CreateModuleFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createModuleFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createScalaFunction(&mut self, ctx: &CreateScalaFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createScalaFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createScalaJarFunction(&mut self, ctx: &CreateScalaJarFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createScalaJarFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSqlFunction(&mut self, ctx: &CreateSqlFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createSqlFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createPythonProcedure(&mut self, ctx: &CreatePythonProcedureContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createPythonProcedure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createAnonymousPythonProcedure(&mut self, ctx: &CreateAnonymousPythonProcedureContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createAnonymousPythonProcedure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unset(&mut self, ctx: &UnsetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_merge(&mut self, ctx: &MergeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_merge(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_alter(&mut self, ctx: &AlterContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_alter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_begin(&mut self, ctx: &BeginContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_begin(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createFoo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_drop(&mut self, ctx: &DropContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_drop(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delete(&mut self, ctx: &DeleteContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_delete(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_truncateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comment(&mut self, ctx: &CommentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_comment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_renameTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_addColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_renameColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dropColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setColumnType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setTableAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setTableProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableExecute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_analyze(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_refreshMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_renameMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setMaterializedViewProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_renameView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setViewAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_call(&mut self, ctx: &CallContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_call(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createRole(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grant(&mut self, ctx: &GrantContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_grant(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_revoke(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deny(&mut self, ctx: &DenyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_deny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explain(&mut self, ctx: &ExplainContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_explain(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_show(&mut self, ctx: &ShowContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_show(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_reset(&mut self, ctx: &ResetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_reset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_startTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commit(&mut self, ctx: &CommitContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_commit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rollback(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_prepare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_deallocate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_execute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_describeInput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_describeOutput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_update(&mut self, ctx: &UpdateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_update(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableElements(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unpivotNullClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeCreateTableClauses(&mut self, ctx: &SnowflakeCreateTableClausesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeCreateTableClauses(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableConstraint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_constraintProperties(&mut self, ctx: &ConstraintPropertiesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_constraintProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeValueRow(&mut self, ctx: &SnowflakeValueRowContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeValueRow(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeValueItem(&mut self, ctx: &SnowflakeValueItemContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeValueItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeCreateExternalTableClauses(&mut self, ctx: &SnowflakeCreateExternalTableClausesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeCreateExternalTableClauses(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_locationSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionedByNameSpec(&mut self, ctx: &PartitionedByNameSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_partitionedByNameSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_createFileFormat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_compressionSpec(&mut self, ctx: &CompressionSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_compressionSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_headerRowSpec(&mut self, ctx: &HeaderRowSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_headerRowSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delimiterSpec(&mut self, ctx: &DelimiterSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_delimiterSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_query(&mut self, ctx: &QueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_query(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_with(&mut self, ctx: &WithContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_with(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineConstraint(&mut self, ctx: &InlineConstraintContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_inlineConstraint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnDefinitionForView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_externalColumnDefinition(&mut self, ctx: &ExternalColumnDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_externalColumnDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_fieldDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnNameComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnSchemaWithMetadata(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnOptionList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnSchemaSimpleType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_properties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_propertyAssignments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nestedProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_defaultProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_propertyKey(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_defaultPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_identifierPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_expressionPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryNoWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryLimit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimitTargetRedshiftSnowflake(&mut self, ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryLimitTargetRedshiftSnowflake(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rowOrRows(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_limitRowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setOperation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setOperationIntersect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setIntersectOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_setQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_inlineTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryPrimaryDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_table(&mut self, ctx: &TableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_table(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_subquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sortItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_connectByItem(&mut self, ctx: &ConnectByItemContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_connectByItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_querySpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_connectBy(&mut self, ctx: &ConnectByContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_connectBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_replaceDefinition(&mut self, ctx: &ReplaceDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_replaceDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_querySelectItems(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aggregationClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_groupByAll(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_groupByDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollup(&mut self, ctx: &RollupContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rollup(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cube(&mut self, ctx: &CubeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_cube(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_multipleGroupingSets(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_singleGroupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_groupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_windowDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_windowSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_windowSpecificationPartitionBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_orderBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_namedQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_selectItemAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_selectSingle(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_selectMulti(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_multiSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_selectStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relation(&mut self, ctx: &RelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_relation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_asofJoinRelation(&mut self, ctx: &AsofJoinRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_asofJoinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_relationDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_joinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_joinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_joinCriteria(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_noJoinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliased2(&mut self, ctx: &Aliased2Context<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aliased2(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_inlineTableDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sampledRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sampleOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sampleMethod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_samplePercentage(&mut self, ctx: &SamplePercentageContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_samplePercentage(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleCount(&mut self, ctx: &SampleCountContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sampleCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleSeed(&mut self, ctx: &SampleSeedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_sampleSeed(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_trimsSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_listAggOverflowBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_listaggCountIndication(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternRecognitionTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternRecognition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_measureDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rowsPerMatch(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_emptyMatchHandling(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_skipTo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_subsetDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_variableDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atBefore(&mut self, ctx: &AtBeforeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_atBefore(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliased(&mut self, ctx: &AliasedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aliased(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_directory(&mut self, ctx: &DirectoryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_directory(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_changesRelation(&mut self, ctx: &ChangesRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_changesRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pattern(&mut self, ctx: &PatternContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableFunctionInvocation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelation2(&mut self, ctx: &AliasedRelation2Context<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aliasedRelation2(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotAggregates(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntoDefault(&mut self, ctx: &PivotIntoDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotIntoDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotAsAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_singleColumnUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnsToUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_singleColumnUnpivotDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotIntosDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntosAny(&mut self, ctx: &PivotIntosAnyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotIntosAny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntosQuery(&mut self, ctx: &PivotIntosQueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivotIntosQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivot(&mut self, ctx: &PivotContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aliasedRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_aliasedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnAliases(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateral(&mut self, ctx: &LateralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_lateral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_subqueryRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_parenthesizedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_validate(&mut self, ctx: &ValidateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_validate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_defaultTableFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableFunctionArgumentCopartition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableFunctionArgumentName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableFunctionArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableArgumentTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_tableArgumentQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_descriptorArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_descriptorField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_copartitionTables(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_expression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_defaultBooleanExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_logicalNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_or(&mut self, ctx: &OrContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_or(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_predicated(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_and(&mut self, ctx: &AndContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_and(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_comparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_quantifiedComparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nonComparisonExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_between(&mut self, ctx: &BetweenContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_between(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inList(&mut self, ctx: &InListContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_inList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_inSubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_likeAny(&mut self, ctx: &LikeAnyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_likeAny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_collate(&mut self, ctx: &CollateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_collate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexp(&mut self, ctx: &RegexpContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_regexp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rlike(&mut self, ctx: &RlikeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rlike(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_like(&mut self, ctx: &LikeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_like(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_similarTo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nullPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_distinctFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unknownPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_valueExpressionDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_concatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_arithmeticBinary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_arithmeticUnary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_atTimeZone(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dereference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mod(&mut self, ctx: &ModContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_mod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_firstValueFunction(&mut self, ctx: &FirstValueFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_firstValueFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnReferenceByPosition(&mut self, ctx: &ColumnReferenceByPositionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnReferenceByPosition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueDereference(&mut self, ctx: &ValueDereferenceContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_valueDereference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierExpression(&mut self, ctx: &IdentifierExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_identifierExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arrayAggFunction(&mut self, ctx: &ArrayAggFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_arrayAggFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decode(&mut self, ctx: &DecodeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_decode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereferenceByPosition(&mut self, ctx: &DereferenceByPositionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dereferenceByPosition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_countStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_percentileContFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cast(&mut self, ctx: &CastContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_cast(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_minhash(&mut self, ctx: &MinhashContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_minhash(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_lambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_parenthesizedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trim(&mut self, ctx: &TrimContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_trim(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_array(&mut self, ctx: &ArrayContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_array(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_normalize(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_castOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_simpleCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_columnReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rowConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_objectLiteral(&mut self, ctx: &ObjectLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_objectLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_subqueryExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_constantDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extract(&mut self, ctx: &ExtractContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_extract(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measure(&mut self, ctx: &MeasureContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_measure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variable(&mut self, ctx: &VariableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_variable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exists(&mut self, ctx: &ExistsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_exists(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_percentileDiscFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_position(&mut self, ctx: &PositionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_position(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_listagg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_searchedCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionCallHead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionCallTail(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_positionalArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_namedArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_filesNamedFunctionArgument(&mut self, ctx: &FilesNamedFunctionArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_filesNamedFunctionArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_multiArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionExtraArguments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nullLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_intervalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_numericLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_booleanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_binaryLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_typeConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereferenceKey(&mut self, ctx: &DereferenceKeyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dereferenceKey(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereferenceKeyElement(&mut self, ctx: &DereferenceKeyElementContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dereferenceKeyElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereferenceKeyText(&mut self, ctx: &DereferenceKeyTextContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dereferenceKeyText(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedParameter(&mut self, ctx: &NamedParameterContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_namedParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stageFileSpec(&mut self, ctx: &StageFileSpecContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_stageFileSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_field(&mut self, ctx: &FieldContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_field(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_processingMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nullTreatment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_basicStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unicodeStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dollarQuotedStringLiteral(&mut self, ctx: &DollarQuotedStringLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dollarQuotedStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_timeZoneSpecifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_comparisonOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_comparisonQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_booleanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_interval(&mut self, ctx: &IntervalContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_interval(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_intervalField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_normalForm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_typeIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_typeNotNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_typeNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_functionSignatureGenericType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_doublePrecisionType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_characterVarying(&mut self, ctx: &CharacterVaryingContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_characterVarying(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dateTimeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_lambdaType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyStructType(&mut self, ctx: &LegacyStructTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_legacyStructType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structuredObjectType(&mut self, ctx: &StructuredObjectTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_structuredObjectType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_legacyMapType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_primitiveType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_typeParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_whenClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_filter(&mut self, ctx: &FilterContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_filter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_over(&mut self, ctx: &OverContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_over(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_windowFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_frameExtent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unboundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_currentRowBound(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_boundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_quantifiedPrimary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternConcatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternAlternation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternVariable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_emptyPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_patternPermutation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_groupedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_partitionStartAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_partitionEndAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_excludedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_zeroOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_oneOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_zeroOrOneQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rangeQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_isolationLevel(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_transactionAccessMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_readUncommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_readCommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_repeatableRead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_serializable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_privilege(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_qualifiedNameDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierFunction(&mut self, ctx: &IdentifierFunctionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_identifierFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonquotedIdentifier(&mut self, ctx: &NonquotedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nonquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dashedIdentifier(&mut self, ctx: &DashedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_dashedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_maybeDashedIdentifier(&mut self, ctx: &MaybeDashedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_maybeDashedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_queryPeriod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rangeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unspecifiedPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_userPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_rolePrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_strictIdentifierDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_strictNonReservedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_unquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_quotedIdentifierDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_quotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_pathComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_standaloneIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_identifierList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_identifierSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_decimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_doubleLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_integerLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentAny(&mut self, ctx: &SnowflakeFunctionArgumentAnyContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentAny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentArray(&mut self, ctx: &SnowflakeFunctionArgumentArrayContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentArray(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentObject(&mut self, ctx: &SnowflakeFunctionArgumentObjectContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentObject(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentMap(&mut self, ctx: &SnowflakeFunctionArgumentMapContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentMap(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentVector(&mut self, ctx: &SnowflakeFunctionArgumentVectorContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentVector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentFloat(&mut self, ctx: &SnowflakeFunctionArgumentFloatContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentFloat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeFunctionArgumentDefault(&mut self, ctx: &SnowflakeFunctionArgumentDefaultContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeFunctionArgumentDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeShowFunctionArguments(&mut self, ctx: &SnowflakeShowFunctionArgumentsContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeShowFunctionArguments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_snowflakeShowFunctionArgumentsList(&mut self, ctx: &SnowflakeShowFunctionArgumentsListContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_snowflakeShowFunctionArgumentsList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_strictNonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>){
		let result = <Self as SnowflakeVisitorCompat>::visit_nonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}