#![allow(nonstandard_style)]
// Generated from Databricks.g4 by ANTLR 4.8
use antlr_rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::databricksparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link DatabricksParser}.
 */
pub trait DatabricksVisitor<'input>: ParseTreeVisitor<'input,DatabricksParserContextType>{
	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_use(&mut self, ctx: &UseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTableAsSelect}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTableLike(&mut self, ctx: &CreateTableLikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dmlStatement}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dmlStatement(&mut self, ctx: &DmlStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_merge(&mut self, ctx: &MergeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_set(&mut self, ctx: &SetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_drop(&mut self, ctx: &DropContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_delete(&mut self, ctx: &DeleteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_comment(&mut self, ctx: &CommentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_grant(&mut self, ctx: &GrantContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_explain(&mut self, ctx: &ExplainContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_show(&mut self, ctx: &ShowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_reset(&mut self, ctx: &ResetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_commit(&mut self, ctx: &CommitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_update(&mut self, ctx: &UpdateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createFoo}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code alter}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_alter(&mut self, ctx: &AlterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code optimize}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_optimize(&mut self, ctx: &OptimizeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableElements}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierReference}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierReference(&mut self, ctx: &IdentifierReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierCommentList(&mut self, ctx: &IdentifierCommentListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierComment}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierComment(&mut self, ctx: &IdentifierCommentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#schemaBinding}.
	 * @param ctx the parse tree
	 */
	fn visit_schemaBinding(&mut self, ctx: &SchemaBindingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#createTableClauses}.
	 * @param ctx the parse tree
	 */
	fn visit_createTableClauses(&mut self, ctx: &CreateTableClausesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableProvider}.
	 * @param ctx the parse tree
	 */
	fn visit_tableProvider(&mut self, ctx: &TableProviderContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionTransform}
	 * labeled alternative in {@link DatabricksParser#partitionField}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionTransform(&mut self, ctx: &PartitionTransformContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionColumn}
	 * labeled alternative in {@link DatabricksParser#partitionField}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identityTransform}
	 * labeled alternative in {@link DatabricksParser#transform}.
	 * @param ctx the parse tree
	 */
	fn visit_identityTransform(&mut self, ctx: &IdentityTransformContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code applyTransform}
	 * labeled alternative in {@link DatabricksParser#transform}.
	 * @param ctx the parse tree
	 */
	fn visit_applyTransform(&mut self, ctx: &ApplyTransformContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#transformArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_transformArgument(&mut self, ctx: &TransformArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colType}.
	 * @param ctx the parse tree
	 */
	fn visit_colType(&mut self, ctx: &ColTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#skewSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_skewSpec(&mut self, ctx: &SkewSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#clusterBySpec}.
	 * @param ctx the parse tree
	 */
	fn visit_clusterBySpec(&mut self, ctx: &ClusterBySpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_bucketSpec(&mut self, ctx: &BucketSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#constantList}.
	 * @param ctx the parse tree
	 */
	fn visit_constantList(&mut self, ctx: &ConstantListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedConstantList(&mut self, ctx: &NestedConstantListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link DatabricksParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_rowFormatSerde(&mut self, ctx: &RowFormatSerdeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link DatabricksParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_rowFormatDelimited(&mut self, ctx: &RowFormatDelimitedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link DatabricksParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFileFormat(&mut self, ctx: &TableFileFormatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link DatabricksParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	fn visit_genericFileFormat(&mut self, ctx: &GenericFileFormatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#storageHandler}.
	 * @param ctx the parse tree
	 */
	fn visit_storageHandler(&mut self, ctx: &StorageHandlerContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#literalType}.
	 * @param ctx the parse tree
	 */
	fn visit_literalType(&mut self, ctx: &LiteralTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_singleInsertQuery(&mut self, ctx: &SingleInsertQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_multiInsertQuery(&mut self, ctx: &MultiInsertQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deleteFromTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_deleteFromTable(&mut self, ctx: &DeleteFromTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code updateTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_updateTable(&mut self, ctx: &UpdateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mergeIntoTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_mergeIntoTable(&mut self, ctx: &MergeIntoTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#ctes}.
	 * @param ctx the parse tree
	 */
	fn visit_ctes(&mut self, ctx: &CtesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
	fn visit_insertOverwriteTable(&mut self, ctx: &InsertOverwriteTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
	fn visit_insertIntoTable(&mut self, ctx: &InsertIntoTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertIntoReplaceWhere}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
	fn visit_insertIntoReplaceWhere(&mut self, ctx: &InsertIntoReplaceWhereContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
	fn visit_insertOverwriteHiveDir(&mut self, ctx: &InsertOverwriteHiveDirContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
	fn visit_insertOverwriteDir(&mut self, ctx: &InsertOverwriteDirContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
	fn visit_multiInsertQueryBody(&mut self, ctx: &MultiInsertQueryBodyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_tableAlias(&mut self, ctx: &TableAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#whereClause}.
	 * @param ctx the parse tree
	 */
	fn visit_whereClause(&mut self, ctx: &WhereClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setClause}.
	 * @param ctx the parse tree
	 */
	fn visit_setClause(&mut self, ctx: &SetClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#matchedClause}.
	 * @param ctx the parse tree
	 */
	fn visit_matchedClause(&mut self, ctx: &MatchedClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedClause}.
	 * @param ctx the parse tree
	 */
	fn visit_notMatchedClause(&mut self, ctx: &NotMatchedClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedBySourceClause}.
	 * @param ctx the parse tree
	 */
	fn visit_notMatchedBySourceClause(&mut self, ctx: &NotMatchedBySourceClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#optionsClause}.
	 * @param ctx the parse tree
	 */
	fn visit_optionsClause(&mut self, ctx: &OptionsClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionSpec(&mut self, ctx: &PartitionSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#lateralView}.
	 * @param ctx the parse tree
	 */
	fn visit_lateralView(&mut self, ctx: &LateralViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
	fn visit_fromStatementBody(&mut self, ctx: &FromStatementBodyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	fn visit_queryOrganization(&mut self, ctx: &QueryOrganizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#assignmentList}.
	 * @param ctx the parse tree
	 */
	fn visit_assignmentList(&mut self, ctx: &AssignmentListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#assignment}.
	 * @param ctx the parse tree
	 */
	fn visit_assignment(&mut self, ctx: &AssignmentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#matchedAction}.
	 * @param ctx the parse tree
	 */
	fn visit_matchedAction(&mut self, ctx: &MatchedActionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedAction}.
	 * @param ctx the parse tree
	 */
	fn visit_notMatchedAction(&mut self, ctx: &NotMatchedActionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedBySourceAction}.
	 * @param ctx the parse tree
	 */
	fn visit_notMatchedBySourceAction(&mut self, ctx: &NotMatchedBySourceActionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionVal(&mut self, ctx: &PartitionValContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#transformClause}.
	 * @param ctx the parse tree
	 */
	fn visit_transformClause(&mut self, ctx: &TransformClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectClause}.
	 * @param ctx the parse tree
	 */
	fn visit_selectClause(&mut self, ctx: &SelectClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#havingClause}.
	 * @param ctx the parse tree
	 */
	fn visit_havingClause(&mut self, ctx: &HavingClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
	fn visit_multipartIdentifierList(&mut self, ctx: &MultipartIdentifierListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#expressionSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_expressionSeq(&mut self, ctx: &ExpressionSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	fn visit_colTypeList(&mut self, ctx: &ColTypeListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#hint}.
	 * @param ctx the parse tree
	 */
	fn visit_hint(&mut self, ctx: &HintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_hintStatement(&mut self, ctx: &HintStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#query}.
	 * @param ctx the parse tree
	 */
	fn visit_query(&mut self, ctx: &QueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#with}.
	 * @param ctx the parse tree
	 */
	fn visit_with(&mut self, ctx: &WithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableElement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
	fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnDefinitionForView}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fieldDefinitions}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldDefinitions(&mut self, ctx: &FieldDefinitionsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnName}.
	 * @param ctx the parse tree
	 */
	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colDefinitionOption}.
	 * @param ctx the parse tree
	 */
	fn visit_colDefinitionOption(&mut self, ctx: &ColDefinitionOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#generationExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_generationExpression(&mut self, ctx: &GenerationExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#defaultExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultExpression(&mut self, ctx: &DefaultExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnOption}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link DatabricksParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#properties}.
	 * @param ctx the parse tree
	 */
	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link DatabricksParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link DatabricksParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryLimit}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDatabricks}
	 * labeled alternative in {@link DatabricksParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimitTargetDatabricks(&mut self, ctx: &QueryLimitTargetDatabricksContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowClause}.
	 * @param ctx the parse tree
	 */
	fn visit_windowClause(&mut self, ctx: &WindowClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperation}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_table(&mut self, ctx: &TableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sortItem}.
	 * @param ctx the parse tree
	 */
	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByWith}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByWith(&mut self, ctx: &GroupByWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpElementAnalytics}
	 * labeled alternative in {@link DatabricksParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_grpElementAnalytics(&mut self, ctx: &GrpElementAnalyticsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpElementExpression}
	 * labeled alternative in {@link DatabricksParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_grpElementExpression(&mut self, ctx: &GrpElementExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpAnalyticsSugar}
	 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
	fn visit_grpAnalyticsSugar(&mut self, ctx: &GrpAnalyticsSugarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpAnalyticsSets}
	 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
	fn visit_grpAnalyticsSets(&mut self, ctx: &GrpAnalyticsSetsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpSetsElementAnalytics}
	 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
	 * @param ctx the parse tree
	 */
	fn visit_grpSetsElementAnalytics(&mut self, ctx: &GrpSetsElementAnalyticsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grpSetsElementSet}
	 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
	 * @param ctx the parse tree
	 */
	fn visit_grpSetsElementSet(&mut self, ctx: &GrpSetsElementSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#orderBy}.
	 * @param ctx the parse tree
	 */
	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link DatabricksParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link DatabricksParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code structItemSingle}
	 * labeled alternative in {@link DatabricksParser#structItem}.
	 * @param ctx the parse tree
	 */
	fn visit_structItemSingle(&mut self, ctx: &StructItemSingleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code structItemMulti}
	 * labeled alternative in {@link DatabricksParser#structItem}.
	 * @param ctx the parse tree
	 */
	fn visit_structItemMulti(&mut self, ctx: &StructItemMultiContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiSelect}.
	 * @param ctx the parse tree
	 */
	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectStar}.
	 * @param ctx the parse tree
	 */
	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#relation}.
	 * @param ctx the parse tree
	 */
	fn visit_relation(&mut self, ctx: &RelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#joinType}.
	 * @param ctx the parse tree
	 */
	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sample}.
	 * @param ctx the parse tree
	 */
	fn visit_sample(&mut self, ctx: &SampleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleByPercentile(&mut self, ctx: &SampleByPercentileContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleByRows(&mut self, ctx: &SampleByRowsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleByBucket(&mut self, ctx: &SampleByBucketContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleByBytes(&mut self, ctx: &SampleByBytesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#lateralViewRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_lateralViewRelation(&mut self, ctx: &LateralViewRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lateralViewRelationTargetDefault}
	 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_lateralViewRelationTargetDefault(&mut self, ctx: &LateralViewRelationTargetDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lateralViewRelationTargetIncremental}
	 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_lateralViewRelationTargetIncremental(&mut self, ctx: &LateralViewRelationTargetIncrementalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#extensibleRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_extensibleRelation(&mut self, ctx: &ExtensibleRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code extensibleRelationTargetIncremental}
	 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_extensibleRelationTargetIncremental(&mut self, ctx: &ExtensibleRelationTargetIncrementalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code extensibleRelationTargetDefault}
	 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_extensibleRelationTargetDefault(&mut self, ctx: &ExtensibleRelationTargetDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relationExtensionJoin}
	 * labeled alternative in {@link DatabricksParser#relationExtension}.
	 * @param ctx the parse tree
	 */
	fn visit_relationExtensionJoin(&mut self, ctx: &RelationExtensionJoinContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relationExtensionPivot}
	 * labeled alternative in {@link DatabricksParser#relationExtension}.
	 * @param ctx the parse tree
	 */
	fn visit_relationExtensionPivot(&mut self, ctx: &RelationExtensionPivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code joinRelationDefault}
	 * labeled alternative in {@link DatabricksParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_joinRelationDefault(&mut self, ctx: &JoinRelationDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code joinRelationNatural}
	 * labeled alternative in {@link DatabricksParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_joinRelationNatural(&mut self, ctx: &JoinRelationNaturalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntoNamedExpression}
	 * labeled alternative in {@link DatabricksParser#pivotInto}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiColumnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#valueColumnSet}.
	 * @param ctx the parse tree
	 */
	fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnSetsToUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multiColumnUnpivotDefault}
	 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
	fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link DatabricksParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link DatabricksParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_pivot(&mut self, ctx: &PivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link DatabricksParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code streamTableTarget}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_streamTableTarget(&mut self, ctx: &StreamTableTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code sampledRelationDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelationDefault(&mut self, ctx: &SampledRelationDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionTableDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_functionTableDefault(&mut self, ctx: &FunctionTableDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#temporalClause}.
	 * @param ctx the parse tree
	 */
	fn visit_temporalClause(&mut self, ctx: &TemporalClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#version}.
	 * @param ctx the parse tree
	 */
	fn visit_version(&mut self, ctx: &VersionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link DatabricksParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultBooleanExpression}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_or(&mut self, ctx: &OrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_and(&mut self, ctx: &AndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nonComparisonExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inList(&mut self, ctx: &InListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code regexp}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_regexp(&mut self, ctx: &RegexpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedLike}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_like(&mut self, ctx: &LikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truePredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code falsePredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code structConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code anyValue}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_anyValue(&mut self, ctx: &AnyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decode}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_decode(&mut self, ctx: &DecodeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_substring(&mut self, ctx: &SubstringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileContFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_cast(&mut self, ctx: &CastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code namedStruct}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_namedStruct(&mut self, ctx: &NamedStructContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_trim(&mut self, ctx: &TrimContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_array(&mut self, ctx: &ArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tryCastOperator}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_tryCastOperator(&mut self, ctx: &TryCastOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arraysZip}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arraysZip(&mut self, ctx: &ArraysZipContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code castOperator}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code currentLike}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_currentLike(&mut self, ctx: &CurrentLikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code last}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_last(&mut self, ctx: &LastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code overlay}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_overlay(&mut self, ctx: &OverlayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_collate(&mut self, ctx: &CollateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonExtract}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonExtract(&mut self, ctx: &JsonExtractContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_extract(&mut self, ctx: &ExtractContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_measure(&mut self, ctx: &MeasureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_variable(&mut self, ctx: &VariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_exists(&mut self, ctx: &ExistsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fromJson}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_fromJson(&mut self, ctx: &FromJsonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileDiscFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_position(&mut self, ctx: &PositionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code mapFromEntries}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_mapFromEntries(&mut self, ctx: &MapFromEntriesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code modeFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_modeFunction(&mut self, ctx: &ModeFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code first}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_first(&mut self, ctx: &FirstContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multiArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code posParameterLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_posParameterLiteral(&mut self, ctx: &PosParameterLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code namedParameterLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_namedParameterLiteral(&mut self, ctx: &NamedParameterLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringConcatination}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
	fn visit_stringConcatination(&mut self, ctx: &StringConcatinationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPath}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonPath(&mut self, ctx: &JsonPathContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPathElement1}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonPathElement1(&mut self, ctx: &JsonPathElement1Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPathElement2}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonPathElement2(&mut self, ctx: &JsonPathElement2Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionName}.
	 * @param ctx the parse tree
	 */
	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#field}.
	 * @param ctx the parse tree
	 */
	fn visit_field(&mut self, ctx: &FieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link DatabricksParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doubleQuotedStringLiteral}
	 * labeled alternative in {@link DatabricksParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleQuotedStringLiteral(&mut self, ctx: &DoubleQuotedStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneInterval}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneInterval(&mut self, ctx: &StandaloneIntervalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#interval}.
	 * @param ctx the parse tree
	 */
	fn visit_interval(&mut self, ctx: &IntervalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalValue}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalValue(&mut self, ctx: &IntervalValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalValueField}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalValueField(&mut self, ctx: &IntervalValueFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalTypeField}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalTypeField(&mut self, ctx: &IntervalTypeFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#collateClause}.
	 * @param ctx the parse tree
	 */
	fn visit_collateClause(&mut self, ctx: &CollateClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link DatabricksParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link DatabricksParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rowField}.
	 * @param ctx the parse tree
	 */
	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#commentSpec}.
	 * @param ctx the parse tree
	 */
	fn visit_commentSpec(&mut self, ctx: &CommentSpecContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#whenClause}.
	 * @param ctx the parse tree
	 */
	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#filter}.
	 * @param ctx the parse tree
	 */
	fn visit_filter(&mut self, ctx: &FilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#over}.
	 * @param ctx the parse tree
	 */
	fn visit_over(&mut self, ctx: &OverContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#frameExtent}.
	 * @param ctx the parse tree
	 */
	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#privilege}.
	 * @param ctx the parse tree
	 */
	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link DatabricksParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rangeType}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code strictIdentifierDefault}
	 * labeled alternative in {@link DatabricksParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code strictNonReservedIdentifier}
	 * labeled alternative in {@link DatabricksParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pathComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierList}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_exponentLiteral(&mut self, ctx: &ExponentLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_bigIntLiteral(&mut self, ctx: &BigIntLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_smallIntLiteral(&mut self, ctx: &SmallIntLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_tinyIntLiteral(&mut self, ctx: &TinyIntLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_bigDecimalLiteral(&mut self, ctx: &BigDecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentStruct}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentMap}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentArray}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentLambda}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentInteger}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentDefault}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#prestoShowFunctionRowField}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#prestoShowFunctionTypes}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) { self.visit_children(ctx) }

}

pub trait DatabricksVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= DatabricksParserContextType>{
	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#singleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneType}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_use(&mut self, ctx: &UseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTableAsSelect}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTableLike(&mut self, ctx: &CreateTableLikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dmlStatement}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dmlStatement(&mut self, ctx: &DmlStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_merge(&mut self, ctx: &MergeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_set(&mut self, ctx: &SetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_drop(&mut self, ctx: &DropContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_delete(&mut self, ctx: &DeleteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_comment(&mut self, ctx: &CommentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_grant(&mut self, ctx: &GrantContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_explain(&mut self, ctx: &ExplainContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_show(&mut self, ctx: &ShowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_reset(&mut self, ctx: &ResetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_commit(&mut self, ctx: &CommitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_update(&mut self, ctx: &UpdateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createFoo}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code alter}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_alter(&mut self, ctx: &AlterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code optimize}
	 * labeled alternative in {@link DatabricksParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_optimize(&mut self, ctx: &OptimizeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableElements}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierReference}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierReference(&mut self, ctx: &IdentifierReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierCommentList(&mut self, ctx: &IdentifierCommentListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierComment}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierComment(&mut self, ctx: &IdentifierCommentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#schemaBinding}.
	 * @param ctx the parse tree
	 */
		fn visit_schemaBinding(&mut self, ctx: &SchemaBindingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#createTableClauses}.
	 * @param ctx the parse tree
	 */
		fn visit_createTableClauses(&mut self, ctx: &CreateTableClausesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableProvider}.
	 * @param ctx the parse tree
	 */
		fn visit_tableProvider(&mut self, ctx: &TableProviderContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionTransform}
	 * labeled alternative in {@link DatabricksParser#partitionField}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionTransform(&mut self, ctx: &PartitionTransformContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionColumn}
	 * labeled alternative in {@link DatabricksParser#partitionField}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identityTransform}
	 * labeled alternative in {@link DatabricksParser#transform}.
	 * @param ctx the parse tree
	 */
		fn visit_identityTransform(&mut self, ctx: &IdentityTransformContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code applyTransform}
	 * labeled alternative in {@link DatabricksParser#transform}.
	 * @param ctx the parse tree
	 */
		fn visit_applyTransform(&mut self, ctx: &ApplyTransformContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#transformArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_transformArgument(&mut self, ctx: &TransformArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colType}.
	 * @param ctx the parse tree
	 */
		fn visit_colType(&mut self, ctx: &ColTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#skewSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_skewSpec(&mut self, ctx: &SkewSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#clusterBySpec}.
	 * @param ctx the parse tree
	 */
		fn visit_clusterBySpec(&mut self, ctx: &ClusterBySpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_bucketSpec(&mut self, ctx: &BucketSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#constantList}.
	 * @param ctx the parse tree
	 */
		fn visit_constantList(&mut self, ctx: &ConstantListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedConstantList(&mut self, ctx: &NestedConstantListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link DatabricksParser#rowFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_rowFormatSerde(&mut self, ctx: &RowFormatSerdeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link DatabricksParser#rowFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_rowFormatDelimited(&mut self, ctx: &RowFormatDelimitedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link DatabricksParser#fileFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFileFormat(&mut self, ctx: &TableFileFormatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link DatabricksParser#fileFormat}.
	 * @param ctx the parse tree
	 */
		fn visit_genericFileFormat(&mut self, ctx: &GenericFileFormatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#storageHandler}.
	 * @param ctx the parse tree
	 */
		fn visit_storageHandler(&mut self, ctx: &StorageHandlerContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#locationSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#literalType}.
	 * @param ctx the parse tree
	 */
		fn visit_literalType(&mut self, ctx: &LiteralTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_singleInsertQuery(&mut self, ctx: &SingleInsertQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_multiInsertQuery(&mut self, ctx: &MultiInsertQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deleteFromTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_deleteFromTable(&mut self, ctx: &DeleteFromTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code updateTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_updateTable(&mut self, ctx: &UpdateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mergeIntoTable}
	 * labeled alternative in {@link DatabricksParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_mergeIntoTable(&mut self, ctx: &MergeIntoTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#ctes}.
	 * @param ctx the parse tree
	 */
		fn visit_ctes(&mut self, ctx: &CtesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
		fn visit_insertOverwriteTable(&mut self, ctx: &InsertOverwriteTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
		fn visit_insertIntoTable(&mut self, ctx: &InsertIntoTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertIntoReplaceWhere}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
		fn visit_insertIntoReplaceWhere(&mut self, ctx: &InsertIntoReplaceWhereContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
		fn visit_insertOverwriteHiveDir(&mut self, ctx: &InsertOverwriteHiveDirContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link DatabricksParser#insertInto}.
	 * @param ctx the parse tree
	 */
		fn visit_insertOverwriteDir(&mut self, ctx: &InsertOverwriteDirContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
		fn visit_multiInsertQueryBody(&mut self, ctx: &MultiInsertQueryBodyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_tableAlias(&mut self, ctx: &TableAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#whereClause}.
	 * @param ctx the parse tree
	 */
		fn visit_whereClause(&mut self, ctx: &WhereClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setClause}.
	 * @param ctx the parse tree
	 */
		fn visit_setClause(&mut self, ctx: &SetClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#matchedClause}.
	 * @param ctx the parse tree
	 */
		fn visit_matchedClause(&mut self, ctx: &MatchedClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedClause}.
	 * @param ctx the parse tree
	 */
		fn visit_notMatchedClause(&mut self, ctx: &NotMatchedClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedBySourceClause}.
	 * @param ctx the parse tree
	 */
		fn visit_notMatchedBySourceClause(&mut self, ctx: &NotMatchedBySourceClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#optionsClause}.
	 * @param ctx the parse tree
	 */
		fn visit_optionsClause(&mut self, ctx: &OptionsClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionSpec(&mut self, ctx: &PartitionSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#lateralView}.
	 * @param ctx the parse tree
	 */
		fn visit_lateralView(&mut self, ctx: &LateralViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
		fn visit_fromStatementBody(&mut self, ctx: &FromStatementBodyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
		fn visit_queryOrganization(&mut self, ctx: &QueryOrganizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#assignmentList}.
	 * @param ctx the parse tree
	 */
		fn visit_assignmentList(&mut self, ctx: &AssignmentListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#assignment}.
	 * @param ctx the parse tree
	 */
		fn visit_assignment(&mut self, ctx: &AssignmentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#matchedAction}.
	 * @param ctx the parse tree
	 */
		fn visit_matchedAction(&mut self, ctx: &MatchedActionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedAction}.
	 * @param ctx the parse tree
	 */
		fn visit_notMatchedAction(&mut self, ctx: &NotMatchedActionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#notMatchedBySourceAction}.
	 * @param ctx the parse tree
	 */
		fn visit_notMatchedBySourceAction(&mut self, ctx: &NotMatchedBySourceActionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#partitionVal}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionVal(&mut self, ctx: &PartitionValContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#transformClause}.
	 * @param ctx the parse tree
	 */
		fn visit_transformClause(&mut self, ctx: &TransformClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectClause}.
	 * @param ctx the parse tree
	 */
		fn visit_selectClause(&mut self, ctx: &SelectClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#havingClause}.
	 * @param ctx the parse tree
	 */
		fn visit_havingClause(&mut self, ctx: &HavingClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
		fn visit_multipartIdentifierList(&mut self, ctx: &MultipartIdentifierListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#expressionSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_expressionSeq(&mut self, ctx: &ExpressionSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colTypeList}.
	 * @param ctx the parse tree
	 */
		fn visit_colTypeList(&mut self, ctx: &ColTypeListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#hint}.
	 * @param ctx the parse tree
	 */
		fn visit_hint(&mut self, ctx: &HintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#hintStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_hintStatement(&mut self, ctx: &HintStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#query}.
	 * @param ctx the parse tree
	 */
		fn visit_query(&mut self, ctx: &QueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#with}.
	 * @param ctx the parse tree
	 */
		fn visit_with(&mut self, ctx: &WithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableElement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
		fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnDefinitionForView}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fieldDefinitions}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldDefinitions(&mut self, ctx: &FieldDefinitionsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnName}.
	 * @param ctx the parse tree
	 */
		fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#colDefinitionOption}.
	 * @param ctx the parse tree
	 */
		fn visit_colDefinitionOption(&mut self, ctx: &ColDefinitionOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#generationExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_generationExpression(&mut self, ctx: &GenerationExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#defaultExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultExpression(&mut self, ctx: &DefaultExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnOption}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link DatabricksParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#properties}.
	 * @param ctx the parse tree
	 */
		fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link DatabricksParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link DatabricksParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#propertyKey}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link DatabricksParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryLimit}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDatabricks}
	 * labeled alternative in {@link DatabricksParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimitTargetDatabricks(&mut self, ctx: &QueryLimitTargetDatabricksContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowClause}.
	 * @param ctx the parse tree
	 */
		fn visit_windowClause(&mut self, ctx: &WindowClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperation}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#inlineTable}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_table(&mut self, ctx: &TableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link DatabricksParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sortItem}.
	 * @param ctx the parse tree
	 */
		fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#querySpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
		fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByAll}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByWith}
	 * labeled alternative in {@link DatabricksParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByWith(&mut self, ctx: &GroupByWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpElementAnalytics}
	 * labeled alternative in {@link DatabricksParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_grpElementAnalytics(&mut self, ctx: &GrpElementAnalyticsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpElementExpression}
	 * labeled alternative in {@link DatabricksParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_grpElementExpression(&mut self, ctx: &GrpElementExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpAnalyticsSugar}
	 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
		fn visit_grpAnalyticsSugar(&mut self, ctx: &GrpAnalyticsSugarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpAnalyticsSets}
	 * labeled alternative in {@link DatabricksParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
		fn visit_grpAnalyticsSets(&mut self, ctx: &GrpAnalyticsSetsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpSetsElementAnalytics}
	 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
	 * @param ctx the parse tree
	 */
		fn visit_grpSetsElementAnalytics(&mut self, ctx: &GrpSetsElementAnalyticsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grpSetsElementSet}
	 * labeled alternative in {@link DatabricksParser#grpSetsElement}.
	 * @param ctx the parse tree
	 */
		fn visit_grpSetsElementSet(&mut self, ctx: &GrpSetsElementSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#groupingSet}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#orderBy}.
	 * @param ctx the parse tree
	 */
		fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#namedQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link DatabricksParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link DatabricksParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code structItemSingle}
	 * labeled alternative in {@link DatabricksParser#structItem}.
	 * @param ctx the parse tree
	 */
		fn visit_structItemSingle(&mut self, ctx: &StructItemSingleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code structItemMulti}
	 * labeled alternative in {@link DatabricksParser#structItem}.
	 * @param ctx the parse tree
	 */
		fn visit_structItemMulti(&mut self, ctx: &StructItemMultiContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiSelect}.
	 * @param ctx the parse tree
	 */
		fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#selectStar}.
	 * @param ctx the parse tree
	 */
		fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#relation}.
	 * @param ctx the parse tree
	 */
		fn visit_relation(&mut self, ctx: &RelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#joinType}.
	 * @param ctx the parse tree
	 */
		fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sample}.
	 * @param ctx the parse tree
	 */
		fn visit_sample(&mut self, ctx: &SampleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleByPercentile(&mut self, ctx: &SampleByPercentileContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleByRows(&mut self, ctx: &SampleByRowsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleByBucket(&mut self, ctx: &SampleByBucketContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link DatabricksParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleByBytes(&mut self, ctx: &SampleByBytesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#lateralViewRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_lateralViewRelation(&mut self, ctx: &LateralViewRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lateralViewRelationTargetDefault}
	 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_lateralViewRelationTargetDefault(&mut self, ctx: &LateralViewRelationTargetDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lateralViewRelationTargetIncremental}
	 * labeled alternative in {@link DatabricksParser#lateralViewRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_lateralViewRelationTargetIncremental(&mut self, ctx: &LateralViewRelationTargetIncrementalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#extensibleRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_extensibleRelation(&mut self, ctx: &ExtensibleRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code extensibleRelationTargetIncremental}
	 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_extensibleRelationTargetIncremental(&mut self, ctx: &ExtensibleRelationTargetIncrementalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code extensibleRelationTargetDefault}
	 * labeled alternative in {@link DatabricksParser#extensibleRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_extensibleRelationTargetDefault(&mut self, ctx: &ExtensibleRelationTargetDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relationExtensionJoin}
	 * labeled alternative in {@link DatabricksParser#relationExtension}.
	 * @param ctx the parse tree
	 */
		fn visit_relationExtensionJoin(&mut self, ctx: &RelationExtensionJoinContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relationExtensionPivot}
	 * labeled alternative in {@link DatabricksParser#relationExtension}.
	 * @param ctx the parse tree
	 */
		fn visit_relationExtensionPivot(&mut self, ctx: &RelationExtensionPivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code joinRelationDefault}
	 * labeled alternative in {@link DatabricksParser#joinRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_joinRelationDefault(&mut self, ctx: &JoinRelationDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code joinRelationNatural}
	 * labeled alternative in {@link DatabricksParser#joinRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_joinRelationNatural(&mut self, ctx: &JoinRelationNaturalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotAggregates}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotFrom}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntoNamedExpression}
	 * labeled alternative in {@link DatabricksParser#pivotInto}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pivotAsAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#singleColumnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnsToUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#multiColumnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#valueColumnSet}.
	 * @param ctx the parse tree
	 */
		fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnSetsToUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleColumnUnpivotDefault}
	 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multiColumnUnpivotDefault}
	 * labeled alternative in {@link DatabricksParser#columnUnpivot}.
	 * @param ctx the parse tree
	 */
		fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivotIntosDefault}
	 * labeled alternative in {@link DatabricksParser#pivotIntos}.
	 * @param ctx the parse tree
	 */
		fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code pivot}
	 * labeled alternative in {@link DatabricksParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_pivot(&mut self, ctx: &PivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unpivot}
	 * labeled alternative in {@link DatabricksParser#pivotOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code streamTableTarget}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_streamTableTarget(&mut self, ctx: &StreamTableTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code sampledRelationDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelationDefault(&mut self, ctx: &SampledRelationDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionTableDefault}
	 * labeled alternative in {@link DatabricksParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_functionTableDefault(&mut self, ctx: &FunctionTableDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#temporalClause}.
	 * @param ctx the parse tree
	 */
		fn visit_temporalClause(&mut self, ctx: &TemporalClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#version}.
	 * @param ctx the parse tree
	 */
		fn visit_version(&mut self, ctx: &VersionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#columnAliases}.
	 * @param ctx the parse tree
	 */
		fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link DatabricksParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link DatabricksParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#tableArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link DatabricksParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultBooleanExpression}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_or(&mut self, ctx: &OrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link DatabricksParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_and(&mut self, ctx: &AndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
		fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link DatabricksParser#comparisonPredicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nonComparisonExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_between(&mut self, ctx: &BetweenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inList(&mut self, ctx: &InListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code regexp}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_regexp(&mut self, ctx: &RegexpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedLike}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_like(&mut self, ctx: &LikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truePredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code falsePredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link DatabricksParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link DatabricksParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code structConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code anyValue}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_anyValue(&mut self, ctx: &AnyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decode}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_decode(&mut self, ctx: &DecodeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_substring(&mut self, ctx: &SubstringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileContFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_cast(&mut self, ctx: &CastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code namedStruct}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_namedStruct(&mut self, ctx: &NamedStructContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_trim(&mut self, ctx: &TrimContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_array(&mut self, ctx: &ArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tryCastOperator}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_tryCastOperator(&mut self, ctx: &TryCastOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arraysZip}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arraysZip(&mut self, ctx: &ArraysZipContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code castOperator}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code currentLike}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_currentLike(&mut self, ctx: &CurrentLikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code last}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_last(&mut self, ctx: &LastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code overlay}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_overlay(&mut self, ctx: &OverlayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_collate(&mut self, ctx: &CollateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonExtract}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonExtract(&mut self, ctx: &JsonExtractContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_extract(&mut self, ctx: &ExtractContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_measure(&mut self, ctx: &MeasureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_variable(&mut self, ctx: &VariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_exists(&mut self, ctx: &ExistsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fromJson}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_fromJson(&mut self, ctx: &FromJsonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileDiscFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_position(&mut self, ctx: &PositionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code mapFromEntries}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_mapFromEntries(&mut self, ctx: &MapFromEntriesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code modeFunction}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_modeFunction(&mut self, ctx: &ModeFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code first}
	 * labeled alternative in {@link DatabricksParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_first(&mut self, ctx: &FirstContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code namedArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multiArgument}
	 * labeled alternative in {@link DatabricksParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
		fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code posParameterLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_posParameterLiteral(&mut self, ctx: &PosParameterLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code namedParameterLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_namedParameterLiteral(&mut self, ctx: &NamedParameterLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringConcatination}
	 * labeled alternative in {@link DatabricksParser#constant}.
	 * @param ctx the parse tree
	 */
		fn visit_stringConcatination(&mut self, ctx: &StringConcatinationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPath}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonPath(&mut self, ctx: &JsonPathContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPathElement1}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonPathElement1(&mut self, ctx: &JsonPathElement1Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#jsonPathElement2}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonPathElement2(&mut self, ctx: &JsonPathElement2Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#functionName}.
	 * @param ctx the parse tree
	 */
		fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#field}.
	 * @param ctx the parse tree
	 */
		fn visit_field(&mut self, ctx: &FieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
		fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link DatabricksParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doubleQuotedStringLiteral}
	 * labeled alternative in {@link DatabricksParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleQuotedStringLiteral(&mut self, ctx: &DoubleQuotedStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
		fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#booleanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneInterval}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneInterval(&mut self, ctx: &StandaloneIntervalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#interval}.
	 * @param ctx the parse tree
	 */
		fn visit_interval(&mut self, ctx: &IntervalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalValue}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalValue(&mut self, ctx: &IntervalValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalValueField}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalValueField(&mut self, ctx: &IntervalValueFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#intervalTypeField}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalTypeField(&mut self, ctx: &IntervalTypeFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#collateClause}.
	 * @param ctx the parse tree
	 */
		fn visit_collateClause(&mut self, ctx: &CollateClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link DatabricksParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link DatabricksParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link DatabricksParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rowField}.
	 * @param ctx the parse tree
	 */
		fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#commentSpec}.
	 * @param ctx the parse tree
	 */
		fn visit_commentSpec(&mut self, ctx: &CommentSpecContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#typeParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#whenClause}.
	 * @param ctx the parse tree
	 */
		fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#filter}.
	 * @param ctx the parse tree
	 */
		fn visit_filter(&mut self, ctx: &FilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#over}.
	 * @param ctx the parse tree
	 */
		fn visit_over(&mut self, ctx: &OverContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#windowFrame}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#frameExtent}.
	 * @param ctx the parse tree
	 */
		fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link DatabricksParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#privilege}.
	 * @param ctx the parse tree
	 */
		fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link DatabricksParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#rangeType}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link DatabricksParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code strictIdentifierDefault}
	 * labeled alternative in {@link DatabricksParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code strictNonReservedIdentifier}
	 * labeled alternative in {@link DatabricksParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code backQuotedIdentifier}
	 * labeled alternative in {@link DatabricksParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#pathComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierList}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_exponentLiteral(&mut self, ctx: &ExponentLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_bigIntLiteral(&mut self, ctx: &BigIntLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_smallIntLiteral(&mut self, ctx: &SmallIntLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_tinyIntLiteral(&mut self, ctx: &TinyIntLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link DatabricksParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_bigDecimalLiteral(&mut self, ctx: &BigDecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentStruct}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentMap}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentArray}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentLambda}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentInteger}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentDefault}
	 * labeled alternative in {@link DatabricksParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#prestoShowFunctionRowField}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#prestoShowFunctionTypes}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link DatabricksParser#nonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> DatabricksVisitor<'input> for T
where
	T: DatabricksVisitorCompat<'input>
{
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multipleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_singleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_standaloneExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_standaloneQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_standaloneType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_statementDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_use(&mut self, ctx: &UseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_use(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dropSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_renameSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setSchemaAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dropTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dropView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createTableAsSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTableLike(&mut self, ctx: &CreateTableLikeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createTableLike(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dmlStatement(&mut self, ctx: &DmlStatementContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dmlStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_showColumns(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_merge(&mut self, ctx: &MergeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_merge(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_set(&mut self, ctx: &SetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_set(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_drop(&mut self, ctx: &DropContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_drop(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delete(&mut self, ctx: &DeleteContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_delete(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_truncateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comment(&mut self, ctx: &CommentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_comment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_renameTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_addColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_renameColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dropColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setColumnType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setTableAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setTableProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableExecute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_analyze(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_renameView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setViewAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createRole(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grant(&mut self, ctx: &GrantContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grant(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_revoke(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explain(&mut self, ctx: &ExplainContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_explain(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_show(&mut self, ctx: &ShowContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_show(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_reset(&mut self, ctx: &ResetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_reset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commit(&mut self, ctx: &CommitContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_commit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rollback(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_execute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_update(&mut self, ctx: &UpdateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_update(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFoo(&mut self, ctx: &CreateFooContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createFoo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_alter(&mut self, ctx: &AlterContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_alter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_optimize(&mut self, ctx: &OptimizeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_optimize(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableElements(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierReference(&mut self, ctx: &IdentifierReferenceContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierCommentList(&mut self, ctx: &IdentifierCommentListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierCommentList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierComment(&mut self, ctx: &IdentifierCommentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierComment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_schemaBinding(&mut self, ctx: &SchemaBindingContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_schemaBinding(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTableClauses(&mut self, ctx: &CreateTableClausesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createTableClauses(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableProvider(&mut self, ctx: &TableProviderContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableProvider(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionTransform(&mut self, ctx: &PartitionTransformContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_partitionTransform(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionColumn(&mut self, ctx: &PartitionColumnContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_partitionColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identityTransform(&mut self, ctx: &IdentityTransformContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identityTransform(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_applyTransform(&mut self, ctx: &ApplyTransformContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_applyTransform(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transformArgument(&mut self, ctx: &TransformArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_transformArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_colType(&mut self, ctx: &ColTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_colType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_skewSpec(&mut self, ctx: &SkewSpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_skewSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_clusterBySpec(&mut self, ctx: &ClusterBySpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_clusterBySpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bucketSpec(&mut self, ctx: &BucketSpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_bucketSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_constantList(&mut self, ctx: &ConstantListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_constantList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedConstantList(&mut self, ctx: &NestedConstantListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nestedConstantList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowFormatSerde(&mut self, ctx: &RowFormatSerdeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowFormatSerde(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowFormatDelimited(&mut self, ctx: &RowFormatDelimitedContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowFormatDelimited(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFileFormat(&mut self, ctx: &CreateFileFormatContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_createFileFormat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFileFormat(&mut self, ctx: &TableFileFormatContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableFileFormat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_genericFileFormat(&mut self, ctx: &GenericFileFormatContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_genericFileFormat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_storageHandler(&mut self, ctx: &StorageHandlerContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_storageHandler(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_locationSpec(&mut self, ctx: &LocationSpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_locationSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalType(&mut self, ctx: &LiteralTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_literalType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleInsertQuery(&mut self, ctx: &SingleInsertQueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_singleInsertQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiInsertQuery(&mut self, ctx: &MultiInsertQueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiInsertQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deleteFromTable(&mut self, ctx: &DeleteFromTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_deleteFromTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_updateTable(&mut self, ctx: &UpdateTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_updateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mergeIntoTable(&mut self, ctx: &MergeIntoTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_mergeIntoTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_ctes(&mut self, ctx: &CtesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_ctes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertOverwriteTable(&mut self, ctx: &InsertOverwriteTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_insertOverwriteTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertIntoTable(&mut self, ctx: &InsertIntoTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_insertIntoTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertIntoReplaceWhere(&mut self, ctx: &InsertIntoReplaceWhereContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_insertIntoReplaceWhere(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertOverwriteHiveDir(&mut self, ctx: &InsertOverwriteHiveDirContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_insertOverwriteHiveDir(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertOverwriteDir(&mut self, ctx: &InsertOverwriteDirContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_insertOverwriteDir(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiInsertQueryBody(&mut self, ctx: &MultiInsertQueryBodyContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiInsertQueryBody(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableAlias(&mut self, ctx: &TableAliasContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whereClause(&mut self, ctx: &WhereClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_whereClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setClause(&mut self, ctx: &SetClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matchedClause(&mut self, ctx: &MatchedClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_matchedClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notMatchedClause(&mut self, ctx: &NotMatchedClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_notMatchedClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notMatchedBySourceClause(&mut self, ctx: &NotMatchedBySourceClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_notMatchedBySourceClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_optionsClause(&mut self, ctx: &OptionsClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_optionsClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionSpec(&mut self, ctx: &PartitionSpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_partitionSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateralView(&mut self, ctx: &LateralViewContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lateralView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fromStatementBody(&mut self, ctx: &FromStatementBodyContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_fromStatementBody(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryOrganization(&mut self, ctx: &QueryOrganizationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryOrganization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_assignmentList(&mut self, ctx: &AssignmentListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_assignmentList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_assignment(&mut self, ctx: &AssignmentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_assignment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matchedAction(&mut self, ctx: &MatchedActionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_matchedAction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notMatchedAction(&mut self, ctx: &NotMatchedActionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_notMatchedAction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notMatchedBySourceAction(&mut self, ctx: &NotMatchedBySourceActionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_notMatchedBySourceAction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionVal(&mut self, ctx: &PartitionValContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_partitionVal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedExpressionSeq(&mut self, ctx: &NamedExpressionSeqContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedExpressionSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedExpression(&mut self, ctx: &NamedExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotNullClause(&mut self, ctx: &UnpivotNullClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unpivotNullClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transformClause(&mut self, ctx: &TransformClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_transformClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectClause(&mut self, ctx: &SelectClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_selectClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_havingClause(&mut self, ctx: &HavingClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_havingClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multipartIdentifierList(&mut self, ctx: &MultipartIdentifierListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multipartIdentifierList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expressionSeq(&mut self, ctx: &ExpressionSeqContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_expressionSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_colTypeList(&mut self, ctx: &ColTypeListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_colTypeList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_hint(&mut self, ctx: &HintContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_hint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_hintStatement(&mut self, ctx: &HintStatementContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_hintStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_query(&mut self, ctx: &QueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_query(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_with(&mut self, ctx: &WithContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_with(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableConstraint(&mut self, ctx: &TableConstraintContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableConstraint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinitionForView(&mut self, ctx: &ColumnDefinitionForViewContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnDefinitionForView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldDefinitions(&mut self, ctx: &FieldDefinitionsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_fieldDefinitions(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_fieldDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnNameComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnSchemaWithMetadata(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_colDefinitionOption(&mut self, ctx: &ColDefinitionOptionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_colDefinitionOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_generationExpression(&mut self, ctx: &GenerationExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_generationExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultExpression(&mut self, ctx: &DefaultExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_defaultExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnOptionList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnSchemaSimpleType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_properties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_propertyAssignments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nestedProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_defaultProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_propertyKey(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_defaultPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_expressionPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryNoWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryLimit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimitTargetDatabricks(&mut self, ctx: &QueryLimitTargetDatabricksContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryLimitTargetDatabricks(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowClause(&mut self, ctx: &WindowClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_windowClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_limitRowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setOperation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setOperationIntersect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setIntersectOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_setQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_inlineTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryPrimaryDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_table(&mut self, ctx: &TableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_table(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_inlineTableDefault1(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_subquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sortItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_querySpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_querySelectItems(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_aggregationClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByAll(&mut self, ctx: &GroupByAllContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_groupByAll(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_groupByDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByWith(&mut self, ctx: &GroupByWithContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_groupByWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpElementAnalytics(&mut self, ctx: &GrpElementAnalyticsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpElementAnalytics(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpElementExpression(&mut self, ctx: &GrpElementExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpElementExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpAnalyticsSugar(&mut self, ctx: &GrpAnalyticsSugarContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpAnalyticsSugar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpAnalyticsSets(&mut self, ctx: &GrpAnalyticsSetsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpAnalyticsSets(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpSetsElementAnalytics(&mut self, ctx: &GrpSetsElementAnalyticsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpSetsElementAnalytics(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grpSetsElementSet(&mut self, ctx: &GrpSetsElementSetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_grpSetsElementSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_groupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_windowDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_windowSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_windowSpecificationPartitionBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_orderBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_selectItemAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_selectSingle(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_selectMulti(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structItemSingle(&mut self, ctx: &StructItemSingleContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_structItemSingle(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structItemMulti(&mut self, ctx: &StructItemMultiContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_structItemMulti(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_selectStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relation(&mut self, ctx: &RelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_relation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_joinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_joinCriteria(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampledRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampledRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sample(&mut self, ctx: &SampleContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sample(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampleOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleByPercentile(&mut self, ctx: &SampleByPercentileContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampleByPercentile(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleByRows(&mut self, ctx: &SampleByRowsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampleByRows(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleByBucket(&mut self, ctx: &SampleByBucketContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampleByBucket(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleByBytes(&mut self, ctx: &SampleByBytesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampleByBytes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_trimsSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_variableDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotedRelationTarget(&mut self, ctx: &PivotedRelationTargetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotedRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateralViewRelation(&mut self, ctx: &LateralViewRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lateralViewRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateralViewRelationTargetDefault(&mut self, ctx: &LateralViewRelationTargetDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lateralViewRelationTargetDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateralViewRelationTargetIncremental(&mut self, ctx: &LateralViewRelationTargetIncrementalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lateralViewRelationTargetIncremental(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extensibleRelation(&mut self, ctx: &ExtensibleRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_extensibleRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extensibleRelationTargetIncremental(&mut self, ctx: &ExtensibleRelationTargetIncrementalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_extensibleRelationTargetIncremental(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extensibleRelationTargetDefault(&mut self, ctx: &ExtensibleRelationTargetDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_extensibleRelationTargetDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relationExtensionJoin(&mut self, ctx: &RelationExtensionJoinContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_relationExtensionJoin(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relationExtensionPivot(&mut self, ctx: &RelationExtensionPivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_relationExtensionPivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinRelationDefault(&mut self, ctx: &JoinRelationDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_joinRelationDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinRelationNatural(&mut self, ctx: &JoinRelationNaturalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_joinRelationNatural(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotedRelation(&mut self, ctx: &PivotedRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAggregates(&mut self, ctx: &PivotAggregatesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotAggregates(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotFrom(&mut self, ctx: &PivotFromContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntoNamedExpression(&mut self, ctx: &PivotIntoNamedExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotIntoNamedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotAsAlias(&mut self, ctx: &PivotAsAliasContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotAsAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivot(&mut self, ctx: &SingleColumnUnpivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_singleColumnUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnsToUnpivot(&mut self, ctx: &ColumnsToUnpivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnsToUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotAlias(&mut self, ctx: &UnpivotAliasContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unpivotAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiColumnUnpivot(&mut self, ctx: &MultiColumnUnpivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiColumnUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueColumnSet(&mut self, ctx: &ValueColumnSetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_valueColumnSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivotColumnSet(&mut self, ctx: &UnpivotColumnSetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unpivotColumnSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSetsToUnpivot(&mut self, ctx: &ColumnSetsToUnpivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnSetsToUnpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleColumnUnpivotDefault(&mut self, ctx: &SingleColumnUnpivotDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_singleColumnUnpivotDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiColumnUnpivotDefault(&mut self, ctx: &MultiColumnUnpivotDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiColumnUnpivotDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivotIntosDefault(&mut self, ctx: &PivotIntosDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivotIntosDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pivot(&mut self, ctx: &PivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpivot(&mut self, ctx: &UnpivotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unpivot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_streamTableTarget(&mut self, ctx: &StreamTableTargetContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_streamTableTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelationDefault(&mut self, ctx: &SampledRelationDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_sampledRelationDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTableDefault(&mut self, ctx: &InlineTableDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_inlineTableDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionTableDefault(&mut self, ctx: &FunctionTableDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionTableDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_temporalClause(&mut self, ctx: &TemporalClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_temporalClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_version(&mut self, ctx: &VersionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_version(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_aliasedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnAliases(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_subqueryRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_parenthesizedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_defaultTableFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableFunctionArgumentName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableFunctionArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableArgumentTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tableArgumentQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_expression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultBooleanExpression(&mut self, ctx: &DefaultBooleanExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_defaultBooleanExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_logicalNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_or(&mut self, ctx: &OrContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_or(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_predicated(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_and(&mut self, ctx: &AndContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_and(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_comparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_quantifiedComparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonComparisonExpression(&mut self, ctx: &NonComparisonExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nonComparisonExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_between(&mut self, ctx: &BetweenContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_between(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inList(&mut self, ctx: &InListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_inList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_inSubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexp(&mut self, ctx: &RegexpContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_regexp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedLike(&mut self, ctx: &QuantifiedLikeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_quantifiedLike(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_like(&mut self, ctx: &LikeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_like(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nullPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_distinctFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truePredicate(&mut self, ctx: &TruePredicateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_truePredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_falsePredicate(&mut self, ctx: &FalsePredicateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_falsePredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unknownPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_valueExpressionDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_concatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_arithmeticBinary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_arithmeticUnary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_atTimeZone(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_dereference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_structConstructor(&mut self, ctx: &StructConstructorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_structConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_anyValue(&mut self, ctx: &AnyValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_anyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decode(&mut self, ctx: &DecodeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_decode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_substring(&mut self, ctx: &SubstringContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_substring(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_countStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileContFunction(&mut self, ctx: &PercentileContFunctionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_percentileContFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cast(&mut self, ctx: &CastContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_cast(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedStruct(&mut self, ctx: &NamedStructContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedStruct(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_parenthesizedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trim(&mut self, ctx: &TrimContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_trim(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_array(&mut self, ctx: &ArrayContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_array(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tryCastOperator(&mut self, ctx: &TryCastOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tryCastOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arraysZip(&mut self, ctx: &ArraysZipContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_arraysZip(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_castOperator(&mut self, ctx: &CastOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_castOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_simpleCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentLike(&mut self, ctx: &CurrentLikeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_currentLike(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_columnReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_last(&mut self, ctx: &LastContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_last(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_overlay(&mut self, ctx: &OverlayContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_overlay(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_subscript(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_subqueryExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_collate(&mut self, ctx: &CollateContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_collate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonExtract(&mut self, ctx: &JsonExtractContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_jsonExtract(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_constantDefault(&mut self, ctx: &ConstantDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_constantDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extract(&mut self, ctx: &ExtractContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_extract(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measure(&mut self, ctx: &MeasureContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_measure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_arrayConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variable(&mut self, ctx: &VariableContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_variable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exists(&mut self, ctx: &ExistsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_exists(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fromJson(&mut self, ctx: &FromJsonContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_fromJson(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileDiscFunction(&mut self, ctx: &PercentileDiscFunctionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_percentileDiscFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_position(&mut self, ctx: &PositionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_position(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_listagg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_searchedCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mapFromEntries(&mut self, ctx: &MapFromEntriesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_mapFromEntries(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_modeFunction(&mut self, ctx: &ModeFunctionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_modeFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_first(&mut self, ctx: &FirstContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_first(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionCallHead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionCallTail(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_positionalArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedArgument(&mut self, ctx: &NamedArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiArgument(&mut self, ctx: &MultiArgumentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_multiArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionExtraArguments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nullLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_posParameterLiteral(&mut self, ctx: &PosParameterLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_posParameterLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedParameterLiteral(&mut self, ctx: &NamedParameterLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_namedParameterLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_intervalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_typeConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_numericLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_booleanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringConcatination(&mut self, ctx: &StringConcatinationContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_stringConcatination(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonPath(&mut self, ctx: &JsonPathContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_jsonPath(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonPathElement1(&mut self, ctx: &JsonPathElement1Context<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_jsonPathElement1(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonPathElement2(&mut self, ctx: &JsonPathElement2Context<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_jsonPathElement2(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_field(&mut self, ctx: &FieldContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_field(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nullTreatment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_basicStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleQuotedStringLiteral(&mut self, ctx: &DoubleQuotedStringLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_doubleQuotedStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_timeZoneSpecifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_comparisonOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_comparisonQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_booleanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneInterval(&mut self, ctx: &StandaloneIntervalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_standaloneInterval(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_interval(&mut self, ctx: &IntervalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_interval(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalValue(&mut self, ctx: &IntervalValueContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_intervalValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalValueField(&mut self, ctx: &IntervalValueFieldContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_intervalValueField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalTypeField(&mut self, ctx: &IntervalTypeFieldContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_intervalTypeField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_typeIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_collateClause(&mut self, ctx: &CollateClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_collateClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_typeNotNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_typeNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_functionSignatureGenericType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_intervalType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_legacyMapType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_legacyArrayType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_lambdaType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_primitiveType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commentSpec(&mut self, ctx: &CommentSpecContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_commentSpec(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_typeParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_whenClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_filter(&mut self, ctx: &FilterContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_filter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_over(&mut self, ctx: &OverContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_over(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_windowFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_frameExtent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unboundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_currentRowBound(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_boundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_privilege(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_qualifiedNameDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_queryPeriod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rangeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unspecifiedPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_userPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_rolePrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictIdentifierDefault(&mut self, ctx: &StrictIdentifierDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_strictIdentifierDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictNonReservedIdentifier(&mut self, ctx: &StrictNonReservedIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_strictNonReservedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_unquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_quotedIdentifierDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_backQuotedIdentifier(&mut self, ctx: &BackQuotedIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_backQuotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_quotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_pathComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_standaloneIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_identifierSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_decimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_doubleLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_integerLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exponentLiteral(&mut self, ctx: &ExponentLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_exponentLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigIntLiteral(&mut self, ctx: &BigIntLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_bigIntLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_smallIntLiteral(&mut self, ctx: &SmallIntLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_smallIntLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tinyIntLiteral(&mut self, ctx: &TinyIntLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_tinyIntLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_floatLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bigDecimalLiteral(&mut self, ctx: &BigDecimalLiteralContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_bigDecimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentStruct(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentMap(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentArray(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentLambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentInteger(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoFunctionArgumentDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoShowFunctionRowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_prestoShowFunctionTypes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_strictNonReserved(&mut self, ctx: &StrictNonReservedContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_strictNonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>){
		let result = <Self as DatabricksVisitorCompat>::visit_nonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}