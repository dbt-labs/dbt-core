#![allow(nonstandard_style)]
// Generated from Trino.g4 by ANTLR 4.8
use antlr_rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::trinoparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link TrinoParser}.
 */
pub trait TrinoVisitor<'input>: ParseTreeVisitor<'input,TrinoParserContextType>{
	/**
	 * Visit a parse tree produced by {@link TrinoParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneType}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_use(&mut self, ctx: &UseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTableAsSelect}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRecursiveTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code insertInto}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_merge(&mut self, ctx: &MergeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_set(&mut self, ctx: &SetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_drop(&mut self, ctx: &DropContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_delete(&mut self, ctx: &DeleteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_comment(&mut self, ctx: &CommentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_call(&mut self, ctx: &CallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_grant(&mut self, ctx: &GrantContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deny(&mut self, ctx: &DenyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_explain(&mut self, ctx: &ExplainContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_show(&mut self, ctx: &ShowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_reset(&mut self, ctx: &ResetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_commit(&mut self, ctx: &CommitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
	fn visit_update(&mut self, ctx: &UpdateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableElements}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#query}.
	 * @param ctx the parse tree
	 */
	fn visit_query(&mut self, ctx: &QueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#with}.
	 * @param ctx the parse tree
	 */
	fn visit_with(&mut self, ctx: &WithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableElement}.
	 * @param ctx the parse tree
	 */
	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnName}.
	 * @param ctx the parse tree
	 */
	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnOption}.
	 * @param ctx the parse tree
	 */
	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link TrinoParser#columnSchema}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#properties}.
	 * @param ctx the parse tree
	 */
	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link TrinoParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link TrinoParser#property}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryLimit}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDefault}
	 * labeled alternative in {@link TrinoParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowOrRows}.
	 * @param ctx the parse tree
	 */
	fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowCount}.
	 * @param ctx the parse tree
	 */
	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperation}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_table(&mut self, ctx: &TableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sortItem}.
	 * @param ctx the parse tree
	 */
	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link TrinoParser#groupBy}.
	 * @param ctx the parse tree
	 */
	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_rollup(&mut self, ctx: &RollupContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_cube(&mut self, ctx: &CubeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#orderBy}.
	 * @param ctx the parse tree
	 */
	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link TrinoParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link TrinoParser#selectItem}.
	 * @param ctx the parse tree
	 */
	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#multiSelect}.
	 * @param ctx the parse tree
	 */
	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#selectStar}.
	 * @param ctx the parse tree
	 */
	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#relation}.
	 * @param ctx the parse tree
	 */
	fn visit_relation(&mut self, ctx: &RelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link TrinoParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link TrinoParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#joinType}.
	 * @param ctx the parse tree
	 */
	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#patternRecognitionTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#patternRecognition}.
	 * @param ctx the parse tree
	 */
	fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#measureDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowsPerMatch}.
	 * @param ctx the parse tree
	 */
	fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#emptyMatchHandling}.
	 * @param ctx the parse tree
	 */
	fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#skipTo}.
	 * @param ctx the parse tree
	 */
	fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#subsetDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnAliases}.
	 * @param ctx the parse tree
	 */
	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_unnest(&mut self, ctx: &UnnestContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lateral}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_lateral(&mut self, ctx: &LateralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link TrinoParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#descriptorField}.
	 * @param ctx the parse tree
	 */
	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_or(&mut self, ctx: &OrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_and(&mut self, ctx: &AndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inList(&mut self, ctx: &InListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_like(&mut self, ctx: &LikeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code similarTo}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonValue}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonValue(&mut self, ctx: &JsonValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_substring(&mut self, ctx: &SubstringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_cast(&mut self, ctx: &CastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_trim(&mut self, ctx: &TrimContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_array(&mut self, ctx: &ArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonObject}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonObject(&mut self, ctx: &JsonObjectContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonArray}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonArray(&mut self, ctx: &JsonArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonExists}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonExists(&mut self, ctx: &JsonExistsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code jsonQuery}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonQuery(&mut self, ctx: &JsonQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_extract(&mut self, ctx: &ExtractContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_measure(&mut self, ctx: &MeasureContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_variable(&mut self, ctx: &VariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_exists(&mut self, ctx: &ExistsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_position(&mut self, ctx: &PositionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link TrinoParser#callArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionName}.
	 * @param ctx the parse tree
	 */
	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#field}.
	 * @param ctx the parse tree
	 */
	fn visit_field(&mut self, ctx: &FieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonPathInvocation}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonPathInvocation(&mut self, ctx: &JsonPathInvocationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonValueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonValueExpression(&mut self, ctx: &JsonValueExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonRepresentation}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonRepresentation(&mut self, ctx: &JsonRepresentationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonArgument(&mut self, ctx: &JsonArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonExistsErrorBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonExistsErrorBehavior(&mut self, ctx: &JsonExistsErrorBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonValueBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonValueBehavior(&mut self, ctx: &JsonValueBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonQueryWrapperBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonQueryWrapperBehavior(&mut self, ctx: &JsonQueryWrapperBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonQueryBehavior}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonQueryBehavior(&mut self, ctx: &JsonQueryBehaviorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonObjectMember}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonObjectMember(&mut self, ctx: &JsonObjectMemberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#processingMode}.
	 * @param ctx the parse tree
	 */
	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link TrinoParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link TrinoParser#string}.
	 * @param ctx the parse tree
	 */
	fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#interval}.
	 * @param ctx the parse tree
	 */
	fn visit_interval(&mut self, ctx: &IntervalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#intervalField}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#normalForm}.
	 * @param ctx the parse tree
	 */
	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link TrinoParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link TrinoParser#type_}.
	 * @param ctx the parse tree
	 */
	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rowType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code dateTimeType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doublePrecisionType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowField}.
	 * @param ctx the parse tree
	 */
	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#typeParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#whenClause}.
	 * @param ctx the parse tree
	 */
	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#filter}.
	 * @param ctx the parse tree
	 */
	fn visit_filter(&mut self, ctx: &FilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#over}.
	 * @param ctx the parse tree
	 */
	fn visit_over(&mut self, ctx: &OverContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#frameExtent}.
	 * @param ctx the parse tree
	 */
	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link TrinoParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link TrinoParser#transactionMode}.
	 * @param ctx the parse tree
	 */
	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code explainFormat}
	 * labeled alternative in {@link TrinoParser#explainOption}.
	 * @param ctx the parse tree
	 */
	fn visit_explainFormat(&mut self, ctx: &ExplainFormatContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code explainType}
	 * labeled alternative in {@link TrinoParser#explainOption}.
	 * @param ctx the parse tree
	 */
	fn visit_explainType(&mut self, ctx: &ExplainTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#privilege}.
	 * @param ctx the parse tree
	 */
	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link TrinoParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#pathExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rangeType}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link TrinoParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link TrinoParser#identifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#pathComponent}.
	 * @param ctx the parse tree
	 */
	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#identifierList}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentStruct}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentMap}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentArray}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentLambda}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentInteger}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentDefault}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#prestoShowFunctionRowField}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#prestoShowFunctionTypes}.
	 * @param ctx the parse tree
	 */
	fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TrinoParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) { self.visit_children(ctx) }

}

pub trait TrinoVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= TrinoParserContextType>{
	/**
	 * Visit a parse tree produced by {@link TrinoParser#multipleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#singleStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneType}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_use(&mut self, ctx: &UseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setSchemaAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropView(&mut self, ctx: &DropViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTableAsSelect}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRecursiveTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code insertInto}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createView(&mut self, ctx: &CreateViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code merge}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_merge(&mut self, ctx: &MergeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code set}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_set(&mut self, ctx: &SetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createSchema}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code drop}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_drop(&mut self, ctx: &DropContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code delete}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_delete(&mut self, ctx: &DeleteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comment}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_comment(&mut self, ctx: &CommentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code addColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dropColumn}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setColumnType}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableExecute}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code refreshMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameMaterializedView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setMaterializedViewProperties}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code renameView}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code setViewAuthorization}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_call(&mut self, ctx: &CallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code createRole}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code grant}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_grant(&mut self, ctx: &GrantContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code revoke}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_revoke(&mut self, ctx: &RevokeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deny}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deny(&mut self, ctx: &DenyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_explain(&mut self, ctx: &ExplainContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code show}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_show(&mut self, ctx: &ShowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code reset}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_reset(&mut self, ctx: &ResetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code startTransaction}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code commit}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_commit(&mut self, ctx: &CommitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollback}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollback(&mut self, ctx: &RollbackContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prepare}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_prepare(&mut self, ctx: &PrepareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code deallocate}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code execute}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_execute(&mut self, ctx: &ExecuteContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeInput}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code describeOutput}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code update}
	 * labeled alternative in {@link TrinoParser#statement}.
	 * @param ctx the parse tree
	 */
		fn visit_update(&mut self, ctx: &UpdateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableElements}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#query}.
	 * @param ctx the parse tree
	 */
		fn visit_query(&mut self, ctx: &QueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#with}.
	 * @param ctx the parse tree
	 */
		fn visit_with(&mut self, ctx: &WithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableElement}.
	 * @param ctx the parse tree
	 */
		fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#fieldDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnName}.
	 * @param ctx the parse tree
	 */
		fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnNameComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnSchemaWithMetadata}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnOptionList}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnOption}.
	 * @param ctx the parse tree
	 */
		fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnSchemaSimpleType}
	 * labeled alternative in {@link TrinoParser#columnSchema}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#properties}.
	 * @param ctx the parse tree
	 */
		fn visit_properties(&mut self, ctx: &PropertiesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#propertyAssignments}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nestedProperty}
	 * labeled alternative in {@link TrinoParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultProperty}
	 * labeled alternative in {@link TrinoParser#property}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#propertyKey}.
	 * @param ctx the parse tree
	 */
		fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identifierPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code expressionPropertyValue}
	 * labeled alternative in {@link TrinoParser#propertyValue}.
	 * @param ctx the parse tree
	 */
		fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryNoWith}.
	 * @param ctx the parse tree
	 */
		fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryLimit}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryLimitTargetDefault}
	 * labeled alternative in {@link TrinoParser#queryLimitTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowOrRows}.
	 * @param ctx the parse tree
	 */
		fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#limitRowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowCount}.
	 * @param ctx the parse tree
	 */
		fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperation}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setOperationIntersect}.
	 * @param ctx the parse tree
	 */
		fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setIntersectOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#inlineTable}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_table(&mut self, ctx: &TableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link TrinoParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sortItem}.
	 * @param ctx the parse tree
	 */
		fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#querySpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#querySelectItems}.
	 * @param ctx the parse tree
	 */
		fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupByDefault}
	 * labeled alternative in {@link TrinoParser#groupBy}.
	 * @param ctx the parse tree
	 */
		fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rollup}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_rollup(&mut self, ctx: &RollupContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cube}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_cube(&mut self, ctx: &CubeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code multipleGroupingSets}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code singleGroupingSet}
	 * labeled alternative in {@link TrinoParser#groupingElement}.
	 * @param ctx the parse tree
	 */
		fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#groupingSet}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowSpecificationPartitionBy}.
	 * @param ctx the parse tree
	 */
		fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#orderBy}.
	 * @param ctx the parse tree
	 */
		fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#namedQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#selectItemAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectSingle}
	 * labeled alternative in {@link TrinoParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code selectMulti}
	 * labeled alternative in {@link TrinoParser#selectItem}.
	 * @param ctx the parse tree
	 */
		fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#multiSelect}.
	 * @param ctx the parse tree
	 */
		fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#selectStar}.
	 * @param ctx the parse tree
	 */
		fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#relation}.
	 * @param ctx the parse tree
	 */
		fn visit_relation(&mut self, ctx: &RelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relationDefault}
	 * labeled alternative in {@link TrinoParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code joinRelation}
	 * labeled alternative in {@link TrinoParser#joinedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#joinType}.
	 * @param ctx the parse tree
	 */
		fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#noJoinRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampledRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampledRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampleOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#trimsSpecification}.
	 * @param ctx the parse tree
	 */
		fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#listAggOverflowBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#listaggCountIndication}.
	 * @param ctx the parse tree
	 */
		fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#patternRecognitionTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#patternRecognition}.
	 * @param ctx the parse tree
	 */
		fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#measureDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowsPerMatch}.
	 * @param ctx the parse tree
	 */
		fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#emptyMatchHandling}.
	 * @param ctx the parse tree
	 */
		fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#skipTo}.
	 * @param ctx the parse tree
	 */
		fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#subsetDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#variableDefinition}.
	 * @param ctx the parse tree
	 */
		fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aliasedRelationTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#aliasedRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#columnAliases}.
	 * @param ctx the parse tree
	 */
		fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_tableName(&mut self, ctx: &TableNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryRelation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unnest}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_unnest(&mut self, ctx: &UnnestContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lateral}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_lateral(&mut self, ctx: &LateralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableFunctionInvocation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedRelation}
	 * labeled alternative in {@link TrinoParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultTableFunctionCall}
	 * labeled alternative in {@link TrinoParser#tableFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgumentCopartition}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgumentName}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableFunctionArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#tableArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentTable}
	 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code tableArgumentQuery}
	 * labeled alternative in {@link TrinoParser#tableArgumentRelation}.
	 * @param ctx the parse tree
	 */
		fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#descriptorArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#descriptorField}.
	 * @param ctx the parse tree
	 */
		fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#copartitionTables}.
	 * @param ctx the parse tree
	 */
		fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_expression(&mut self, ctx: &ExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code or}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_or(&mut self, ctx: &OrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code and}
	 * labeled alternative in {@link TrinoParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_and(&mut self, ctx: &AndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedComparison}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_between(&mut self, ctx: &BetweenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inList}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inList(&mut self, ctx: &InListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inSubquery}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code like}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_like(&mut self, ctx: &LikeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code similarTo}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullPredicate}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code distinctFrom}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unknownPredicate}
	 * labeled alternative in {@link TrinoParser#predicate}.
	 * @param ctx the parse tree
	 */
		fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code concatenation}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code atTimeZone}
	 * labeled alternative in {@link TrinoParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonValue}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonValue(&mut self, ctx: &JsonValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_substring(&mut self, ctx: &SubstringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countStar}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_countStar(&mut self, ctx: &CountStarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_cast(&mut self, ctx: &CastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_trim(&mut self, ctx: &TrimContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code array}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_array(&mut self, ctx: &ArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code normalize}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonObject}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonObject(&mut self, ctx: &JsonObjectContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonArray}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonArray(&mut self, ctx: &JsonArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonExists}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonExists(&mut self, ctx: &JsonExistsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code jsonQuery}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonQuery(&mut self, ctx: &JsonQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_extract(&mut self, ctx: &ExtractContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code measure}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_measure(&mut self, ctx: &MeasureContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code arrayConstructor}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_variable(&mut self, ctx: &VariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_exists(&mut self, ctx: &ExistsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_position(&mut self, ctx: &PositionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code listagg}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_listagg(&mut self, ctx: &ListaggContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link TrinoParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionCallHead}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionCallTail}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code positionalArgument}
	 * labeled alternative in {@link TrinoParser#callArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionExtraArguments}.
	 * @param ctx the parse tree
	 */
		fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#functionName}.
	 * @param ctx the parse tree
	 */
		fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#field}.
	 * @param ctx the parse tree
	 */
		fn visit_field(&mut self, ctx: &FieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonPathInvocation}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonPathInvocation(&mut self, ctx: &JsonPathInvocationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonValueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonValueExpression(&mut self, ctx: &JsonValueExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonRepresentation}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonRepresentation(&mut self, ctx: &JsonRepresentationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonArgument(&mut self, ctx: &JsonArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonExistsErrorBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonExistsErrorBehavior(&mut self, ctx: &JsonExistsErrorBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonValueBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonValueBehavior(&mut self, ctx: &JsonValueBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonQueryWrapperBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonQueryWrapperBehavior(&mut self, ctx: &JsonQueryWrapperBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonQueryBehavior}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonQueryBehavior(&mut self, ctx: &JsonQueryBehaviorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#jsonObjectMember}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonObjectMember(&mut self, ctx: &JsonObjectMemberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#processingMode}.
	 * @param ctx the parse tree
	 */
		fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#nullTreatment}.
	 * @param ctx the parse tree
	 */
		fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code basicStringLiteral}
	 * labeled alternative in {@link TrinoParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unicodeStringLiteral}
	 * labeled alternative in {@link TrinoParser#string}.
	 * @param ctx the parse tree
	 */
		fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#timeZoneSpecifier}.
	 * @param ctx the parse tree
	 */
		fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#comparisonQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#booleanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#interval}.
	 * @param ctx the parse tree
	 */
		fn visit_interval(&mut self, ctx: &IntervalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#intervalField}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#normalForm}.
	 * @param ctx the parse tree
	 */
		fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#typeIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNotNull}
	 * labeled alternative in {@link TrinoParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeNull}
	 * labeled alternative in {@link TrinoParser#type_}.
	 * @param ctx the parse tree
	 */
		fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionSignatureGenericType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rowType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code intervalType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code dateTimeType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doublePrecisionType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyMapType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code legacyArrayType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code primitiveType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambdaType}
	 * labeled alternative in {@link TrinoParser#nonnullableType}.
	 * @param ctx the parse tree
	 */
		fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rowField}.
	 * @param ctx the parse tree
	 */
		fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#typeParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#whenClause}.
	 * @param ctx the parse tree
	 */
		fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#filter}.
	 * @param ctx the parse tree
	 */
		fn visit_filter(&mut self, ctx: &FilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#over}.
	 * @param ctx the parse tree
	 */
		fn visit_over(&mut self, ctx: &OverContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#windowFrame}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#frameExtent}.
	 * @param ctx the parse tree
	 */
		fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unboundedFrame}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code currentRowBound}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code boundedFrame}
	 * labeled alternative in {@link TrinoParser#frameBound}.
	 * @param ctx the parse tree
	 */
		fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quantifiedPrimary}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternConcatenation}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternAlternation}
	 * labeled alternative in {@link TrinoParser#rowPattern}.
	 * @param ctx the parse tree
	 */
		fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternVariable}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code emptyPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code patternPermutation}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupedPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionStartAnchor}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code partitionEndAnchor}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code excludedPattern}
	 * labeled alternative in {@link TrinoParser#patternPrimary}.
	 * @param ctx the parse tree
	 */
		fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrMoreQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code oneOrMoreQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code zeroOrOneQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeQuantifier}
	 * labeled alternative in {@link TrinoParser#patternQuantifier}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code isolationLevel}
	 * labeled alternative in {@link TrinoParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code transactionAccessMode}
	 * labeled alternative in {@link TrinoParser#transactionMode}.
	 * @param ctx the parse tree
	 */
		fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readUncommitted}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code readCommitted}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code repeatableRead}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code serializable}
	 * labeled alternative in {@link TrinoParser#levelOfIsolation}.
	 * @param ctx the parse tree
	 */
		fn visit_serializable(&mut self, ctx: &SerializableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code explainFormat}
	 * labeled alternative in {@link TrinoParser#explainOption}.
	 * @param ctx the parse tree
	 */
		fn visit_explainFormat(&mut self, ctx: &ExplainFormatContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code explainType}
	 * labeled alternative in {@link TrinoParser#explainOption}.
	 * @param ctx the parse tree
	 */
		fn visit_explainType(&mut self, ctx: &ExplainTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#privilege}.
	 * @param ctx the parse tree
	 */
		fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code qualifiedNameDefault}
	 * labeled alternative in {@link TrinoParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#pathExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#queryPeriod}.
	 * @param ctx the parse tree
	 */
		fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#rangeType}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unspecifiedPrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code userPrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rolePrincipal}
	 * labeled alternative in {@link TrinoParser#principal}.
	 * @param ctx the parse tree
	 */
		fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link TrinoParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierDefault}
	 * labeled alternative in {@link TrinoParser#identifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#pathComponent}.
	 * @param ctx the parse tree
	 */
		fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#standaloneIdentifier}.
	 * @param ctx the parse tree
	 */
		fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#identifierList}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link TrinoParser#number}.
	 * @param ctx the parse tree
	 */
		fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentStruct}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentMap}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentArray}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentLambda}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentInteger}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prestoFunctionArgumentDefault}
	 * labeled alternative in {@link TrinoParser#prestoShowFunctionType}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#prestoShowFunctionRowField}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#prestoShowFunctionTypes}.
	 * @param ctx the parse tree
	 */
		fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TrinoParser#nonReserved}.
	 * @param ctx the parse tree
	 */
		fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> TrinoVisitor<'input> for T
where
	T: TrinoVisitorCompat<'input>
{
	fn visit_multipleStatement(&mut self, ctx: &MultipleStatementContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_multipleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleStatement(&mut self, ctx: &SingleStatementContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_singleStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneExpression(&mut self, ctx: &StandaloneExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_standaloneExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneQualifiedName(&mut self, ctx: &StandaloneQualifiedNameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_standaloneQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneType(&mut self, ctx: &StandaloneTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_standaloneType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statementDefault(&mut self, ctx: &StatementDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_statementDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_use(&mut self, ctx: &UseContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_use(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropSchema(&mut self, ctx: &DropSchemaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dropSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameSchema(&mut self, ctx: &RenameSchemaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_renameSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setSchemaAuthorization(&mut self, ctx: &SetSchemaAuthorizationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setSchemaAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropTable(&mut self, ctx: &DropTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dropTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropView(&mut self, ctx: &DropViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dropView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTableAsSelect(&mut self, ctx: &CreateTableAsSelectContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createTableAsSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRecursiveTable(&mut self, ctx: &CreateRecursiveTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createRecursiveTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createTable(&mut self, ctx: &CreateTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_insertInto(&mut self, ctx: &InsertIntoContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_insertInto(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createMaterializedView(&mut self, ctx: &CreateMaterializedViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createView(&mut self, ctx: &CreateViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_showColumns(&mut self, ctx: &ShowColumnsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_showColumns(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createFunction(&mut self, ctx: &CreateFunctionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_merge(&mut self, ctx: &MergeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_merge(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_set(&mut self, ctx: &SetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_set(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createSchema(&mut self, ctx: &CreateSchemaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createSchema(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_drop(&mut self, ctx: &DropContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_drop(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delete(&mut self, ctx: &DeleteContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_delete(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_truncateTable(&mut self, ctx: &TruncateTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_truncateTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comment(&mut self, ctx: &CommentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_comment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameTable(&mut self, ctx: &RenameTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_renameTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addColumn(&mut self, ctx: &AddColumnContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_addColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameColumn(&mut self, ctx: &RenameColumnContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_renameColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropColumn(&mut self, ctx: &DropColumnContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dropColumn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setColumnType(&mut self, ctx: &SetColumnTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setColumnType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableAuthorization(&mut self, ctx: &SetTableAuthorizationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setTableAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setTableProperties(&mut self, ctx: &SetTablePropertiesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setTableProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableExecute(&mut self, ctx: &TableExecuteContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableExecute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_analyze(&mut self, ctx: &AnalyzeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_analyze(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_refreshMaterializedView(&mut self, ctx: &RefreshMaterializedViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_refreshMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameMaterializedView(&mut self, ctx: &RenameMaterializedViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_renameMaterializedView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setMaterializedViewProperties(&mut self, ctx: &SetMaterializedViewPropertiesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setMaterializedViewProperties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameView(&mut self, ctx: &RenameViewContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_renameView(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setViewAuthorization(&mut self, ctx: &SetViewAuthorizationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setViewAuthorization(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_call(&mut self, ctx: &CallContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_call(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_createRole(&mut self, ctx: &CreateRoleContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_createRole(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grant(&mut self, ctx: &GrantContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_grant(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_revoke(&mut self, ctx: &RevokeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_revoke(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deny(&mut self, ctx: &DenyContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_deny(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explain(&mut self, ctx: &ExplainContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_explain(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_show(&mut self, ctx: &ShowContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_show(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_reset(&mut self, ctx: &ResetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_reset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_startTransaction(&mut self, ctx: &StartTransactionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_startTransaction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commit(&mut self, ctx: &CommitContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_commit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollback(&mut self, ctx: &RollbackContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rollback(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prepare(&mut self, ctx: &PrepareContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prepare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_deallocate(&mut self, ctx: &DeallocateContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_deallocate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_execute(&mut self, ctx: &ExecuteContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_execute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeInput(&mut self, ctx: &DescribeInputContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_describeInput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeOutput(&mut self, ctx: &DescribeOutputContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_describeOutput(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_update(&mut self, ctx: &UpdateContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_update(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElements(&mut self, ctx: &TableElementsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableElements(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_query(&mut self, ctx: &QueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_query(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_with(&mut self, ctx: &WithContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_with(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableElement(&mut self, ctx: &TableElementContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnDefinition(&mut self, ctx: &ColumnDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldDefinition(&mut self, ctx: &FieldDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_fieldDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnName(&mut self, ctx: &ColumnNameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnNameComponent(&mut self, ctx: &ColumnNameComponentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnNameComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaWithMetadata(&mut self, ctx: &ColumnSchemaWithMetadataContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnSchemaWithMetadata(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOptionList(&mut self, ctx: &ColumnOptionListContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnOptionList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnOption(&mut self, ctx: &ColumnOptionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSchemaSimpleType(&mut self, ctx: &ColumnSchemaSimpleTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnSchemaSimpleType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_properties(&mut self, ctx: &PropertiesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_properties(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyAssignments(&mut self, ctx: &PropertyAssignmentsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_propertyAssignments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedProperty(&mut self, ctx: &NestedPropertyContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_nestedProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultProperty(&mut self, ctx: &DefaultPropertyContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_defaultProperty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_propertyKey(&mut self, ctx: &PropertyKeyContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_propertyKey(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultPropertyValue(&mut self, ctx: &DefaultPropertyValueContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_defaultPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierPropertyValue(&mut self, ctx: &IdentifierPropertyValueContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_identifierPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expressionPropertyValue(&mut self, ctx: &ExpressionPropertyValueContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_expressionPropertyValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryNoWith(&mut self, ctx: &QueryNoWithContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryNoWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimit(&mut self, ctx: &QueryLimitContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryLimit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryLimitTargetDefault(&mut self, ctx: &QueryLimitTargetDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryLimitTargetDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowOrRows(&mut self, ctx: &RowOrRowsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowOrRows(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_limitRowCount(&mut self, ctx: &LimitRowCountContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_limitRowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowCount(&mut self, ctx: &RowCountContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryTerm(&mut self, ctx: &QueryTermContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperation(&mut self, ctx: &SetOperationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setOperation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperator(&mut self, ctx: &SetOperatorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setOperationIntersect(&mut self, ctx: &SetOperationIntersectContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setOperationIntersect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setIntersectOperator(&mut self, ctx: &SetIntersectOperatorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setIntersectOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_setQuantifier(&mut self, ctx: &SetQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_setQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTable(&mut self, ctx: &InlineTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_inlineTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPrimaryDefault(&mut self, ctx: &QueryPrimaryDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryPrimaryDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_table(&mut self, ctx: &TableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_table(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inlineTableDefault1(&mut self, ctx: &InlineTableDefault1Context<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_inlineTableDefault1(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subquery(&mut self, ctx: &SubqueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_subquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortItem(&mut self, ctx: &SortItemContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_sortItem(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySpecification(&mut self, ctx: &QuerySpecificationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_querySpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_querySelectItems(&mut self, ctx: &QuerySelectItemsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_querySelectItems(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregationClause(&mut self, ctx: &AggregationClauseContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_aggregationClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupByDefault(&mut self, ctx: &GroupByDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_groupByDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rollup(&mut self, ctx: &RollupContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rollup(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cube(&mut self, ctx: &CubeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_cube(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multipleGroupingSets(&mut self, ctx: &MultipleGroupingSetsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_multipleGroupingSets(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleGroupingSet(&mut self, ctx: &SingleGroupingSetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_singleGroupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingSet(&mut self, ctx: &GroupingSetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_groupingSet(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowDefinition(&mut self, ctx: &WindowDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_windowDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecification(&mut self, ctx: &WindowSpecificationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_windowSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowSpecificationPartitionBy(&mut self, ctx: &WindowSpecificationPartitionByContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_windowSpecificationPartitionBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_orderBy(&mut self, ctx: &OrderByContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_orderBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedQuery(&mut self, ctx: &NamedQueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_namedQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectItemAlias(&mut self, ctx: &SelectItemAliasContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_selectItemAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectSingle(&mut self, ctx: &SelectSingleContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_selectSingle(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectMulti(&mut self, ctx: &SelectMultiContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_selectMulti(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiSelect(&mut self, ctx: &MultiSelectContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_multiSelect(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectStar(&mut self, ctx: &SelectStarContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_selectStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relation(&mut self, ctx: &RelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_relation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relationDefault(&mut self, ctx: &RelationDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_relationDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinRelation(&mut self, ctx: &JoinRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_joinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_joinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_joinCriteria(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_noJoinRelation(&mut self, ctx: &NoJoinRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_noJoinRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelationTarget(&mut self, ctx: &SampledRelationTargetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_sampledRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampledRelation(&mut self, ctx: &SampledRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_sampledRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleOperator(&mut self, ctx: &SampleOperatorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_sampleOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sampleMethod(&mut self, ctx: &SampleMethodContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_sampleMethod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trimsSpecification(&mut self, ctx: &TrimsSpecificationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_trimsSpecification(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listAggOverflowBehavior(&mut self, ctx: &ListAggOverflowBehaviorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_listAggOverflowBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listaggCountIndication(&mut self, ctx: &ListaggCountIndicationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_listaggCountIndication(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternRecognitionTarget(&mut self, ctx: &PatternRecognitionTargetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternRecognitionTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternRecognition(&mut self, ctx: &PatternRecognitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternRecognition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measureDefinition(&mut self, ctx: &MeasureDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_measureDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowsPerMatch(&mut self, ctx: &RowsPerMatchContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowsPerMatch(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_emptyMatchHandling(&mut self, ctx: &EmptyMatchHandlingContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_emptyMatchHandling(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_skipTo(&mut self, ctx: &SkipToContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_skipTo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subsetDefinition(&mut self, ctx: &SubsetDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_subsetDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variableDefinition(&mut self, ctx: &VariableDefinitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_variableDefinition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelationTarget(&mut self, ctx: &AliasedRelationTargetContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_aliasedRelationTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aliasedRelation(&mut self, ctx: &AliasedRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_aliasedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnAliases(&mut self, ctx: &ColumnAliasesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnAliases(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableName(&mut self, ctx: &TableNameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryRelation(&mut self, ctx: &SubqueryRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_subqueryRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unnest(&mut self, ctx: &UnnestContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unnest(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lateral(&mut self, ctx: &LateralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_lateral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionInvocation(&mut self, ctx: &TableFunctionInvocationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableFunctionInvocation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedRelation(&mut self, ctx: &ParenthesizedRelationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_parenthesizedRelation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultTableFunctionCall(&mut self, ctx: &DefaultTableFunctionCallContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_defaultTableFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentCopartition(&mut self, ctx: &TableFunctionArgumentCopartitionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableFunctionArgumentCopartition(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgumentName(&mut self, ctx: &TableFunctionArgumentNameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableFunctionArgumentName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunctionArgument(&mut self, ctx: &TableFunctionArgumentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableFunctionArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgument(&mut self, ctx: &TableArgumentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentTable(&mut self, ctx: &TableArgumentTableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableArgumentTable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableArgumentQuery(&mut self, ctx: &TableArgumentQueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_tableArgumentQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorArgument(&mut self, ctx: &DescriptorArgumentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_descriptorArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_descriptorField(&mut self, ctx: &DescriptorFieldContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_descriptorField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_copartitionTables(&mut self, ctx: &CopartitionTablesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_copartitionTables(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expression(&mut self, ctx: &ExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_expression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_logicalNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_predicated(&mut self, ctx: &PredicatedContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_predicated(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_or(&mut self, ctx: &OrContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_or(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_and(&mut self, ctx: &AndContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_and(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparison(&mut self, ctx: &ComparisonContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_comparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedComparison(&mut self, ctx: &QuantifiedComparisonContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_quantifiedComparison(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_between(&mut self, ctx: &BetweenContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_between(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inList(&mut self, ctx: &InListContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_inList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inSubquery(&mut self, ctx: &InSubqueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_inSubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_like(&mut self, ctx: &LikeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_like(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_similarTo(&mut self, ctx: &SimilarToContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_similarTo(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullPredicate(&mut self, ctx: &NullPredicateContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_nullPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_distinctFrom(&mut self, ctx: &DistinctFromContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_distinctFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unknownPredicate(&mut self, ctx: &UnknownPredicateContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unknownPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueExpressionDefault(&mut self, ctx: &ValueExpressionDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_valueExpressionDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_concatenation(&mut self, ctx: &ConcatenationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_concatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticBinary(&mut self, ctx: &ArithmeticBinaryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_arithmeticBinary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arithmeticUnary(&mut self, ctx: &ArithmeticUnaryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_arithmeticUnary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atTimeZone(&mut self, ctx: &AtTimeZoneContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_atTimeZone(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dereference(&mut self, ctx: &DereferenceContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dereference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeConstructor(&mut self, ctx: &TypeConstructorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_typeConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonValue(&mut self, ctx: &JsonValueContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_substring(&mut self, ctx: &SubstringContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_substring(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countStar(&mut self, ctx: &CountStarContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_countStar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cast(&mut self, ctx: &CastContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_cast(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_lambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenthesizedExpression(&mut self, ctx: &ParenthesizedExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_parenthesizedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trim(&mut self, ctx: &TrimContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_trim(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_array(&mut self, ctx: &ArrayContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_array(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalize(&mut self, ctx: &NormalizeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_normalize(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonObject(&mut self, ctx: &JsonObjectContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonObject(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_intervalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_numericLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_booleanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonArray(&mut self, ctx: &JsonArrayContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonArray(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_simpleCase(&mut self, ctx: &SimpleCaseContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_simpleCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnReference(&mut self, ctx: &ColumnReferenceContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_columnReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullLiteral(&mut self, ctx: &NullLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_nullLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowConstructor(&mut self, ctx: &RowConstructorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subscript(&mut self, ctx: &SubscriptContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_subscript(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonExists(&mut self, ctx: &JsonExistsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonExists(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subqueryExpression(&mut self, ctx: &SubqueryExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_subqueryExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryLiteral(&mut self, ctx: &BinaryLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_binaryLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonQuery(&mut self, ctx: &JsonQueryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extract(&mut self, ctx: &ExtractContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_extract(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_measure(&mut self, ctx: &MeasureContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_measure(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_arrayConstructor(&mut self, ctx: &ArrayConstructorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_arrayConstructor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variable(&mut self, ctx: &VariableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_variable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exists(&mut self, ctx: &ExistsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_exists(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_position(&mut self, ctx: &PositionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_position(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_listagg(&mut self, ctx: &ListaggContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_listagg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchedCase(&mut self, ctx: &SearchedCaseContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_searchedCase(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallHead(&mut self, ctx: &FunctionCallHeadContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionCallHead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallTail(&mut self, ctx: &FunctionCallTailContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionCallTail(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionalArgument(&mut self, ctx: &PositionalArgumentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_positionalArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionExtraArguments(&mut self, ctx: &FunctionExtraArgumentsContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionExtraArguments(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_field(&mut self, ctx: &FieldContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_field(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonPathInvocation(&mut self, ctx: &JsonPathInvocationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonPathInvocation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonValueExpression(&mut self, ctx: &JsonValueExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonValueExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonRepresentation(&mut self, ctx: &JsonRepresentationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonRepresentation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonArgument(&mut self, ctx: &JsonArgumentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonExistsErrorBehavior(&mut self, ctx: &JsonExistsErrorBehaviorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonExistsErrorBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonValueBehavior(&mut self, ctx: &JsonValueBehaviorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonValueBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonQueryWrapperBehavior(&mut self, ctx: &JsonQueryWrapperBehaviorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonQueryWrapperBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonQueryBehavior(&mut self, ctx: &JsonQueryBehaviorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonQueryBehavior(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonObjectMember(&mut self, ctx: &JsonObjectMemberContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_jsonObjectMember(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_processingMode(&mut self, ctx: &ProcessingModeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_processingMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullTreatment(&mut self, ctx: &NullTreatmentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_nullTreatment(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_basicStringLiteral(&mut self, ctx: &BasicStringLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_basicStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unicodeStringLiteral(&mut self, ctx: &UnicodeStringLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unicodeStringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeZoneSpecifier(&mut self, ctx: &TimeZoneSpecifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_timeZoneSpecifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_comparisonOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonQuantifier(&mut self, ctx: &ComparisonQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_comparisonQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanValue(&mut self, ctx: &BooleanValueContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_booleanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_interval(&mut self, ctx: &IntervalContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_interval(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalField(&mut self, ctx: &IntervalFieldContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_intervalField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_normalForm(&mut self, ctx: &NormalFormContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_normalForm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeIdentifier(&mut self, ctx: &TypeIdentifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_typeIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNotNull(&mut self, ctx: &TypeNotNullContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_typeNotNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeNull(&mut self, ctx: &TypeNullContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_typeNull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionSignatureGenericType(&mut self, ctx: &FunctionSignatureGenericTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_functionSignatureGenericType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowType(&mut self, ctx: &RowTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalType(&mut self, ctx: &IntervalTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_intervalType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateTimeType(&mut self, ctx: &DateTimeTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_dateTimeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doublePrecisionType(&mut self, ctx: &DoublePrecisionTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_doublePrecisionType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyMapType(&mut self, ctx: &LegacyMapTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_legacyMapType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_legacyArrayType(&mut self, ctx: &LegacyArrayTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_legacyArrayType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_primitiveType(&mut self, ctx: &PrimitiveTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_primitiveType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambdaType(&mut self, ctx: &LambdaTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_lambdaType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowField(&mut self, ctx: &RowFieldContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeParameter(&mut self, ctx: &TypeParameterContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_typeParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whenClause(&mut self, ctx: &WhenClauseContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_whenClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_filter(&mut self, ctx: &FilterContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_filter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_over(&mut self, ctx: &OverContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_over(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFrame(&mut self, ctx: &WindowFrameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_windowFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_frameExtent(&mut self, ctx: &FrameExtentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_frameExtent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unboundedFrame(&mut self, ctx: &UnboundedFrameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unboundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentRowBound(&mut self, ctx: &CurrentRowBoundContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_currentRowBound(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_boundedFrame(&mut self, ctx: &BoundedFrameContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_boundedFrame(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quantifiedPrimary(&mut self, ctx: &QuantifiedPrimaryContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_quantifiedPrimary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternConcatenation(&mut self, ctx: &PatternConcatenationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternConcatenation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternAlternation(&mut self, ctx: &PatternAlternationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternAlternation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternVariable(&mut self, ctx: &PatternVariableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternVariable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_emptyPattern(&mut self, ctx: &EmptyPatternContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_emptyPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternPermutation(&mut self, ctx: &PatternPermutationContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_patternPermutation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupedPattern(&mut self, ctx: &GroupedPatternContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_groupedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionStartAnchor(&mut self, ctx: &PartitionStartAnchorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_partitionStartAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionEndAnchor(&mut self, ctx: &PartitionEndAnchorContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_partitionEndAnchor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_excludedPattern(&mut self, ctx: &ExcludedPatternContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_excludedPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrMoreQuantifier(&mut self, ctx: &ZeroOrMoreQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_zeroOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_oneOrMoreQuantifier(&mut self, ctx: &OneOrMoreQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_oneOrMoreQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_zeroOrOneQuantifier(&mut self, ctx: &ZeroOrOneQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_zeroOrOneQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeQuantifier(&mut self, ctx: &RangeQuantifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rangeQuantifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_isolationLevel(&mut self, ctx: &IsolationLevelContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_isolationLevel(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transactionAccessMode(&mut self, ctx: &TransactionAccessModeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_transactionAccessMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readUncommitted(&mut self, ctx: &ReadUncommittedContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_readUncommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_readCommitted(&mut self, ctx: &ReadCommittedContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_readCommitted(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_repeatableRead(&mut self, ctx: &RepeatableReadContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_repeatableRead(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_serializable(&mut self, ctx: &SerializableContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_serializable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explainFormat(&mut self, ctx: &ExplainFormatContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_explainFormat(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explainType(&mut self, ctx: &ExplainTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_explainType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_privilege(&mut self, ctx: &PrivilegeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_privilege(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_qualifiedNameDefault(&mut self, ctx: &QualifiedNameDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_qualifiedNameDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathExpression(&mut self, ctx: &PathExpressionContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_pathExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryPeriod(&mut self, ctx: &QueryPeriodContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_queryPeriod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeType(&mut self, ctx: &RangeTypeContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rangeType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unspecifiedPrincipal(&mut self, ctx: &UnspecifiedPrincipalContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unspecifiedPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_userPrincipal(&mut self, ctx: &UserPrincipalContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_userPrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rolePrincipal(&mut self, ctx: &RolePrincipalContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_rolePrincipal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unquotedIdentifier(&mut self, ctx: &UnquotedIdentifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_unquotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifierDefault(&mut self, ctx: &QuotedIdentifierDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_quotedIdentifierDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedIdentifier(&mut self, ctx: &QuotedIdentifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_quotedIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathComponent(&mut self, ctx: &PathComponentContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_pathComponent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_standaloneIdentifier(&mut self, ctx: &StandaloneIdentifierContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_standaloneIdentifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierList(&mut self, ctx: &IdentifierListContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_identifierList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identifierSeq(&mut self, ctx: &IdentifierSeqContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_identifierSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_decimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_doubleLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_integerLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentStruct(&mut self, ctx: &PrestoFunctionArgumentStructContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentStruct(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentMap(&mut self, ctx: &PrestoFunctionArgumentMapContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentMap(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentArray(&mut self, ctx: &PrestoFunctionArgumentArrayContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentArray(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentLambda(&mut self, ctx: &PrestoFunctionArgumentLambdaContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentLambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentInteger(&mut self, ctx: &PrestoFunctionArgumentIntegerContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentInteger(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoFunctionArgumentDefault(&mut self, ctx: &PrestoFunctionArgumentDefaultContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoFunctionArgumentDefault(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoShowFunctionRowField(&mut self, ctx: &PrestoShowFunctionRowFieldContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoShowFunctionRowField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prestoShowFunctionTypes(&mut self, ctx: &PrestoShowFunctionTypesContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_prestoShowFunctionTypes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nonReserved(&mut self, ctx: &NonReservedContext<'input>){
		let result = <Self as TrinoVisitorCompat>::visit_nonReserved(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}