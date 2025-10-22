#![allow(nonstandard_style)]
// Generated from Snowflake.g4 by ANTLR 4.8
use antlr_rust::tree::ParseTreeListener;
use super::snowflakeparser::*;

pub trait SnowflakeListener<'input> : ParseTreeListener<'input,SnowflakeParserContextType>{
/**
 * Enter a parse tree produced by {@link SnowflakeParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn enter_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#multipleStatement}.
 * @param ctx the parse tree
 */
fn exit_multipleStatement(&mut self, _ctx: &MultipleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#singleStatement}.
 * @param ctx the parse tree
 */
fn enter_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#singleStatement}.
 * @param ctx the parse tree
 */
fn exit_singleStatement(&mut self, _ctx: &SingleStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn enter_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#standaloneExpression}.
 * @param ctx the parse tree
 */
fn exit_standaloneExpression(&mut self, _ctx: &StandaloneExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#standaloneQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_standaloneQualifiedName(&mut self, _ctx: &StandaloneQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#standaloneType}.
 * @param ctx the parse tree
 */
fn enter_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#standaloneType}.
 * @param ctx the parse tree
 */
fn exit_standaloneType(&mut self, _ctx: &StandaloneTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statementDefault}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_statementDefault(&mut self, _ctx: &StatementDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code use}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code use}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_use(&mut self, _ctx: &UseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropSchema(&mut self, _ctx: &DropSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameSchema(&mut self, _ctx: &RenameSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setSchemaAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setSchemaAuthorization(&mut self, _ctx: &SetSchemaAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropTable(&mut self, _ctx: &DropTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropView(&mut self, _ctx: &DropViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createExternalTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createExternalTable(&mut self, _ctx: &CreateExternalTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createExternalTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createExternalTable(&mut self, _ctx: &CreateExternalTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeCreateTableAsSelect}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_snowflakeCreateTableAsSelect(&mut self, _ctx: &SnowflakeCreateTableAsSelectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeCreateTableAsSelect}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_snowflakeCreateTableAsSelect(&mut self, _ctx: &SnowflakeCreateTableAsSelectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createTableClone}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createTableClone(&mut self, _ctx: &CreateTableCloneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createTableClone}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createTableClone(&mut self, _ctx: &CreateTableCloneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createDynamicTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createDynamicTable(&mut self, _ctx: &CreateDynamicTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createDynamicTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createDynamicTable(&mut self, _ctx: &CreateDynamicTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createEventTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createEventTable(&mut self, _ctx: &CreateEventTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createEventTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createEventTable(&mut self, _ctx: &CreateEventTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createIcebergTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createIcebergTable(&mut self, _ctx: &CreateIcebergTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createIcebergTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createIcebergTable(&mut self, _ctx: &CreateIcebergTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRecursiveTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRecursiveTable(&mut self, _ctx: &CreateRecursiveTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRecursiveTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRecursiveTable(&mut self, _ctx: &CreateRecursiveTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeCreateTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_snowflakeCreateTable(&mut self, _ctx: &SnowflakeCreateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeCreateTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_snowflakeCreateTable(&mut self, _ctx: &SnowflakeCreateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeCreateTableUsingTemplate}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_snowflakeCreateTableUsingTemplate(&mut self, _ctx: &SnowflakeCreateTableUsingTemplateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeCreateTableUsingTemplate}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_snowflakeCreateTableUsingTemplate(&mut self, _ctx: &SnowflakeCreateTableUsingTemplateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeInsertInto}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_snowflakeInsertInto(&mut self, _ctx: &SnowflakeInsertIntoContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeInsertInto}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_snowflakeInsertInto(&mut self, _ctx: &SnowflakeInsertIntoContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createMaterializedView(&mut self, _ctx: &CreateMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createView(&mut self, _ctx: &CreateViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code set}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code set}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_set(&mut self, _ctx: &SetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code showColumns}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_showColumns(&mut self, _ctx: &ShowColumnsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createStage}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createStage(&mut self, _ctx: &CreateStageContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createStage}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createStage(&mut self, _ctx: &CreateStageContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createJavaFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createJavaFunction(&mut self, _ctx: &CreateJavaFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createJavaFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createJavaFunction(&mut self, _ctx: &CreateJavaFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createJarFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createJarFunction(&mut self, _ctx: &CreateJarFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createJarFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createJarFunction(&mut self, _ctx: &CreateJarFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createJSFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createJSFunction(&mut self, _ctx: &CreateJSFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createJSFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createJSFunction(&mut self, _ctx: &CreateJSFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createPythonFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createPythonFunction(&mut self, _ctx: &CreatePythonFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createPythonFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createPythonFunction(&mut self, _ctx: &CreatePythonFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createModuleFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createModuleFunction(&mut self, _ctx: &CreateModuleFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createModuleFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createModuleFunction(&mut self, _ctx: &CreateModuleFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createScalaFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createScalaFunction(&mut self, _ctx: &CreateScalaFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createScalaFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createScalaFunction(&mut self, _ctx: &CreateScalaFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createScalaJarFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createScalaJarFunction(&mut self, _ctx: &CreateScalaJarFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createScalaJarFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createScalaJarFunction(&mut self, _ctx: &CreateScalaJarFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSqlFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSqlFunction(&mut self, _ctx: &CreateSqlFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSqlFunction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSqlFunction(&mut self, _ctx: &CreateSqlFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createPythonProcedure}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createPythonProcedure(&mut self, _ctx: &CreatePythonProcedureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createPythonProcedure}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createPythonProcedure(&mut self, _ctx: &CreatePythonProcedureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createAnonymousPythonProcedure}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createAnonymousPythonProcedure(&mut self, _ctx: &CreateAnonymousPythonProcedureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createAnonymousPythonProcedure}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createAnonymousPythonProcedure(&mut self, _ctx: &CreateAnonymousPythonProcedureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unset}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_unset(&mut self, _ctx: &UnsetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unset}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_unset(&mut self, _ctx: &UnsetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code merge}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code merge}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_merge(&mut self, _ctx: &MergeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code alter}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code alter}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_alter(&mut self, _ctx: &AlterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code begin}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code begin}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_begin(&mut self, _ctx: &BeginContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createFoo}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createFoo(&mut self, _ctx: &CreateFooContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createSchema}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createSchema(&mut self, _ctx: &CreateSchemaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code drop}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code drop}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_drop(&mut self, _ctx: &DropContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code delete}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code delete}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_delete(&mut self, _ctx: &DeleteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code truncateTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_truncateTable(&mut self, _ctx: &TruncateTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comment}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comment}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_comment(&mut self, _ctx: &CommentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameTable}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameTable(&mut self, _ctx: &RenameTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code addColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_addColumn(&mut self, _ctx: &AddColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameColumn(&mut self, _ctx: &RenameColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dropColumn}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_dropColumn(&mut self, _ctx: &DropColumnContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setColumnType}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setColumnType(&mut self, _ctx: &SetColumnTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableAuthorization(&mut self, _ctx: &SetTableAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setTableProperties}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setTableProperties(&mut self, _ctx: &SetTablePropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableExecute}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_tableExecute(&mut self, _ctx: &TableExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code analyze}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code analyze}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_analyze(&mut self, _ctx: &AnalyzeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code refreshMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_refreshMaterializedView(&mut self, _ctx: &RefreshMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameMaterializedView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameMaterializedView(&mut self, _ctx: &RenameMaterializedViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setMaterializedViewProperties}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setMaterializedViewProperties(&mut self, _ctx: &SetMaterializedViewPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code renameView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code renameView}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_renameView(&mut self, _ctx: &RenameViewContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code setViewAuthorization}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_setViewAuthorization(&mut self, _ctx: &SetViewAuthorizationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code call}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code call}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_call(&mut self, _ctx: &CallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code createRole}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code createRole}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_createRole(&mut self, _ctx: &CreateRoleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code grant}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code grant}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_grant(&mut self, _ctx: &GrantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code revoke}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code revoke}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_revoke(&mut self, _ctx: &RevokeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deny}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deny}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deny(&mut self, _ctx: &DenyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code explain}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code explain}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_explain(&mut self, _ctx: &ExplainContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code show}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code show}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_show(&mut self, _ctx: &ShowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code reset}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code reset}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_reset(&mut self, _ctx: &ResetContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code startTransaction}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_startTransaction(&mut self, _ctx: &StartTransactionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code commit}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code commit}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_commit(&mut self, _ctx: &CommitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollback}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollback}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_rollback(&mut self, _ctx: &RollbackContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prepare}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prepare}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_prepare(&mut self, _ctx: &PrepareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code deallocate}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_deallocate(&mut self, _ctx: &DeallocateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code execute}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code execute}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_execute(&mut self, _ctx: &ExecuteContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeInput}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeInput(&mut self, _ctx: &DescribeInputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code describeOutput}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_describeOutput(&mut self, _ctx: &DescribeOutputContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code update}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn enter_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code update}
 * labeled alternative in {@link SnowflakeParser#statement}.
 * @param ctx the parse tree
 */
fn exit_update(&mut self, _ctx: &UpdateContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableElements}.
 * @param ctx the parse tree
 */
fn enter_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableElements}.
 * @param ctx the parse tree
 */
fn exit_tableElements(&mut self, _ctx: &TableElementsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn enter_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#unpivotNullClause}.
 * @param ctx the parse tree
 */
fn exit_unpivotNullClause(&mut self, _ctx: &UnpivotNullClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeCreateTableClauses}.
 * @param ctx the parse tree
 */
fn enter_snowflakeCreateTableClauses(&mut self, _ctx: &SnowflakeCreateTableClausesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeCreateTableClauses}.
 * @param ctx the parse tree
 */
fn exit_snowflakeCreateTableClauses(&mut self, _ctx: &SnowflakeCreateTableClausesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn enter_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableConstraint}.
 * @param ctx the parse tree
 */
fn exit_tableConstraint(&mut self, _ctx: &TableConstraintContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#constraintProperties}.
 * @param ctx the parse tree
 */
fn enter_constraintProperties(&mut self, _ctx: &ConstraintPropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#constraintProperties}.
 * @param ctx the parse tree
 */
fn exit_constraintProperties(&mut self, _ctx: &ConstraintPropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeValueRow}.
 * @param ctx the parse tree
 */
fn enter_snowflakeValueRow(&mut self, _ctx: &SnowflakeValueRowContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeValueRow}.
 * @param ctx the parse tree
 */
fn exit_snowflakeValueRow(&mut self, _ctx: &SnowflakeValueRowContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeValueItem}.
 * @param ctx the parse tree
 */
fn enter_snowflakeValueItem(&mut self, _ctx: &SnowflakeValueItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeValueItem}.
 * @param ctx the parse tree
 */
fn exit_snowflakeValueItem(&mut self, _ctx: &SnowflakeValueItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeCreateExternalTableClauses}.
 * @param ctx the parse tree
 */
fn enter_snowflakeCreateExternalTableClauses(&mut self, _ctx: &SnowflakeCreateExternalTableClausesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeCreateExternalTableClauses}.
 * @param ctx the parse tree
 */
fn exit_snowflakeCreateExternalTableClauses(&mut self, _ctx: &SnowflakeCreateExternalTableClausesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#locationSpec}.
 * @param ctx the parse tree
 */
fn enter_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#locationSpec}.
 * @param ctx the parse tree
 */
fn exit_locationSpec(&mut self, _ctx: &LocationSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#partitionedByNameSpec}.
 * @param ctx the parse tree
 */
fn enter_partitionedByNameSpec(&mut self, _ctx: &PartitionedByNameSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#partitionedByNameSpec}.
 * @param ctx the parse tree
 */
fn exit_partitionedByNameSpec(&mut self, _ctx: &PartitionedByNameSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn enter_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#createFileFormat}.
 * @param ctx the parse tree
 */
fn exit_createFileFormat(&mut self, _ctx: &CreateFileFormatContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#compressionSpec}.
 * @param ctx the parse tree
 */
fn enter_compressionSpec(&mut self, _ctx: &CompressionSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#compressionSpec}.
 * @param ctx the parse tree
 */
fn exit_compressionSpec(&mut self, _ctx: &CompressionSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#headerRowSpec}.
 * @param ctx the parse tree
 */
fn enter_headerRowSpec(&mut self, _ctx: &HeaderRowSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#headerRowSpec}.
 * @param ctx the parse tree
 */
fn exit_headerRowSpec(&mut self, _ctx: &HeaderRowSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#delimiterSpec}.
 * @param ctx the parse tree
 */
fn enter_delimiterSpec(&mut self, _ctx: &DelimiterSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#delimiterSpec}.
 * @param ctx the parse tree
 */
fn exit_delimiterSpec(&mut self, _ctx: &DelimiterSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#with}.
 * @param ctx the parse tree
 */
fn enter_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#with}.
 * @param ctx the parse tree
 */
fn exit_with(&mut self, _ctx: &WithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableElement}.
 * @param ctx the parse tree
 */
fn enter_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableElement}.
 * @param ctx the parse tree
 */
fn exit_tableElement(&mut self, _ctx: &TableElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn enter_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnDefinition}.
 * @param ctx the parse tree
 */
fn exit_columnDefinition(&mut self, _ctx: &ColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#inlineConstraint}.
 * @param ctx the parse tree
 */
fn enter_inlineConstraint(&mut self, _ctx: &InlineConstraintContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#inlineConstraint}.
 * @param ctx the parse tree
 */
fn exit_inlineConstraint(&mut self, _ctx: &InlineConstraintContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnDefinitionForView}.
 * @param ctx the parse tree
 */
fn enter_columnDefinitionForView(&mut self, _ctx: &ColumnDefinitionForViewContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnDefinitionForView}.
 * @param ctx the parse tree
 */
fn exit_columnDefinitionForView(&mut self, _ctx: &ColumnDefinitionForViewContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#externalColumnDefinition}.
 * @param ctx the parse tree
 */
fn enter_externalColumnDefinition(&mut self, _ctx: &ExternalColumnDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#externalColumnDefinition}.
 * @param ctx the parse tree
 */
fn exit_externalColumnDefinition(&mut self, _ctx: &ExternalColumnDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn enter_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#fieldDefinition}.
 * @param ctx the parse tree
 */
fn exit_fieldDefinition(&mut self, _ctx: &FieldDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnName}.
 * @param ctx the parse tree
 */
fn enter_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnName}.
 * @param ctx the parse tree
 */
fn exit_columnName(&mut self, _ctx: &ColumnNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn enter_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnNameComponent}.
 * @param ctx the parse tree
 */
fn exit_columnNameComponent(&mut self, _ctx: &ColumnNameComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnSchemaWithMetadata}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaWithMetadata(&mut self, _ctx: &ColumnSchemaWithMetadataContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn enter_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnOptionList}.
 * @param ctx the parse tree
 */
fn exit_columnOptionList(&mut self, _ctx: &ColumnOptionListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnOption}.
 * @param ctx the parse tree
 */
fn enter_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnOption}.
 * @param ctx the parse tree
 */
fn exit_columnOption(&mut self, _ctx: &ColumnOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link SnowflakeParser#columnSchema}.
 * @param ctx the parse tree
 */
fn enter_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnSchemaSimpleType}
 * labeled alternative in {@link SnowflakeParser#columnSchema}.
 * @param ctx the parse tree
 */
fn exit_columnSchemaSimpleType(&mut self, _ctx: &ColumnSchemaSimpleTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#properties}.
 * @param ctx the parse tree
 */
fn enter_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#properties}.
 * @param ctx the parse tree
 */
fn exit_properties(&mut self, _ctx: &PropertiesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn enter_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#propertyAssignments}.
 * @param ctx the parse tree
 */
fn exit_propertyAssignments(&mut self, _ctx: &PropertyAssignmentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link SnowflakeParser#property}.
 * @param ctx the parse tree
 */
fn enter_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedProperty}
 * labeled alternative in {@link SnowflakeParser#property}.
 * @param ctx the parse tree
 */
fn exit_nestedProperty(&mut self, _ctx: &NestedPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link SnowflakeParser#property}.
 * @param ctx the parse tree
 */
fn enter_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultProperty}
 * labeled alternative in {@link SnowflakeParser#property}.
 * @param ctx the parse tree
 */
fn exit_defaultProperty(&mut self, _ctx: &DefaultPropertyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#propertyKey}.
 * @param ctx the parse tree
 */
fn enter_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#propertyKey}.
 * @param ctx the parse tree
 */
fn exit_propertyKey(&mut self, _ctx: &PropertyKeyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_defaultPropertyValue(&mut self, _ctx: &DefaultPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_identifierPropertyValue(&mut self, _ctx: &IdentifierPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn enter_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code expressionPropertyValue}
 * labeled alternative in {@link SnowflakeParser#propertyValue}.
 * @param ctx the parse tree
 */
fn exit_expressionPropertyValue(&mut self, _ctx: &ExpressionPropertyValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn enter_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#queryNoWith}.
 * @param ctx the parse tree
 */
fn exit_queryNoWith(&mut self, _ctx: &QueryNoWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#queryLimit}.
 * @param ctx the parse tree
 */
fn enter_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#queryLimit}.
 * @param ctx the parse tree
 */
fn exit_queryLimit(&mut self, _ctx: &QueryLimitContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
 * labeled alternative in {@link SnowflakeParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn enter_queryLimitTargetRedshiftSnowflake(&mut self, _ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryLimitTargetRedshiftSnowflake}
 * labeled alternative in {@link SnowflakeParser#queryLimitTarget}.
 * @param ctx the parse tree
 */
fn exit_queryLimitTargetRedshiftSnowflake(&mut self, _ctx: &QueryLimitTargetRedshiftSnowflakeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#rowOrRows}.
 * @param ctx the parse tree
 */
fn enter_rowOrRows(&mut self, _ctx: &RowOrRowsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#rowOrRows}.
 * @param ctx the parse tree
 */
fn exit_rowOrRows(&mut self, _ctx: &RowOrRowsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn enter_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#limitRowCount}.
 * @param ctx the parse tree
 */
fn exit_limitRowCount(&mut self, _ctx: &LimitRowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#rowCount}.
 * @param ctx the parse tree
 */
fn enter_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#rowCount}.
 * @param ctx the parse tree
 */
fn exit_rowCount(&mut self, _ctx: &RowCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#queryTerm}.
 * @param ctx the parse tree
 */
fn enter_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#queryTerm}.
 * @param ctx the parse tree
 */
fn exit_queryTerm(&mut self, _ctx: &QueryTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#setOperation}.
 * @param ctx the parse tree
 */
fn enter_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#setOperation}.
 * @param ctx the parse tree
 */
fn exit_setOperation(&mut self, _ctx: &SetOperationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#setOperator}.
 * @param ctx the parse tree
 */
fn enter_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#setOperator}.
 * @param ctx the parse tree
 */
fn exit_setOperator(&mut self, _ctx: &SetOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn enter_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#setOperationIntersect}.
 * @param ctx the parse tree
 */
fn exit_setOperationIntersect(&mut self, _ctx: &SetOperationIntersectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn enter_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#setIntersectOperator}.
 * @param ctx the parse tree
 */
fn exit_setIntersectOperator(&mut self, _ctx: &SetIntersectOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn enter_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#setQuantifier}.
 * @param ctx the parse tree
 */
fn exit_setQuantifier(&mut self, _ctx: &SetQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#inlineTable}.
 * @param ctx the parse tree
 */
fn enter_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#inlineTable}.
 * @param ctx the parse tree
 */
fn exit_inlineTable(&mut self, _ctx: &InlineTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code queryPrimaryDefault}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_queryPrimaryDefault(&mut self, _ctx: &QueryPrimaryDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code table}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code table}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_table(&mut self, _ctx: &TableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subquery}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn enter_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subquery}
 * labeled alternative in {@link SnowflakeParser#queryPrimary}.
 * @param ctx the parse tree
 */
fn exit_subquery(&mut self, _ctx: &SubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sortItem}.
 * @param ctx the parse tree
 */
fn enter_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sortItem}.
 * @param ctx the parse tree
 */
fn exit_sortItem(&mut self, _ctx: &SortItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#connectByItem}.
 * @param ctx the parse tree
 */
fn enter_connectByItem(&mut self, _ctx: &ConnectByItemContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#connectByItem}.
 * @param ctx the parse tree
 */
fn exit_connectByItem(&mut self, _ctx: &ConnectByItemContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#querySpecification}.
 * @param ctx the parse tree
 */
fn enter_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#querySpecification}.
 * @param ctx the parse tree
 */
fn exit_querySpecification(&mut self, _ctx: &QuerySpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#connectBy}.
 * @param ctx the parse tree
 */
fn enter_connectBy(&mut self, _ctx: &ConnectByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#connectBy}.
 * @param ctx the parse tree
 */
fn exit_connectBy(&mut self, _ctx: &ConnectByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#replaceDefinition}.
 * @param ctx the parse tree
 */
fn enter_replaceDefinition(&mut self, _ctx: &ReplaceDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#replaceDefinition}.
 * @param ctx the parse tree
 */
fn exit_replaceDefinition(&mut self, _ctx: &ReplaceDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn enter_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#querySelectItems}.
 * @param ctx the parse tree
 */
fn exit_querySelectItems(&mut self, _ctx: &QuerySelectItemsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn enter_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#aggregationClause}.
 * @param ctx the parse tree
 */
fn exit_aggregationClause(&mut self, _ctx: &AggregationClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link SnowflakeParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByAll}
 * labeled alternative in {@link SnowflakeParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByAll(&mut self, _ctx: &GroupByAllContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link SnowflakeParser#groupBy}.
 * @param ctx the parse tree
 */
fn enter_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupByDefault}
 * labeled alternative in {@link SnowflakeParser#groupBy}.
 * @param ctx the parse tree
 */
fn exit_groupByDefault(&mut self, _ctx: &GroupByDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rollup}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rollup}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_rollup(&mut self, _ctx: &RollupContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cube}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cube}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_cube(&mut self, _ctx: &CubeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multipleGroupingSets}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_multipleGroupingSets(&mut self, _ctx: &MultipleGroupingSetsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn enter_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleGroupingSet}
 * labeled alternative in {@link SnowflakeParser#groupingElement}.
 * @param ctx the parse tree
 */
fn exit_singleGroupingSet(&mut self, _ctx: &SingleGroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#groupingSet}.
 * @param ctx the parse tree
 */
fn enter_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#groupingSet}.
 * @param ctx the parse tree
 */
fn exit_groupingSet(&mut self, _ctx: &GroupingSetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn enter_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#windowDefinition}.
 * @param ctx the parse tree
 */
fn exit_windowDefinition(&mut self, _ctx: &WindowDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn enter_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#windowSpecification}.
 * @param ctx the parse tree
 */
fn exit_windowSpecification(&mut self, _ctx: &WindowSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn enter_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#windowSpecificationPartitionBy}.
 * @param ctx the parse tree
 */
fn exit_windowSpecificationPartitionBy(&mut self, _ctx: &WindowSpecificationPartitionByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#orderBy}.
 * @param ctx the parse tree
 */
fn enter_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#orderBy}.
 * @param ctx the parse tree
 */
fn exit_orderBy(&mut self, _ctx: &OrderByContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#namedQuery}.
 * @param ctx the parse tree
 */
fn enter_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#namedQuery}.
 * @param ctx the parse tree
 */
fn exit_namedQuery(&mut self, _ctx: &NamedQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn enter_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#selectItemAlias}.
 * @param ctx the parse tree
 */
fn exit_selectItemAlias(&mut self, _ctx: &SelectItemAliasContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link SnowflakeParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectSingle}
 * labeled alternative in {@link SnowflakeParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectSingle(&mut self, _ctx: &SelectSingleContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link SnowflakeParser#selectItem}.
 * @param ctx the parse tree
 */
fn enter_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code selectMulti}
 * labeled alternative in {@link SnowflakeParser#selectItem}.
 * @param ctx the parse tree
 */
fn exit_selectMulti(&mut self, _ctx: &SelectMultiContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#multiSelect}.
 * @param ctx the parse tree
 */
fn enter_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#multiSelect}.
 * @param ctx the parse tree
 */
fn exit_multiSelect(&mut self, _ctx: &MultiSelectContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#selectStar}.
 * @param ctx the parse tree
 */
fn enter_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#selectStar}.
 * @param ctx the parse tree
 */
fn exit_selectStar(&mut self, _ctx: &SelectStarContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#relation}.
 * @param ctx the parse tree
 */
fn enter_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#relation}.
 * @param ctx the parse tree
 */
fn exit_relation(&mut self, _ctx: &RelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code asofJoinRelation}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_asofJoinRelation(&mut self, _ctx: &AsofJoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code asofJoinRelation}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_asofJoinRelation(&mut self, _ctx: &AsofJoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relationDefault}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_relationDefault(&mut self, _ctx: &RelationDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn enter_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code joinRelation}
 * labeled alternative in {@link SnowflakeParser#joinedRelation}.
 * @param ctx the parse tree
 */
fn exit_joinRelation(&mut self, _ctx: &JoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn enter_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#noJoinRelation}.
 * @param ctx the parse tree
 */
fn exit_noJoinRelation(&mut self, _ctx: &NoJoinRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code aliased2}
 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_aliased2(&mut self, _ctx: &Aliased2Context<'input>) { }
/**
 * Exit a parse tree produced by the {@code aliased2}
 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_aliased2(&mut self, _ctx: &Aliased2Context<'input>) { }
/**
 * Enter a parse tree produced by the {@code inlineTableDefault}
 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_inlineTableDefault(&mut self, _ctx: &InlineTableDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inlineTableDefault}
 * labeled alternative in {@link SnowflakeParser#sampledRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_inlineTableDefault(&mut self, _ctx: &InlineTableDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn enter_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sampledRelation}.
 * @param ctx the parse tree
 */
fn exit_sampledRelation(&mut self, _ctx: &SampledRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn enter_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sampleOperator}.
 * @param ctx the parse tree
 */
fn exit_sampleOperator(&mut self, _ctx: &SampleOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn enter_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sampleMethod}.
 * @param ctx the parse tree
 */
fn exit_sampleMethod(&mut self, _ctx: &SampleMethodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#samplePercentage}.
 * @param ctx the parse tree
 */
fn enter_samplePercentage(&mut self, _ctx: &SamplePercentageContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#samplePercentage}.
 * @param ctx the parse tree
 */
fn exit_samplePercentage(&mut self, _ctx: &SamplePercentageContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sampleCount}.
 * @param ctx the parse tree
 */
fn enter_sampleCount(&mut self, _ctx: &SampleCountContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sampleCount}.
 * @param ctx the parse tree
 */
fn exit_sampleCount(&mut self, _ctx: &SampleCountContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#sampleSeed}.
 * @param ctx the parse tree
 */
fn enter_sampleSeed(&mut self, _ctx: &SampleSeedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#sampleSeed}.
 * @param ctx the parse tree
 */
fn exit_sampleSeed(&mut self, _ctx: &SampleSeedContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn enter_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#trimsSpecification}.
 * @param ctx the parse tree
 */
fn exit_trimsSpecification(&mut self, _ctx: &TrimsSpecificationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn enter_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#listAggOverflowBehavior}.
 * @param ctx the parse tree
 */
fn exit_listAggOverflowBehavior(&mut self, _ctx: &ListAggOverflowBehaviorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn enter_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#listaggCountIndication}.
 * @param ctx the parse tree
 */
fn exit_listaggCountIndication(&mut self, _ctx: &ListaggCountIndicationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#patternRecognitionTarget}.
 * @param ctx the parse tree
 */
fn enter_patternRecognitionTarget(&mut self, _ctx: &PatternRecognitionTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#patternRecognitionTarget}.
 * @param ctx the parse tree
 */
fn exit_patternRecognitionTarget(&mut self, _ctx: &PatternRecognitionTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn enter_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#patternRecognition}.
 * @param ctx the parse tree
 */
fn exit_patternRecognition(&mut self, _ctx: &PatternRecognitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn enter_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#measureDefinition}.
 * @param ctx the parse tree
 */
fn exit_measureDefinition(&mut self, _ctx: &MeasureDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn enter_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#rowsPerMatch}.
 * @param ctx the parse tree
 */
fn exit_rowsPerMatch(&mut self, _ctx: &RowsPerMatchContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn enter_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#emptyMatchHandling}.
 * @param ctx the parse tree
 */
fn exit_emptyMatchHandling(&mut self, _ctx: &EmptyMatchHandlingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#skipTo}.
 * @param ctx the parse tree
 */
fn enter_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#skipTo}.
 * @param ctx the parse tree
 */
fn exit_skipTo(&mut self, _ctx: &SkipToContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn enter_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#subsetDefinition}.
 * @param ctx the parse tree
 */
fn exit_subsetDefinition(&mut self, _ctx: &SubsetDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn enter_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#variableDefinition}.
 * @param ctx the parse tree
 */
fn exit_variableDefinition(&mut self, _ctx: &VariableDefinitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#atBefore}.
 * @param ctx the parse tree
 */
fn enter_atBefore(&mut self, _ctx: &AtBeforeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#atBefore}.
 * @param ctx the parse tree
 */
fn exit_atBefore(&mut self, _ctx: &AtBeforeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code aliased}
 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code aliased}
 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_aliased(&mut self, _ctx: &AliasedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code directory}
 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_directory(&mut self, _ctx: &DirectoryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code directory}
 * labeled alternative in {@link SnowflakeParser#changeRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_directory(&mut self, _ctx: &DirectoryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#changesRelation}.
 * @param ctx the parse tree
 */
fn enter_changesRelation(&mut self, _ctx: &ChangesRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#changesRelation}.
 * @param ctx the parse tree
 */
fn exit_changesRelation(&mut self, _ctx: &ChangesRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pattern}
 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_pattern(&mut self, _ctx: &PatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pattern}
 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_pattern(&mut self, _ctx: &PatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableFunctionInvocation}
 * labeled alternative in {@link SnowflakeParser#pivotedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionInvocation(&mut self, _ctx: &TableFunctionInvocationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn enter_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pivotedRelation}.
 * @param ctx the parse tree
 */
fn exit_pivotedRelation(&mut self, _ctx: &PivotedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#aliasedRelation2}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation2(&mut self, _ctx: &AliasedRelation2Context<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#aliasedRelation2}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation2(&mut self, _ctx: &AliasedRelation2Context<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn enter_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pivotAggregates}.
 * @param ctx the parse tree
 */
fn exit_pivotAggregates(&mut self, _ctx: &PivotAggregatesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn enter_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pivotFrom}.
 * @param ctx the parse tree
 */
fn exit_pivotFrom(&mut self, _ctx: &PivotFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntoDefault}
 * labeled alternative in {@link SnowflakeParser#pivotInto}.
 * @param ctx the parse tree
 */
fn enter_pivotIntoDefault(&mut self, _ctx: &PivotIntoDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntoDefault}
 * labeled alternative in {@link SnowflakeParser#pivotInto}.
 * @param ctx the parse tree
 */
fn exit_pivotIntoDefault(&mut self, _ctx: &PivotIntoDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn enter_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pivotAsAlias}.
 * @param ctx the parse tree
 */
fn exit_pivotAsAlias(&mut self, _ctx: &PivotAsAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#singleColumnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivot(&mut self, _ctx: &SingleColumnUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn enter_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnsToUnpivot}.
 * @param ctx the parse tree
 */
fn exit_columnsToUnpivot(&mut self, _ctx: &ColumnsToUnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link SnowflakeParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn enter_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code singleColumnUnpivotDefault}
 * labeled alternative in {@link SnowflakeParser#columnUnpivot}.
 * @param ctx the parse tree
 */
fn exit_singleColumnUnpivotDefault(&mut self, _ctx: &SingleColumnUnpivotDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosDefault}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosDefault(&mut self, _ctx: &PivotIntosDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosAny}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosAny(&mut self, _ctx: &PivotIntosAnyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosAny}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosAny(&mut self, _ctx: &PivotIntosAnyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivotIntosQuery}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn enter_pivotIntosQuery(&mut self, _ctx: &PivotIntosQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivotIntosQuery}
 * labeled alternative in {@link SnowflakeParser#pivotIntos}.
 * @param ctx the parse tree
 */
fn exit_pivotIntosQuery(&mut self, _ctx: &PivotIntosQueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code pivot}
 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code pivot}
 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_pivot(&mut self, _ctx: &PivotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn enter_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unpivot}
 * labeled alternative in {@link SnowflakeParser#pivotOperator}.
 * @param ctx the parse tree
 */
fn exit_unpivot(&mut self, _ctx: &UnpivotContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelationTarget(&mut self, _ctx: &AliasedRelationTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#aliasedRelationTarget}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelationTarget(&mut self, _ctx: &AliasedRelationTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn enter_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#aliasedRelation}.
 * @param ctx the parse tree
 */
fn exit_aliasedRelation(&mut self, _ctx: &AliasedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#columnAliases}.
 * @param ctx the parse tree
 */
fn enter_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#columnAliases}.
 * @param ctx the parse tree
 */
fn exit_columnAliases(&mut self, _ctx: &ColumnAliasesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lateral}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lateral}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_lateral(&mut self, _ctx: &LateralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableName}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableName}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_tableName(&mut self, _ctx: &TableNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryRelation}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_subqueryRelation(&mut self, _ctx: &SubqueryRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedRelation}
 * labeled alternative in {@link SnowflakeParser#relationPrimary}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedRelation(&mut self, _ctx: &ParenthesizedRelationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code validate}
 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_validate(&mut self, _ctx: &ValidateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code validate}
 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_validate(&mut self, _ctx: &ValidateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultTableFunctionCall}
 * labeled alternative in {@link SnowflakeParser#tableFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_defaultTableFunctionCall(&mut self, _ctx: &DefaultTableFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentCopartition}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentCopartition(&mut self, _ctx: &TableFunctionArgumentCopartitionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableFunctionArgumentName}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgumentName(&mut self, _ctx: &TableFunctionArgumentNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn enter_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableFunctionArgument}.
 * @param ctx the parse tree
 */
fn exit_tableFunctionArgument(&mut self, _ctx: &TableFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#tableArgument}.
 * @param ctx the parse tree
 */
fn enter_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#tableArgument}.
 * @param ctx the parse tree
 */
fn exit_tableArgument(&mut self, _ctx: &TableArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentTable}
 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentTable(&mut self, _ctx: &TableArgumentTableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn enter_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code tableArgumentQuery}
 * labeled alternative in {@link SnowflakeParser#tableArgumentRelation}.
 * @param ctx the parse tree
 */
fn exit_tableArgumentQuery(&mut self, _ctx: &TableArgumentQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn enter_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#descriptorArgument}.
 * @param ctx the parse tree
 */
fn exit_descriptorArgument(&mut self, _ctx: &DescriptorArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#descriptorField}.
 * @param ctx the parse tree
 */
fn enter_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#descriptorField}.
 * @param ctx the parse tree
 */
fn exit_descriptorField(&mut self, _ctx: &DescriptorFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn enter_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#copartitionTables}.
 * @param ctx the parse tree
 */
fn exit_copartitionTables(&mut self, _ctx: &CopartitionTablesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#expression}.
 * @param ctx the parse tree
 */
fn enter_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#expression}.
 * @param ctx the parse tree
 */
fn exit_expression(&mut self, _ctx: &ExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultBooleanExpression}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_defaultBooleanExpression(&mut self, _ctx: &DefaultBooleanExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code or}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code or}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_or(&mut self, _ctx: &OrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code predicated}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code predicated}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_predicated(&mut self, _ctx: &PredicatedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code and}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn enter_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code and}
 * labeled alternative in {@link SnowflakeParser#booleanExpression}.
 * @param ctx the parse tree
 */
fn exit_and(&mut self, _ctx: &AndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code comparison}
 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code comparison}
 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_comparison(&mut self, _ctx: &ComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn enter_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedComparison}
 * labeled alternative in {@link SnowflakeParser#comparisonPredicate}.
 * @param ctx the parse tree
 */
fn exit_quantifiedComparison(&mut self, _ctx: &QuantifiedComparisonContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn enter_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#nonComparisonExpression}.
 * @param ctx the parse tree
 */
fn exit_nonComparisonExpression(&mut self, _ctx: &NonComparisonExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inList}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inList}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inList(&mut self, _ctx: &InListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubquery}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_inSubquery(&mut self, _ctx: &InSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code likeAny}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_likeAny(&mut self, _ctx: &LikeAnyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code likeAny}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_likeAny(&mut self, _ctx: &LikeAnyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code collate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_collate(&mut self, _ctx: &CollateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code collate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_collate(&mut self, _ctx: &CollateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code regexp}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_regexp(&mut self, _ctx: &RegexpContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code regexp}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_regexp(&mut self, _ctx: &RegexpContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rlike}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_rlike(&mut self, _ctx: &RlikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rlike}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_rlike(&mut self, _ctx: &RlikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code like}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code like}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_like(&mut self, _ctx: &LikeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code similarTo}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_similarTo(&mut self, _ctx: &SimilarToContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullPredicate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_nullPredicate(&mut self, _ctx: &NullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctFrom}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_distinctFrom(&mut self, _ctx: &DistinctFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn enter_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unknownPredicate}
 * labeled alternative in {@link SnowflakeParser#predicate}.
 * @param ctx the parse tree
 */
fn exit_unknownPredicate(&mut self, _ctx: &UnknownPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpressionDefault}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_valueExpressionDefault(&mut self, _ctx: &ValueExpressionDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code concatenation}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_concatenation(&mut self, _ctx: &ConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticBinary}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticBinary(&mut self, _ctx: &ArithmeticBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arithmeticUnary}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_arithmeticUnary(&mut self, _ctx: &ArithmeticUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code atTimeZone}
 * labeled alternative in {@link SnowflakeParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_atTimeZone(&mut self, _ctx: &AtTimeZoneContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereference(&mut self, _ctx: &DereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code mod}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_mod(&mut self, _ctx: &ModContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code mod}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_mod(&mut self, _ctx: &ModContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code firstValueFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_firstValueFunction(&mut self, _ctx: &FirstValueFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code firstValueFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_firstValueFunction(&mut self, _ctx: &FirstValueFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReferenceByPosition}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReferenceByPosition(&mut self, _ctx: &ColumnReferenceByPositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReferenceByPosition}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReferenceByPosition(&mut self, _ctx: &ColumnReferenceByPositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueDereference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_valueDereference(&mut self, _ctx: &ValueDereferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueDereference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_valueDereference(&mut self, _ctx: &ValueDereferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_identifierExpression(&mut self, _ctx: &IdentifierExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_identifierExpression(&mut self, _ctx: &IdentifierExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code arrayAggFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_arrayAggFunction(&mut self, _ctx: &ArrayAggFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code arrayAggFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_arrayAggFunction(&mut self, _ctx: &ArrayAggFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decode}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_decode(&mut self, _ctx: &DecodeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decode}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_decode(&mut self, _ctx: &DecodeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dereferenceByPosition}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_dereferenceByPosition(&mut self, _ctx: &DereferenceByPositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dereferenceByPosition}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_dereferenceByPosition(&mut self, _ctx: &DereferenceByPositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countStar}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countStar}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_countStar(&mut self, _ctx: &CountStarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileContFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileContFunction(&mut self, _ctx: &PercentileContFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code cast}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code cast}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_cast(&mut self, _ctx: &CastContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code minhash}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_minhash(&mut self, _ctx: &MinhashContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code minhash}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_minhash(&mut self, _ctx: &MinhashContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambda}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambda}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code parenthesizedExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_parenthesizedExpression(&mut self, _ctx: &ParenthesizedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code trim}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code trim}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_trim(&mut self, _ctx: &TrimContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code array}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code array}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_array(&mut self, _ctx: &ArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code normalize}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code normalize}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_normalize(&mut self, _ctx: &NormalizeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code castOperator}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_castOperator(&mut self, _ctx: &CastOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code simpleCase}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_simpleCase(&mut self, _ctx: &SimpleCaseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code columnReference}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_columnReference(&mut self, _ctx: &ColumnReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rowConstructor}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_rowConstructor(&mut self, _ctx: &RowConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code objectLiteral}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_objectLiteral(&mut self, _ctx: &ObjectLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code objectLiteral}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_objectLiteral(&mut self, _ctx: &ObjectLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code subqueryExpression}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_subqueryExpression(&mut self, _ctx: &SubqueryExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code constantDefault}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_constantDefault(&mut self, _ctx: &ConstantDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code constantDefault}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_constantDefault(&mut self, _ctx: &ConstantDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code extract}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code extract}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_extract(&mut self, _ctx: &ExtractContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code measure}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code measure}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_measure(&mut self, _ctx: &MeasureContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCall}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code variable}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code variable}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_variable(&mut self, _ctx: &VariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exists}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exists}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_exists(&mut self, _ctx: &ExistsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileDiscFunction}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_percentileDiscFunction(&mut self, _ctx: &PercentileDiscFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code position}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code position}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_position(&mut self, _ctx: &PositionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code listagg}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code listagg}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_listagg(&mut self, _ctx: &ListaggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn enter_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchedCase}
 * labeled alternative in {@link SnowflakeParser#primaryExpression}.
 * @param ctx the parse tree
 */
fn exit_searchedCase(&mut self, _ctx: &SearchedCaseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn enter_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#functionCallHead}.
 * @param ctx the parse tree
 */
fn exit_functionCallHead(&mut self, _ctx: &FunctionCallHeadContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn enter_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#functionCallTail}.
 * @param ctx the parse tree
 */
fn exit_functionCallTail(&mut self, _ctx: &FunctionCallTailContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code positionalArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_positionalArgument(&mut self, _ctx: &PositionalArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code namedArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_namedArgument(&mut self, _ctx: &NamedArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code filesNamedFunctionArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_filesNamedFunctionArgument(&mut self, _ctx: &FilesNamedFunctionArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code filesNamedFunctionArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_filesNamedFunctionArgument(&mut self, _ctx: &FilesNamedFunctionArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn enter_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code multiArgument}
 * labeled alternative in {@link SnowflakeParser#callArgument}.
 * @param ctx the parse tree
 */
fn exit_multiArgument(&mut self, _ctx: &MultiArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn enter_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#functionExtraArguments}.
 * @param ctx the parse tree
 */
fn exit_functionExtraArguments(&mut self, _ctx: &FunctionExtraArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nullLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_nullLiteral(&mut self, _ctx: &NullLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code intervalLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code booleanLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryLiteral}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_binaryLiteral(&mut self, _ctx: &BinaryLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn enter_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeConstructor}
 * labeled alternative in {@link SnowflakeParser#constant}.
 * @param ctx the parse tree
 */
fn exit_typeConstructor(&mut self, _ctx: &TypeConstructorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#dereferenceKey}.
 * @param ctx the parse tree
 */
fn enter_dereferenceKey(&mut self, _ctx: &DereferenceKeyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#dereferenceKey}.
 * @param ctx the parse tree
 */
fn exit_dereferenceKey(&mut self, _ctx: &DereferenceKeyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#dereferenceKeyElement}.
 * @param ctx the parse tree
 */
fn enter_dereferenceKeyElement(&mut self, _ctx: &DereferenceKeyElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#dereferenceKeyElement}.
 * @param ctx the parse tree
 */
fn exit_dereferenceKeyElement(&mut self, _ctx: &DereferenceKeyElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#dereferenceKeyText}.
 * @param ctx the parse tree
 */
fn enter_dereferenceKeyText(&mut self, _ctx: &DereferenceKeyTextContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#dereferenceKeyText}.
 * @param ctx the parse tree
 */
fn exit_dereferenceKeyText(&mut self, _ctx: &DereferenceKeyTextContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#namedParameter}.
 * @param ctx the parse tree
 */
fn enter_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#namedParameter}.
 * @param ctx the parse tree
 */
fn exit_namedParameter(&mut self, _ctx: &NamedParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#stageFileSpec}.
 * @param ctx the parse tree
 */
fn enter_stageFileSpec(&mut self, _ctx: &StageFileSpecContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#stageFileSpec}.
 * @param ctx the parse tree
 */
fn exit_stageFileSpec(&mut self, _ctx: &StageFileSpecContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#field}.
 * @param ctx the parse tree
 */
fn enter_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#field}.
 * @param ctx the parse tree
 */
fn exit_field(&mut self, _ctx: &FieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#processingMode}.
 * @param ctx the parse tree
 */
fn enter_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#processingMode}.
 * @param ctx the parse tree
 */
fn exit_processingMode(&mut self, _ctx: &ProcessingModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn enter_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#nullTreatment}.
 * @param ctx the parse tree
 */
fn exit_nullTreatment(&mut self, _ctx: &NullTreatmentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn enter_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code basicStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn exit_basicStringLiteral(&mut self, _ctx: &BasicStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn enter_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unicodeStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn exit_unicodeStringLiteral(&mut self, _ctx: &UnicodeStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dollarQuotedStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn enter_dollarQuotedStringLiteral(&mut self, _ctx: &DollarQuotedStringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dollarQuotedStringLiteral}
 * labeled alternative in {@link SnowflakeParser#string}.
 * @param ctx the parse tree
 */
fn exit_dollarQuotedStringLiteral(&mut self, _ctx: &DollarQuotedStringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn enter_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#timeZoneSpecifier}.
 * @param ctx the parse tree
 */
fn exit_timeZoneSpecifier(&mut self, _ctx: &TimeZoneSpecifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn enter_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#comparisonQuantifier}.
 * @param ctx the parse tree
 */
fn exit_comparisonQuantifier(&mut self, _ctx: &ComparisonQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#booleanValue}.
 * @param ctx the parse tree
 */
fn enter_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#booleanValue}.
 * @param ctx the parse tree
 */
fn exit_booleanValue(&mut self, _ctx: &BooleanValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#interval}.
 * @param ctx the parse tree
 */
fn enter_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#interval}.
 * @param ctx the parse tree
 */
fn exit_interval(&mut self, _ctx: &IntervalContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#intervalField}.
 * @param ctx the parse tree
 */
fn enter_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#intervalField}.
 * @param ctx the parse tree
 */
fn exit_intervalField(&mut self, _ctx: &IntervalFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#normalForm}.
 * @param ctx the parse tree
 */
fn enter_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#normalForm}.
 * @param ctx the parse tree
 */
fn exit_normalForm(&mut self, _ctx: &NormalFormContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn enter_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#typeIdentifier}.
 * @param ctx the parse tree
 */
fn exit_typeIdentifier(&mut self, _ctx: &TypeIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link SnowflakeParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNotNull}
 * labeled alternative in {@link SnowflakeParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNotNull(&mut self, _ctx: &TypeNotNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link SnowflakeParser#type_}.
 * @param ctx the parse tree
 */
fn enter_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeNull}
 * labeled alternative in {@link SnowflakeParser#type_}.
 * @param ctx the parse tree
 */
fn exit_typeNull(&mut self, _ctx: &TypeNullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionSignatureGenericType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_functionSignatureGenericType(&mut self, _ctx: &FunctionSignatureGenericTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doublePrecisionType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_doublePrecisionType(&mut self, _ctx: &DoublePrecisionTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code characterVarying}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_characterVarying(&mut self, _ctx: &CharacterVaryingContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code characterVarying}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_characterVarying(&mut self, _ctx: &CharacterVaryingContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code dateTimeType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_dateTimeType(&mut self, _ctx: &DateTimeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambdaType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_lambdaType(&mut self, _ctx: &LambdaTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyStructType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyStructType(&mut self, _ctx: &LegacyStructTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyStructType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyStructType(&mut self, _ctx: &LegacyStructTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code structuredObjectType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_structuredObjectType(&mut self, _ctx: &StructuredObjectTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code structuredObjectType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_structuredObjectType(&mut self, _ctx: &StructuredObjectTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code legacyMapType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_legacyMapType(&mut self, _ctx: &LegacyMapTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn enter_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code primitiveType}
 * labeled alternative in {@link SnowflakeParser#nonnullableType}.
 * @param ctx the parse tree
 */
fn exit_primitiveType(&mut self, _ctx: &PrimitiveTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#rowField}.
 * @param ctx the parse tree
 */
fn enter_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#rowField}.
 * @param ctx the parse tree
 */
fn exit_rowField(&mut self, _ctx: &RowFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#typeParameter}.
 * @param ctx the parse tree
 */
fn enter_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#typeParameter}.
 * @param ctx the parse tree
 */
fn exit_typeParameter(&mut self, _ctx: &TypeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#whenClause}.
 * @param ctx the parse tree
 */
fn enter_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#whenClause}.
 * @param ctx the parse tree
 */
fn exit_whenClause(&mut self, _ctx: &WhenClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#filter}.
 * @param ctx the parse tree
 */
fn enter_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#filter}.
 * @param ctx the parse tree
 */
fn exit_filter(&mut self, _ctx: &FilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#over}.
 * @param ctx the parse tree
 */
fn enter_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#over}.
 * @param ctx the parse tree
 */
fn exit_over(&mut self, _ctx: &OverContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#windowFrame}.
 * @param ctx the parse tree
 */
fn enter_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#windowFrame}.
 * @param ctx the parse tree
 */
fn exit_windowFrame(&mut self, _ctx: &WindowFrameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#frameExtent}.
 * @param ctx the parse tree
 */
fn enter_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#frameExtent}.
 * @param ctx the parse tree
 */
fn exit_frameExtent(&mut self, _ctx: &FrameExtentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unboundedFrame}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_unboundedFrame(&mut self, _ctx: &UnboundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code currentRowBound}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_currentRowBound(&mut self, _ctx: &CurrentRowBoundContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn enter_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code boundedFrame}
 * labeled alternative in {@link SnowflakeParser#frameBound}.
 * @param ctx the parse tree
 */
fn exit_boundedFrame(&mut self, _ctx: &BoundedFrameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quantifiedPrimary}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_quantifiedPrimary(&mut self, _ctx: &QuantifiedPrimaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternConcatenation}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternConcatenation(&mut self, _ctx: &PatternConcatenationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn enter_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternAlternation}
 * labeled alternative in {@link SnowflakeParser#rowPattern}.
 * @param ctx the parse tree
 */
fn exit_patternAlternation(&mut self, _ctx: &PatternAlternationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternVariable}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternVariable(&mut self, _ctx: &PatternVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code emptyPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_emptyPattern(&mut self, _ctx: &EmptyPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code patternPermutation}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_patternPermutation(&mut self, _ctx: &PatternPermutationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_groupedPattern(&mut self, _ctx: &GroupedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionStartAnchor}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionStartAnchor(&mut self, _ctx: &PartitionStartAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code partitionEndAnchor}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_partitionEndAnchor(&mut self, _ctx: &PartitionEndAnchorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn enter_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code excludedPattern}
 * labeled alternative in {@link SnowflakeParser#patternPrimary}.
 * @param ctx the parse tree
 */
fn exit_excludedPattern(&mut self, _ctx: &ExcludedPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrMoreQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrMoreQuantifier(&mut self, _ctx: &ZeroOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code oneOrMoreQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_oneOrMoreQuantifier(&mut self, _ctx: &OneOrMoreQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code zeroOrOneQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_zeroOrOneQuantifier(&mut self, _ctx: &ZeroOrOneQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn enter_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeQuantifier}
 * labeled alternative in {@link SnowflakeParser#patternQuantifier}.
 * @param ctx the parse tree
 */
fn exit_rangeQuantifier(&mut self, _ctx: &RangeQuantifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link SnowflakeParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isolationLevel}
 * labeled alternative in {@link SnowflakeParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_isolationLevel(&mut self, _ctx: &IsolationLevelContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link SnowflakeParser#transactionMode}.
 * @param ctx the parse tree
 */
fn enter_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code transactionAccessMode}
 * labeled alternative in {@link SnowflakeParser#transactionMode}.
 * @param ctx the parse tree
 */
fn exit_transactionAccessMode(&mut self, _ctx: &TransactionAccessModeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readUncommitted}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readUncommitted(&mut self, _ctx: &ReadUncommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code readCommitted}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_readCommitted(&mut self, _ctx: &ReadCommittedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code repeatableRead}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_repeatableRead(&mut self, _ctx: &RepeatableReadContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code serializable}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn enter_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code serializable}
 * labeled alternative in {@link SnowflakeParser#levelOfIsolation}.
 * @param ctx the parse tree
 */
fn exit_serializable(&mut self, _ctx: &SerializableContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#privilege}.
 * @param ctx the parse tree
 */
fn enter_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#privilege}.
 * @param ctx the parse tree
 */
fn exit_privilege(&mut self, _ctx: &PrivilegeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code qualifiedNameDefault}
 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_qualifiedNameDefault(&mut self, _ctx: &QualifiedNameDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identifierFunction}
 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_identifierFunction(&mut self, _ctx: &IdentifierFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identifierFunction}
 * labeled alternative in {@link SnowflakeParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_identifierFunction(&mut self, _ctx: &IdentifierFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pathExpression}.
 * @param ctx the parse tree
 */
fn enter_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pathExpression}.
 * @param ctx the parse tree
 */
fn exit_pathExpression(&mut self, _ctx: &PathExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#nonquotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_nonquotedIdentifier(&mut self, _ctx: &NonquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#nonquotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_nonquotedIdentifier(&mut self, _ctx: &NonquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#dashedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_dashedIdentifier(&mut self, _ctx: &DashedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#dashedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_dashedIdentifier(&mut self, _ctx: &DashedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#maybeDashedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_maybeDashedIdentifier(&mut self, _ctx: &MaybeDashedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#maybeDashedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_maybeDashedIdentifier(&mut self, _ctx: &MaybeDashedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn enter_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#queryPeriod}.
 * @param ctx the parse tree
 */
fn exit_queryPeriod(&mut self, _ctx: &QueryPeriodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#rangeType}.
 * @param ctx the parse tree
 */
fn enter_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#rangeType}.
 * @param ctx the parse tree
 */
fn exit_rangeType(&mut self, _ctx: &RangeTypeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn enter_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unspecifiedPrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn exit_unspecifiedPrincipal(&mut self, _ctx: &UnspecifiedPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn enter_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code userPrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn exit_userPrincipal(&mut self, _ctx: &UserPrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn enter_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rolePrincipal}
 * labeled alternative in {@link SnowflakeParser#principal}.
 * @param ctx the parse tree
 */
fn exit_rolePrincipal(&mut self, _ctx: &RolePrincipalContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code strictIdentifierDefault}
 * labeled alternative in {@link SnowflakeParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_strictIdentifierDefault(&mut self, _ctx: &StrictIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code strictIdentifierDefault}
 * labeled alternative in {@link SnowflakeParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_strictIdentifierDefault(&mut self, _ctx: &StrictIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code strictNonReservedIdentifier}
 * labeled alternative in {@link SnowflakeParser#identifier}.
 * @param ctx the parse tree
 */
fn enter_strictNonReservedIdentifier(&mut self, _ctx: &StrictNonReservedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code strictNonReservedIdentifier}
 * labeled alternative in {@link SnowflakeParser#identifier}.
 * @param ctx the parse tree
 */
fn exit_strictNonReservedIdentifier(&mut self, _ctx: &StrictNonReservedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn enter_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unquotedIdentifier}
 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn exit_unquotedIdentifier(&mut self, _ctx: &UnquotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedIdentifierDefault}
 * labeled alternative in {@link SnowflakeParser#strictIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifierDefault(&mut self, _ctx: &QuotedIdentifierDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn enter_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#quotedIdentifier}.
 * @param ctx the parse tree
 */
fn exit_quotedIdentifier(&mut self, _ctx: &QuotedIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#pathComponent}.
 * @param ctx the parse tree
 */
fn enter_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#pathComponent}.
 * @param ctx the parse tree
 */
fn exit_pathComponent(&mut self, _ctx: &PathComponentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn enter_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#standaloneIdentifier}.
 * @param ctx the parse tree
 */
fn exit_standaloneIdentifier(&mut self, _ctx: &StandaloneIdentifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#identifierList}.
 * @param ctx the parse tree
 */
fn enter_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#identifierList}.
 * @param ctx the parse tree
 */
fn exit_identifierList(&mut self, _ctx: &IdentifierListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn enter_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn exit_identifierSeq(&mut self, _ctx: &IdentifierSeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code decimalLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code doubleLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code integerLiteral}
 * labeled alternative in {@link SnowflakeParser#number}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentAny}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentAny(&mut self, _ctx: &SnowflakeFunctionArgumentAnyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentAny}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentAny(&mut self, _ctx: &SnowflakeFunctionArgumentAnyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentArray}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentArray(&mut self, _ctx: &SnowflakeFunctionArgumentArrayContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentArray}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentArray(&mut self, _ctx: &SnowflakeFunctionArgumentArrayContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentObject}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentObject(&mut self, _ctx: &SnowflakeFunctionArgumentObjectContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentObject}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentObject(&mut self, _ctx: &SnowflakeFunctionArgumentObjectContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentMap}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentMap(&mut self, _ctx: &SnowflakeFunctionArgumentMapContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentMap}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentMap(&mut self, _ctx: &SnowflakeFunctionArgumentMapContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentVector}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentVector(&mut self, _ctx: &SnowflakeFunctionArgumentVectorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentVector}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentVector(&mut self, _ctx: &SnowflakeFunctionArgumentVectorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentFloat}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentFloat(&mut self, _ctx: &SnowflakeFunctionArgumentFloatContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentFloat}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentFloat(&mut self, _ctx: &SnowflakeFunctionArgumentFloatContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code snowflakeFunctionArgumentDefault}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn enter_snowflakeFunctionArgumentDefault(&mut self, _ctx: &SnowflakeFunctionArgumentDefaultContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code snowflakeFunctionArgumentDefault}
 * labeled alternative in {@link SnowflakeParser#snowflakeShowFunctionType}.
 * @param ctx the parse tree
 */
fn exit_snowflakeFunctionArgumentDefault(&mut self, _ctx: &SnowflakeFunctionArgumentDefaultContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArguments}.
 * @param ctx the parse tree
 */
fn enter_snowflakeShowFunctionArguments(&mut self, _ctx: &SnowflakeShowFunctionArgumentsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArguments}.
 * @param ctx the parse tree
 */
fn exit_snowflakeShowFunctionArguments(&mut self, _ctx: &SnowflakeShowFunctionArgumentsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArgumentsList}.
 * @param ctx the parse tree
 */
fn enter_snowflakeShowFunctionArgumentsList(&mut self, _ctx: &SnowflakeShowFunctionArgumentsListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#snowflakeShowFunctionArgumentsList}.
 * @param ctx the parse tree
 */
fn exit_snowflakeShowFunctionArgumentsList(&mut self, _ctx: &SnowflakeShowFunctionArgumentsListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn enter_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#strictNonReserved}.
 * @param ctx the parse tree
 */
fn exit_strictNonReserved(&mut self, _ctx: &StrictNonReservedContext<'input>) { }
/**
 * Enter a parse tree produced by {@link SnowflakeParser#nonReserved}.
 * @param ctx the parse tree
 */
fn enter_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }
/**
 * Exit a parse tree produced by {@link SnowflakeParser#nonReserved}.
 * @param ctx the parse tree
 */
fn exit_nonReserved(&mut self, _ctx: &NonReservedContext<'input>) { }

}

antlr_rust::coerce_from!{ 'input : SnowflakeListener<'input> }


