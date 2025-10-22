/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar Snowflake;


@tokenfactory{
pub type LocalTokenFactory<'input> = antlr_rust::token_factory::ArenaCommonFactory<'input>;
}

multipleStatement options {logical='Vec<Arc<LogicalPlan>>,Vec<FunctionRegistration>,Vec<Span>';}
    : statement? (SEMI_COLON statement?)* EOF
    ;

singleStatement options {logical='Arc<LogicalPlan>,Vec<FunctionRegistration>,Option<TargetName>,Properties,StatementType,Option<TableType>,Vec<CteInfo>,UpstreamEntities';}
    : statement? SEMI_COLON? EOF
    ;

standaloneExpression options {logical='Expr';}
    : expression EOF
    ;

standaloneQualifiedName options {logical='Vec<Identifier>';}
    : qualifiedName EOF
    ;

standaloneType
        options {logical='DataType,GenericDataType';}
        options {logical='Field';}
    : type_ EOF
    ;


statement options
 {logical='Arc<LogicalPlan>,Vec<FunctionRegistration>,Option<TargetName>,*Properties,StatementType,*Option<TableType>,*Vec<CteInfo>,*UpstreamEntities';}
    : <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,Vec<CteInfo>,UpstreamEntities'>
      query                                                            #statementDefault
    | USE schema=identifier                                            #use
    | USE catalog=identifier DOT schema=identifier                     #use
    | <logical='Option<TargetName>'>
      DROP SCHEMA (IF EXISTS)? qualifiedName ~SEMI_COLON*                                  #dropSchema
    | ALTER SCHEMA qualifiedName RENAME TO identifier                  #renameSchema
    | ALTER SCHEMA qualifiedName SET AUTHORIZATION principal           #setSchemaAuthorization
    | <logical='Arc<LogicalPlan>,Option<TargetName>'>
      DROP TABLE qualifiedName                                         #dropTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>'>
      DROP VIEW qualifiedName                                          #dropView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>'>
      CREATE
        (OR REPLACE)?
        EXTERNAL TABLE (IF NOT EXISTS)?
        dest=qualifiedName (IF NOT EXISTS)?
        (LPAREN externalColumnDefinition (COMMA externalColumnDefinition)* tail+=COMMA? RPAREN)?
        tail+=COMMA?
        snowflakeCreateExternalTableClauses                                             #createExternalTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)?
        ((LOCAL | GLOBAL)? (TEMP | TEMPORARY | VOLATILE | TRANSIENT))?
        TABLE (IF NOT EXISTS)?
        dest=qualifiedName (IF NOT EXISTS)?
        snowflakeCreateTableClauses
        columnAliases?
        snowflakeCreateTableClauses
        AS query
                                                                       #snowflakeCreateTableAsSelect
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY | VOLATILE | TRANSIENT)? TABLE (IF NOT EXISTS)?
      dest=qualifiedName
      CLONE src=qualifiedName
      (
        (AT | BEFORE) LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression) RPAREN
      )?
      (COPY GRANTS)?                                                   #createTableClone
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? DYNAMIC TABLE (IF NOT EXISTS)?
      dest=qualifiedName
      CLONE src=qualifiedName
      (
        (AT | BEFORE) LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression) RPAREN
      )?
      (
        TARGET_LAG EQ (lag=string | DOWNSTREAM)
        WAREHOUSE EQ warehouseName=string
      )?                                                               #createDynamicTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? EVENT TABLE (IF NOT EXISTS)?
      dest=qualifiedName
      CLONE src=qualifiedName
      (
        (AT | BEFORE) LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression) RPAREN
      )?
                                                                       #createEventTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? ICEBERG TABLE (IF NOT EXISTS)?
      dest=qualifiedName
      CLONE src=qualifiedName
      (
        (AT | BEFORE) LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression) RPAREN
      )?
      (COPY GRANTS)?                                                   #createIcebergTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE RECURSIVE TABLE (IF NOT EXISTS)?
        dest=qualifiedName columnAliases?
        (COMMENT comment=string)?
        (WITH properties)? AS (query | LPAREN query RPAREN)  #createRecursiveTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>'>
      CREATE (OR REPLACE)?
        ((LOCAL | GLOBAL)? (TEMP | TEMPORARY | VOLATILE | TRANSIENT))?
        TABLE (IF NOT EXISTS)?
        dest=qualifiedName (IF NOT EXISTS)?
        snowflakeCreateTableClauses
        LPAREN tableElements RPAREN
        snowflakeCreateTableClauses
                                                                       #snowflakeCreateTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? TABLE
        dest=qualifiedName
        USING TEMPLATE LPAREN query RPAREN                                   #snowflakeCreateTableUsingTemplate
    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      INSERT INTO dest=qualifiedName columnAliases? (
          query
        | VALUES snowflakeValueRow (COMMA snowflakeValueRow)* tail=COMMA?
        )                                                              #snowflakeInsertInto
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
        CREATE (OR REPLACE)?
        SECURE? MATERIALIZED
        VIEW (IF NOT EXISTS)?
            dest=qualifiedName (IF NOT EXISTS)?
            (COPY GRANT)?
            (LPAREN columnDefinitionForView (COMMA columnDefinitionForView)* tail+=COMMA? RPAREN)?
            (COMMENT EQ string)?
            (WITH? ROW ACCESS POLICY identifier ON LPAREN identifier (COMMA identifier)* RPAREN)?
            (WITH? TAG properties)?
            (CLUSTER BY LPAREN expression (COMMA expression)* RPAREN)?
            AS LPAREN query RPAREN                           #createMaterializedView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)?
      SECURE? ((LOCAL | GLOBAL)? (TEMP | TEMPORARY | VOLATILE))? RECURSIVE?
      VIEW (IF NOT EXISTS)?
        dest=qualifiedName (IF NOT EXISTS)?
        (LPAREN columnDefinitionForView (COMMA columnDefinitionForView)* tail+=COMMA? RPAREN)?
        (WITH? ROW ACCESS POLICY identifier ON LPAREN identifier (COMMA identifier)* RPAREN)?
        (WITH? TAG properties)?
        (COPY GRANT)?
        (COMMENT EQ comment=string)?
        AS (LPAREN query RPAREN | query )               #createView
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
     SET property                                                      #set
    | SET LPAREN identifier (COMMA identifier)* RPAREN EQ
          LPAREN expression (COMMA expression)* RPAREN                         #set

    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      (DESCRIBE | DESC) TABLE tableName=qualifiedName                       #showColumns
    | <logical='Arc<LogicalPlan>,Properties,Option<TargetName>'>
    CREATE (OR REPLACE)? (TEMP | TEMPORARY)? STAGE (IF NOT EXISTS)?
      dest=qualifiedName (IF NOT EXISTS)?
      property*
      (WITH? TAG properties )?                                         #createStage
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE JAVA
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*
      AS body=expression                                                    #createJavaFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE JAVA
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*                                                             #createJarFunction
    | CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE JAVASCRIPT
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*
      AS body=expression                                                     #createJSFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE PYTHON
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*
      AS body=expression                                                    #createPythonFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE PYTHON
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*                                                            #createModuleFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS singleReturnType=type_
      (NOT? NULL)?
      LANGUAGE SCALA
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*
      AS body=expression                                                    #createScalaFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName (IF NOT EXISTS)?
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS singleReturnType=type_
      (NOT? NULL)?
      LANGUAGE SCALA
      (CALLED ON NULL INPUT | RETURN NULL ON NULL INPUT | STRICT)?
      (VOLATILE | IMMUTABLE)?
      property*                                                              #createScalaJarFunction
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? FUNCTION
      name=qualifiedName
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      (LANGUAGE SQL)?
      (VOLATILE | IMMUTABLE)?
      MEMORIZABLE?
      property*
      AS body=expression                                                    #createSqlFunction
    | CREATE (OR REPLACE)? (TEMP | TEMPORARY)? SECURE? PROCEDURE
      name=qualifiedName
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (COPY GRANTS)?
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE PYTHON
      property*
      (AS body=expression)?                                                 #createPythonProcedure
    | WITH name=qualifiedName AS PROCEDURE
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      RETURNS (
        singleReturnType=type_
        | TABLE LPAREN tableReturnFieldName+=identifier tableReturnFieldType+=type_ (COMMA tableReturnFieldName+=identifier tableReturnFieldType+=type_)* tail=COMMA? RPAREN
      )
      (NOT? NULL)?
      LANGUAGE PYTHON
      property*
      (EXECUTE AS (OWNER | CALLER | RESTRICTED CALLER))?
       AS body=expression  
      CALL qualifiedName (LPAREN (callArgument (COMMA callArgument)*)? tail=COMMA? RPAREN)?
      (INTO ':' identifier)?                                               
                                                                            #createAnonymousPythonProcedure
    | UNSET identifier                                                      #unset

    /*
    * Catchall for all other statements
    *
    * This section must remain at the bottom!
    */
    | MERGE ~SEMI_COLON*                                                      #merge
    | ALTER ~SEMI_COLON*                                                      #alter
    | BEGIN ~SEMI_COLON*                                                      #begin
    | USE ~SEMI_COLON*                                                        #use
    | CREATE (OR REPLACE)? DATABASE ~SEMI_COLON*                              #createFoo
    | CREATE (OR REPLACE)? SEQUENCE ~SEMI_COLON*                              #createFoo
    | CREATE (OR REPLACE)? MASKING POLICY ~SEMI_COLON*                        #createFoo
    | CREATE (OR REPLACE)? SCHEMA ~SEMI_COLON*                                #createSchema
    | DROP ~SEMI_COLON*                                                       #drop
    | DELETE ~SEMI_COLON*                                                     #delete
    | TRUNCATE ~SEMI_COLON*                                                   #truncateTable
    | COMMENT ~SEMI_COLON*                                                    #comment
    | ALTER TABLE (IF EXISTS)? from=qualifiedName
        RENAME TO to=qualifiedName                                     #renameTable
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ADD COLUMN (IF NOT EXISTS)? column=columnDefinition            #addColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        RENAME COLUMN (IF EXISTS)? from=identifier TO to=identifier    #renameColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        DROP COLUMN (IF EXISTS)? column=qualifiedName                  #dropColumn
    | ALTER TABLE (IF EXISTS)? tableName=qualifiedName
        ALTER COLUMN setColumnName=identifier SET DATA TYPE type_      #setColumnType
    | ALTER TABLE tableName=qualifiedName SET AUTHORIZATION principal  #setTableAuthorization
    | ALTER TABLE tableName=qualifiedName
        SET PROPERTIES propertyAssignments                             #setTableProperties
    | ALTER TABLE tableName=qualifiedName
        EXECUTE procedureName=identifier
        (LPAREN (callArgument (COMMA callArgument)*)? tail=COMMA? RPAREN)?
        (WHERE where_=booleanExpression)?                               #tableExecute
    | ANALYZE ~SEMI_COLON*                                                     #analyze
    | REFRESH MATERIALIZED VIEW qualifiedName                          #refreshMaterializedView
    | ALTER MATERIALIZED VIEW (IF EXISTS)? from=qualifiedName
        RENAME TO to=qualifiedName                                     #renameMaterializedView
    | ALTER MATERIALIZED VIEW qualifiedName
        SET PROPERTIES propertyAssignments                             #setMaterializedViewProperties
    | ALTER VIEW from=qualifiedName RENAME TO to=qualifiedName         #renameView
    | ALTER VIEW from=qualifiedName SET AUTHORIZATION principal        #setViewAuthorization
    | CALL ~SEMI_COLON*                                                       #call
    | CREATE ROLE ~SEMI_COLON*                                                #createRole
    | GRANT ~SEMI_COLON*                                                      #grant
    | REVOKE ~SEMI_COLON*                                                     #revoke
    | DENY
        (privilege (COMMA privilege)* tail=COMMA? | ALL PRIVILEGES)
        ON (SCHEMA | TABLE)? qualifiedName
        TO grantee=principal                                           #deny
    | <logical='Option<TargetName>,Arc<LogicalPlan>,UpstreamEntities'>
      EXPLAIN (USING identifier)? statement                                    #explain
    | SHOW ~SEMI_COLON*                                                       #show
    | RESET ~SEMI_COLON*                                                      #reset
    | START TRANSACTION (transactionMode (COMMA transactionMode)* tail=COMMA?)?      #startTransaction
    | COMMIT ~SEMI_COLON*                                                     #commit
    | ROLLBACK ~SEMI_COLON*                                                   #rollback
    | PREPARE ~SEMI_COLON*                                                    #prepare
    | DEALLOCATE ~SEMI_COLON*                                                 #deallocate
    | EXECUTE ~SEMI_COLON*                                                    #execute
    | DESCRIBE INPUT identifier                                        #describeInput
    | DESCRIBE OUTPUT identifier                                       #describeOutput
    | UPDATE ~SEMI_COLON*                                                     #update

    ;

tableElements
    : tableElement (COMMA tableElement)*
    ;
unpivotNullClause
    : (INCLUDE | EXCLUDE) NULLS
    ;

snowflakeCreateTableClauses options {logical='Properties';}
    : (
    cluster+=CLUSTER BY LPAREN expression (COMMA expression)* RPAREN
    | property
    | COPY GRANTS
    | WITH? ROW ACCESS POLICY identifier ON LPAREN identifier (COMMA identifier)* RPAREN
    | WITH? TAG properties
    )*
    ;

tableConstraint
    : (CONSTRAINT identifier)?
    (
        UNIQUE columnAliases?
    | PRIMARY KEY columnAliases?
    | (FOREIGN KEY)? columnAliases? REFERENCES qualifiedName columnAliases?
    )
    constraintProperties
    ;

constraintProperties
    : (NOT? ENFORCED)? (NOT? DEFERRABLE)? (INITIALLY (DEFERRED| IMMEDIATE))? (ENABLE | DISABLE)? (VALIDATE | NOVALIDATE)? (RELY | NORELY)?
    ;

snowflakeValueRow options {logical='Vec<Expr>';}
    : LPAREN snowflakeValueItem (COMMA snowflakeValueItem )* tail=COMMA? RPAREN
    ;

snowflakeValueItem options {logical='Expr';}
    : expression
    | DEFAULT
    ;


snowflakeCreateExternalTableClauses options {logical='Properties';}
    :
    (locationSpec
    | property
    | partitionedByNameSpec
    | COPY GRANTS
    | WITH? ROW ACCESS POLICY identifier ON LPAREN identifier RPAREN
    | WITH? TAG properties
    )*
    ;


locationSpec options {logical='TableReference';}
    :
    WITH? LOCATION EQ STAGE_NAME
    ;

partitionedByNameSpec
    : PARTITION BY columnAliases
    ;


createFileFormat
    : STORED AS identifier
    ;

compressionSpec options {logical='CompressionTypeVariant';}
    : COMPRESSION TYPE compressionType=(GZIP | BZIP2 | XZ | ZSTD)
    ;

headerRowSpec
    : WITH HEADER ROW
    ;

delimiterSpec
    : DELIMITER string
    ;

query options {logical='Arc<LogicalPlan>,Vec<CteInfo>';}
    :  with? queryNoWith
    ;

with options {logical='Vec<CteInfo>';}
    : WITH RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

tableElement
    : tableConstraint
    | columnDefinition
    ;


columnDefinition options {logical='Field';}
    : fieldDefinition
    inlineConstraint?
    (NOT NULL)?
    (COLLATE string)?
    ((DEFAULT expression | (AUTOINCREMENT|IDENTITY) (number COMMA number| START number INCREMENT number)? (ORDER|NOORDER)?))?
    (WITH? MASKING POLICY identifier (USING LPAREN identifier (COMMA identifier)*RPAREN)?)?
    (WITH? TAG properties)?
    (COMMENT comment=string)?
    ;
inlineConstraint
    : (CONSTRAINT identifier)?
    (UNIQUE | PRIMARY KEY | (FOREIGN KEY)? REFERENCES qualifiedName (LPAREN identifier RPAREN)?)
    constraintProperties
    ;

columnDefinitionForView options {logical='Identifier';}
    : name=columnName
    inlineConstraint?
    (NOT NULL)?
    (COLLATE string)?
    ((DEFAULT expression | (AUTOINCREMENT|IDENTITY) (number COMMA number| START number INCREMENT number)? (ORDER|NOORDER)?))?
    (WITH? MASKING POLICY identifier (USING LPAREN identifier (COMMA identifier)*RPAREN)?)?
    (WITH? TAG properties)?
    (COMMENT string)?
    ;

externalColumnDefinition options {logical='Field';}
    : fieldDefinition
    AS expression
    inlineConstraint?
    ;
fieldDefinition options {logical='Field';}
    : columnName columnSchemaWithMetadata
    ;

columnName options {logical='Identifier';}
    : identifier
    ;

columnNameComponent options {logical='Identifier';}
    : identifier
    ;
columnSchemaWithMetadata options {logical='DataType';}
    : columnSchema (NOT? NULL)?
    ;

columnOptionList
    : columnOption (COMMA columnOption)* tail=COMMA?
    ;

columnOption
    : identifier '=' expression
    ;

columnSchema options {logical='DataType';}
    :
    type_                                  #columnSchemaSimpleType
    ;


properties options {logical='Properties';}
    : LPAREN propertyAssignments? RPAREN
    ;

propertyAssignments options {logical='Properties';}
    : property (COMMA property)* tail=COMMA?
    ;

property options {logical='Property';}
    : propertyKey EQ LPAREN property* RPAREN                               #nestedProperty
    | propertyKey EQ propertyValue                                   #defaultProperty
    ;

propertyKey options {logical='Identifier';}
    : identifier
    ;
propertyValue options {logical='PropertyValue';}
    : DEFAULT       #defaultPropertyValue
    | identifier    #identifierPropertyValue
    | expression    #expressionPropertyValue
    ;

queryNoWith options {logical='Arc<LogicalPlan>';}
    : queryLimit
    ;

queryLimit options {logical='Arc<LogicalPlan>';}
    :
    queryLimitTarget
    (LIMIT limit=limitRowCount (OFFSET offset=rowCount)?
    | (OFFSET offset2=rowCount offsetRow=rowOrRows? )?
      FETCH (FIRST | NEXT)? fetchCount=rowCount countRow=rowOrRows? ONLY?)?
    ;
queryLimitTarget options {logical='Arc<LogicalPlan>';}
    : queryTerm
      orderBy?                  #queryLimitTargetRedshiftSnowflake
    ;
rowOrRows
    : ROW
    | ROWS
    ;

limitRowCount options {logical='Option<usize>';}
    : ALL
    | NULL
    | rowCount
    ;

rowCount options {logical='usize';}
    : INTEGER_VALUE
    | QUESTION_MARK
    ;

queryTerm options {logical='Arc<LogicalPlan>';}
    : setOperation
    ;

setOperation options {logical='Arc<LogicalPlan>';}
    : left=setOperationIntersect (setOperator right+=setOperationIntersect)*
    ;

setOperator options {logical='SetOperator';}
    : UNION setQuantifier? (BY NAME)?
    | (EXCEPT | MINUS_KW) setQuantifier?
    ;
setOperationIntersect options {logical='Arc<LogicalPlan>';}
    : left=queryPrimary (setIntersectOperator right+=queryPrimary)*
    ;

setIntersectOperator options {logical='SetOperator';}
    : INTERSECT setQuantifier?
    ;

setQuantifier options {logical='Quantifier';}
    : DISTINCT
    | ALL
    ;

inlineTable options {logical='Arc<LogicalPlan>';}
    : VALUES expression (COMMA expression)* tail=COMMA?
    ;

queryPrimary options {logical='Arc<LogicalPlan>';}
    : querySpecification                   #queryPrimaryDefault
    | TABLE pathExpression                  #table
    | LPAREN query_=query RPAREN                        #subquery
    ;

sortItem options {logical='SortExpr';}
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

connectByItem
    : expression
    ;

querySpecification options {logical='Arc<LogicalPlan>';}
    : SELECT
      (TOP number)?
      setQuantifier?
      querySelectItems
      (FROM relation (COMMA relation)* tail+=COMMA?)?
      (WHERE where_=booleanExpression)?
       connectBy?
      aggregationClause?
      (HAVING having=booleanExpression)?
      (QUALIFY qualify=booleanExpression)?
    ;

connectBy
    : (START WITH expression)? CONNECT BY connectByItem (AND connectByItem)*
    ;

replaceDefinition options {logical='Expr';}
    : expression AS? identifier
    ;

querySelectItems
    : selectItem (COMMA selectItem)* tail=COMMA?
    ;

aggregationClause options {logical='GroupBy';}
    : GROUP BY groupBy
    ;

groupBy options {logical='GroupBy';}
    : ALL                                                                       #groupByAll
    | setQuantifier? groupingElement (COMMA groupingElement)* tail=COMMA?       #groupByDefault
    ;

groupingElement options {logical='Expr';}
    : ROLLUP LPAREN (expression (COMMA expression)* tail=COMMA?)? RPAREN         #rollup
    | CUBE LPAREN (expression (COMMA expression)* tail=COMMA?)? RPAREN           #cube
    | GROUPING SETS LPAREN groupingSet (COMMA groupingSet)* tail=COMMA? RPAREN   #multipleGroupingSets
    | groupingSet                                                        #singleGroupingSet
    ;

groupingSet options {logical='Vec<Expr>';}
    : LPAREN (expression (COMMA expression)*)? tail=COMMA? RPAREN
    | expression
    ;

windowDefinition
    : name=identifier AS LPAREN windowSpecification RPAREN
    ;

windowSpecification options {logical='WindowSpecification';}
    : (existingWindowName=identifier)?
      windowSpecificationPartitionBy?
      orderBy?
      windowFrame?
    ;

windowSpecificationPartitionBy
    : PARTITION BY partition+=expression (COMMA partition+=expression)* tail+=COMMA?
    ;

orderBy
    : ORDER BY sortItem (COMMA sortItem)* tail+=COMMA?
    ;
namedQuery options {logical='Arc<LogicalPlan>';}
    : name=identifier (columnAliases)? AS LPAREN query RPAREN
    ;

selectItemAlias options {logical='Identifier';}
    : columnName
    ;

selectItem options {logical='Vec<Expr>,*IntrinsicAlias';}
    : <logical='IntrinsicAlias'>
      // Snowflake support AS OVER, reject OVER
      CONNECT_BY_ROOT? expression (over_alias=OVER | AS? selectItemAlias)?  #selectSingle
    | multiSelect                                                #selectMulti
    ;

multiSelect options {logical='Vec<Expr>';}
    : selectStar
      (ILIKE string)?
      (EXCLUDE (identifierList | identifierSeq ) )?
      (REPLACE LPAREN replaceDefinition (COMMA replaceDefinition)* RPAREN )?
      (RENAME (
        from+=identifier AS to+=identifier
        | LPAREN from+=identifier AS to+=identifier (COMMA from+=identifier AS to+=identifier)* RPAREN
      ) )?
    ;

selectStar options {logical='Vec<Expr>';}
    :
      primaryExpression DOT ASTERISK
    | ASTERISK
    ;

relation options {logical='Arc<LogicalPlan>';}
    : target=joinedRelation
    ;

joinedRelation options {logical='Arc<LogicalPlan>';}
    : left=joinedRelation
      ( CROSS JOIN right=noJoinRelation
      | joinType JOIN right=noJoinRelation joinCriteria?
      // Warning: joinedRelation on JOIN rhs combined with optional joinCriteria implies grammar ambiguity. Precedence matters!
      | joinType JOIN rightJoined=joinedRelation joinCriteria?
      | NATURAL joinType JOIN right=noJoinRelation
      )
                                                            #joinRelation
    | left=joinedRelation ASOF JOIN right=noJoinRelation
      MATCH_CONDITION LPAREN expression RPAREN
      joinCriteria?                                         #asofJoinRelation
    | noJoinRelation
                                                            #relationDefault
    ;

joinType options {logical='JoinType';}
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING LPAREN identifier (COMMA identifier)* tail=COMMA? RPAREN
    ;

noJoinRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelation
    ;

sampledRelationTarget options {logical='Arc<LogicalPlan>';}
    : aliasedRelation2                                                      #aliased2
    | LPAREN VALUES expression (COMMA expression)* tail=COMMA? RPAREN
        (AS? identifier columnAliases?)?                                   #inlineTableDefault
    | VALUES expression (COMMA expression)* tail=COMMA?
        (AS? identifier columnAliases?)?                                   #inlineTableDefault
    ;

sampledRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelationTarget sampleOperator?
    ;
sampleOperator options {logical='()';}
    : (SAMPLE | TABLESAMPLE) sampleMethod
    ;

sampleMethod options {logical='()';}
    : (BERNOULLI | ROW)? LPAREN (samplePercentage | sampleCount) RPAREN sampleSeed?
    | (SYSTEM | BLOCK ) LPAREN samplePercentage RPAREN sampleSeed?
    ;
samplePercentage options {logical='()';}
    : (INTEGER_VALUE | DECIMAL_VALUE)
    ;
sampleCount options {logical='()';}
    : INTEGER_VALUE ROWS
    ;
sampleSeed
    : (REPEATABLE | SEED) LPAREN seed=INTEGER_VALUE RPAREN
    ;

trimsSpecification
    : LEADING
    | TRAILING
    | BOTH
    ;
listAggOverflowBehavior
    : ERROR
    | TRUNCATE string? listaggCountIndication
    ;

listaggCountIndication
    : WITH COUNT
    | WITHOUT COUNT
    ;

patternRecognitionTarget options {logical='Arc<LogicalPlan>';}
    :
    changesRelation
    ;

patternRecognition options {logical='Arc<LogicalPlan>';}
    : patternRecognitionTarget
    (
        MATCH_RECOGNIZE LPAREN
          (PARTITION BY partition+=expression (COMMA partition+=expression)* tail+=COMMA?)?
          (ORDER BY sortItem (COMMA sortItem)* tail+=COMMA? )?
          (MEASURES measureDefinition (COMMA measureDefinition)* tail+=COMMA?)?
          rowsPerMatch?
          (AFTER MATCH skipTo)?
          (INITIAL | SEEK)?
          PATTERN LPAREN rowPattern RPAREN
          (SUBSET subsetDefinition (COMMA subsetDefinition)* tail+=COMMA?)?
          DEFINE variableDefinition (COMMA variableDefinition)* tail+=COMMA?
        RPAREN
    )?
    ;

measureDefinition options {logical='Expr';}
    : expression AS identifier
    ;

rowsPerMatch
    : ONE ROW PER MATCH
    | ALL ROWS PER MATCH emptyMatchHandling?
    ;

emptyMatchHandling
    : SHOW EMPTY MATCHES
    | OMIT EMPTY MATCHES
    | WITH UNMATCHED ROWS
    ;

skipTo
    : SKIP_KW TO NEXT ROW
    | SKIP_KW PAST LAST ROW
    | SKIP_KW TO FIRST identifier
    | SKIP_KW TO LAST identifier
    | SKIP_KW TO identifier
    ;

subsetDefinition
    : name=identifier EQ LPAREN union+=identifier (COMMA union+=identifier)* tail=COMMA? RPAREN
    ;

variableDefinition options {logical='Identifier';}
    : identifier AS expression
    ;

atBefore
    : AT LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression | STREAM '=>' string) RPAREN
    | BEFORE LPAREN STATEMENT '=>' expression RPAREN
    ;

changeRelationTarget options {logical='Arc<LogicalPlan>';}
    : aliasedRelation                 #aliased
    | DIRECTORY LPAREN STAGE_NAME RPAREN    #directory

    ;

changesRelation options {logical='Arc<LogicalPlan>';}
    : changeRelationTarget atBefore?
    (
        CHANGES LPAREN INFORMATION '=>' (DEFAULT | APPEND_ONLY) RPAREN
        atBefore
        (END LPAREN (TIMESTAMP '=>' expression | OFFSET '=>' expression | STATEMENT '=>' expression ) RPAREN)?
    )?
    ;

pivotedRelationTarget options {logical='Arc<LogicalPlan>';}
    :
      patternRecognition                                              #pattern
    | TABLE LPAREN tableFunctionCall RPAREN                                 #tableFunctionInvocation
    ;

pivotedRelation options {logical='Arc<LogicalPlan>';}
    : pivotedRelationTarget pivotOperator*
    ;
// The reason we have to add AliasedRelation2 in the following line
// is because the snowflake AliasRelation is behind pivotRelation See https://docs.snowflake.com/en/sql-reference/constructs/from
// The wiki missed is it is also supporting alias between
// [<namespace>.]<object_name> and pivotRelation
aliasedRelation2 options {logical='Arc<LogicalPlan>';}
    : pivotedRelation ((AS alias1=identifier | alias2=strictIdentifier) columnAliases?)?
    ;

pivotAggregates options {logical='Vec<Expr>';}
    : expression
    ;

pivotFrom options {logical='Vec<Expr>';}
    : identifier
    ;

pivotInto options {logical='Expr,PivotColumnName';}
    : expression            #pivotIntoDefault
    ;

pivotAsAlias options {logical='Option<AliasAndColumnAlias>';}
    : ((AS alias1=identifier | alias2=strictIdentifier) columnAliases?)?
    ;
singleColumnUnpivot options {logical='Vec<Expr>';}
    : valuesColumn=identifier FOR nameColumn=identifier IN LPAREN columnsToUnpivot RPAREN
    ;

columnsToUnpivot options {logical='Vec<Column>,Vec<ScalarValue>';}
    : unpivotCol+=identifier (COMMA unpivotCol+=identifier)* tail=COMMA?
    ;

columnUnpivot options {logical='Vec<Expr>';}
    : singleColumnUnpivot       #singleColumnUnpivotDefault
    ;

pivotIntos options {logical='Vec<PivotColumnName>';}
    : pivotInto (COMMA pivotInto)* COMMA?      #pivotIntosDefault
    | ANY orderBy?                             #pivotIntosAny
    | query                                    #pivotIntosQuery
    ;
    
pivotOperator options {logical='Vec<Expr>,Option<AliasAndColumnAlias>';}
    : PIVOT LPAREN pivotAggregates
      FOR pivotFrom IN LPAREN pivotIntos RPAREN
      RPAREN
      // Databricks does not allow alias after PIVOT
      pivotAsAlias
                                                                            #pivot
    | UNPIVOT unpivotNullClause? LPAREN columnUnpivot
      RPAREN pivotAsAlias                                                   #unpivot
    ;

aliasedRelationTarget options {logical='Arc<LogicalPlan>';}
    : relationPrimary
    ;
aliasedRelation options {logical='Arc<LogicalPlan>';}
    : aliasedRelationTarget
    ((AS alias1=identifier | alias2=strictIdentifier) columnAliases?)?
    ;
columnAliases options {logical='Vec<Identifier>';}
    : LPAREN identifierSeq tail=COMMA? RPAREN
    ;

relationPrimary options {logical='Arc<LogicalPlan>,TableReference,*Span';}
    : LATERAL (LPAREN query RPAREN | tableFunctionCall)                     #lateral
    | <logical='Span'>
      tableNameRef=pathExpression                                     #tableName
    | TABLE LPAREN VARIABLE RPAREN                                           #tableName
    | STAGE_NAME
        (LPAREN
            stageFileSpec
            (COMMA stageFileSpec)* tail+=COMMA?
        RPAREN)?                                                         #tableName
    | STRING
        (LPAREN
            stageFileSpec
            (COMMA stageFileSpec)* tail+=COMMA?
        RPAREN)?                                                         #tableName
    | LPAREN query RPAREN                                                   #subqueryRelation
    | LPAREN rel=relation RPAREN                                                #parenthesizedRelation
    ;
tableFunctionCall options {logical='Arc<LogicalPlan>';}
    :
    VALIDATE LPAREN qualifiedName COMMA callArgument RPAREN                                  #validate
    |
    // TODO Snowflake does not accept trailing comma in table function calls: eg ```SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('{"a":1, "b":[77,88]}'), OUTER => true,))```
    functionName LPAREN (tableFunctionArgument (COMMA tableFunctionArgument)* tail+=COMMA?)?
      tableFunctionArgumentCopartition? RPAREN over?                                                  #defaultTableFunctionCall
    // Special form accepting a query as the only argument without having to wrap it in `(...)` or `TABLE(...)`
    | functionName LPAREN (tableFunctionArgumentName '=>')? subquery=queryNoWith tail+=COMMA? RPAREN           #defaultTableFunctionCall
    ;

tableFunctionArgumentCopartition
    : COPARTITION copartitionTables (COMMA copartitionTables)* tail+=COMMA?
    ;
tableFunctionArgumentName options {logical='Identifier';}
    : identifier
    // TODO by the look of https://docs.snowflake.com/en/sql-reference/functions/flatten, these are not special table function argument names
    //  and can be removed from here, as they are non-reserved.
    | OUTER
    | RECURSIVE
    ;

tableFunctionArgument options {logical='Argument';}
    : (tableFunctionArgumentName '=>')? (tableArgument | descriptorArgument | expression) // descriptor before expression to avoid parsing descriptor as a function call
    ;
tableArgument options {logical='Argument';}
    : tableArgumentRelation
        (PARTITION BY (LPAREN (expression (COMMA expression)* tail+=COMMA?)? RPAREN | expression))?
        (PRUNE WHEN EMPTY | KEEP WHEN EMPTY)?
        (ORDER BY (LPAREN sortItem (COMMA sortItem)* tail+=COMMA? RPAREN | sortItem))?
    ;
tableArgumentRelation options {logical='Argument,*Span';}
    : <logical='Span'>
      TABLE LPAREN qualifiedName RPAREN (AS? identifier columnAliases?)?  #tableArgumentTable
    | TABLE LPAREN query RPAREN (AS? identifier columnAliases?)?          #tableArgumentQuery
    ;
descriptorArgument
    : DESCRIPTOR LPAREN descriptorField (COMMA descriptorField)* tail=COMMA?RPAREN
    | CAST LPAREN NULL AS DESCRIPTOR RPAREN
    ;

descriptorField
    : identifier type_?
    ;

copartitionTables
    : LPAREN qualifiedName COMMA qualifiedName (COMMA qualifiedName)* tail=COMMA?RPAREN
    ;
expression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    : booleanExpression
    ;

booleanExpression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
    <logical='usize,f64,String,Identifier,Vec<Expr>,Argument,IntrinsicAlias'>
    left=booleanExpression pred=comparisonPredicate   #predicated
    | <logical='usize,f64,String,Identifier,Vec<Expr>,Argument,IntrinsicAlias'>
    left=nonComparisonExpression                    #defaultBooleanExpression

    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;


// workaround for https://github.com/antlr/antlr4/issues/780
comparisonPredicate options {logical='Expr,*Argument';}
    : comparisonOperator right=valueExpression  (COLLATE string)?               #comparison
    | comparisonOperator comparisonQuantifier LPAREN query RPAREN               #quantifiedComparison
    ;

nonComparisonExpression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    : left=valueExpression pred=predicate?
    ;


// workaround for https://github.com/antlr/antlr4/issues/780
predicate options {logical='Expr,*Argument';}
    :
      NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN LPAREN expression (COMMA expression)* tail=COMMA? RPAREN            #inList
    | NOT? IN LPAREN query RPAREN                                               #inSubquery
    | (LIKE | ILIKE) (ANY | ALL) pattern=valueExpression (ESCAPE escape=valueExpression)?      #likeAny
    | COLLATE string                                                    #collate
    | NOT? REGEXP valueExpression                                       #regexp
    | NOT? RLIKE string                                                 #rlike
    | NOT? (LIKE | ILIKE) pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | NOT? SIMILAR TO pattern=valueExpression (ESCAPE escape=valueExpression)?      #similarTo
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    | IS NOT? UNKNOWN                                                     #unknownPredicate
    ;
valueExpression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
      <logical='usize,f64,String,Identifier,Vec<Expr>,Argument,IntrinsicAlias'>
      primaryExpression '(+)'?                                                          #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    |
      operator=(MINUS | PLUS | POSIX) valueExpression                                   #arithmeticUnary
    |
      left=valueExpression operator=(ASTERISK | SLASH
      | PERCENT
      ) right=valueExpression                                                           #arithmeticBinary
    |
      left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    // | left=valueExpression operator=(BITWISE_SHIFT_LEFT | BITWISE_SHIFT_RIGHT) // disable >> because it is conflict with composite type defintion like array<array<int>>
    | left=valueExpression operator=BITWISE_SHIFT_LEFT
      right=valueExpression                                                             #arithmeticBinary //DataFusion only
    ;

primaryExpression options {logical='Expr,Argument,TableReference,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
      <logical='usize,f64,String'>
      constant                                                                              #constantDefault
    | '{'( string ':' expression (COMMA string ':' expression)* )? '}'                      #objectLiteral
    | <logical='Vec<Expr>'>
      LPAREN expression (COMMA expression)+ tail=COMMA? RPAREN                                    #rowConstructor
    | ROW LPAREN expression (COMMA expression)* RPAREN                                            #rowConstructor
    | POSITION LPAREN needle=valueExpression IN haystack=valueExpression RPAREN                                 #position
    | LISTAGG LPAREN setQuantifier? agg_exprs+=expression (COMMA agg_expr+=expression)? tail+=COMMA? RPAREN
        (WITHIN GROUP LPAREN ORDER BY sortItem (COMMA sortItem)* tail+=COMMA? RPAREN)?
        (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                     #listagg
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS LPAREN query RPAREN                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST LPAREN expression AS type_ RPAREN                                                     #cast
    | TRY_CAST LPAREN expression AS type_ RPAREN                                                 #cast
    | TRIM LPAREN (trimsSpecification? trimChar=valueExpression? FROM)
        trimSource=valueExpression RPAREN                                                    #trim
    | TRIM LPAREN (trimsSpecification trimChar=valueExpression? FROM?)?
        trimSource=valueExpression RPAREN                                                    #trim // only
    | TRIM LPAREN trimSource=valueExpression COMMA trimChar=valueExpression tail=COMMA? RPAREN    #trim
    | NORMALIZE LPAREN valueExpression (COMMA normalForm)? tail=COMMA? RPAREN                     #normalize
    | EXTRACT LPAREN identifier FROM valueExpression RPAREN                                     #extract
    | MINHASH LPAREN k=number COMMA ASTERISK RPAREN                                                 #minhash
    | IDENTIFIER_KW LPAREN string RPAREN                                                     #identifierExpression
    | MOD LPAREN expression COMMA expression RPAREN                                                #mod
    | DECODE LPAREN (callArgument (COMMA callArgument)*)? RPAREN                                   #decode
    | COUNT LPAREN ASTERISK RPAREN functionCallTail                                          #countStar
    |
      functionCallHead functionName
        LPAREN
        (
            (setQuantifier? callArgument (COMMA callArgument)*)? functionExtraArguments
            // Special form accepting a query as the only argument without having to wrap it in `(...)` to form a subquery expression
            | subquery=queryNoWith
        )
        tail+=COMMA?
        RPAREN
        functionCallTail                                                                     #functionCall

    | identifier over                                                                     #measure
    | <logical='Argument'>
      identifier type_? '->' expression                                                          #lambda
    | LPAREN (identifier type_? (COMMA identifier type_?)*)? tail=COMMA? RPAREN '->' expression                 #lambda
    | LPAREN query RPAREN                                                                       #subqueryExpression
    | primaryExpression '::' type_                                                        #castOperator
    | value=primaryExpression dereferenceKey                                                #valueDereference
    | <logical='TableReference,IntrinsicAlias'>
      base_=primaryExpression DOT fieldName=columnNameComponent                                     #dereference
    // TODO here base_ probably should be dereference or column name, as PRIOR should be applicabe to table field only
    | <logical='TableReference,IntrinsicAlias'>
      PRIOR base_=primaryExpression DOT fieldName=columnNameComponent                               #dereference
    | <logical='IntrinsicAlias'>
      base_=primaryExpression DOT DOLLAR INTEGER_VALUE                                       #dereferenceByPosition
    | <logical='Identifier,TableReference,Argument,IntrinsicAlias'>
      columnName                                                                          #columnReference
    | <logical='Identifier,TableReference,Argument,IntrinsicAlias'>
      PRIOR columnName                                                                    #columnReference
    | <logical='IntrinsicAlias'>
      DOLLAR INTEGER_VALUE                                                                   #columnReferenceByPosition
    | <logical='Identifier,IntrinsicAlias'>
      LPAREN expression RPAREN                                                                  #parenthesizedExpression
    | LBRACKET( expression (COMMA expression)* tail=COMMA? )? RBRACKET                                #array
    | VARIABLE                                                                            #variable
    | (ARRAY_AGG | ARRAYAGG) LPAREN setQuantifier? expression RPAREN
      (WITHIN GROUP LPAREN (ORDER BY sortItem (COMMA sortItem)*)? tail+=COMMA? RPAREN )? over?    #arrayAggFunction
    | PERCENTILE_CONT LPAREN number RPAREN
      WITHIN GROUP LPAREN ORDER BY expression (ASC|DESC)? RPAREN
      (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                       #percentileContFunction
    | PERCENTILE_DISC LPAREN number RPAREN
      WITHIN GROUP LPAREN ORDER BY expression (ASC|DESC)? RPAREN
      (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                       #percentileDiscFunction
    | (FIRST_VALUE | LAST_VALUE | LAG) LPAREN
        expression
        ((IGNORE | RESPECT) NULLS)?
      RPAREN over                                                                            #firstValueFunction
    ;

functionCallHead
    : processingMode?
    ;
functionCallTail options {logical='FunctionCallConstraint';}
    : filter? (nullTreatment? over)?
    ;
callArgument options {logical='Expr,Argument,Vec<Expr>,Arguments';}
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    | FILES '=>' string (COMMA string)*              #filesNamedFunctionArgument
    | multiSelect                   #multiArgument
    ;
functionExtraArguments options {logical='FunctionCallConstraint';}
    : (ORDER BY sortItem (COMMA sortItem)*)?
    ;
constant options {logical='Expr,usize,f64,String';}
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | <logical='usize,f64,String'>
      number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | <logical='String'>
      string                                                                                   #stringLiteral
    | BINARY_LITERAL                                                                          #binaryLiteral
    | identifier string                                                                    #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    ;

dereferenceKey options {logical='Vec<ValueDereferencePathElement>';}
    : ':' dereferenceKeyText dereferenceKeyElement*
    | LBRACKET index=valueExpression RBRACKET dereferenceKeyElement*
    ;

dereferenceKeyElement options {logical='ValueDereferencePathElement';}
    : DOT dereferenceKeyText
    | LBRACKET index=valueExpression RBRACKET
    ;

dereferenceKeyText options {logical='ValueDereferencePathElement';}
    // Not using identifier rule here, because the semantics are different (here unquoted identifier binds case-sensitively without normalization)
    : quotedIdentifier | IDENTIFIER | BACKQUOTED_IDENTIFIER | nonReserved
    | ABORT | ABSENT | ACCESS | ADD | ADMIN | AFTER | ALL | ALTER | ANALYZE | AND | ANTI | ANY | ARRAY | AS | ASC | ASOF | AT | ATTACH | AUTHORIZATION | AUTO | AUTOINCREMENT
    | BACKUP | BEFORE | BEGIN | BERNOULLI | BETWEEN | BLOCK | BOTH | BY
    | CALL | CALLED | CANCEL | CASCADE | CASE | CAST | CATALOGS | CHANGES | CHAR | CHARACTER | CLONE | CLOSE | CLUSTER | COLLATE | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | COMPOUND | COMPRESSION | CONDITIONAL | CONNECT | CONNECTION | CONSTRAINT | COPARTITION | COPY | COUNT | CREATE | CROSS | CUBE | CURRENT
    | DATA | DATABASE | DATASHARE | DAY | DEALLOCATE | DECLARE | DECODE | DEFAULT | DEFAULTS | DEFINE | DEFINER | DELETE | DELIMITED | DELIMITER | DENY | DEFERRABLE | DEFERRED | DESC | DESCRIBE | DESCRIPTOR | DIRECTORY | DISABLE | DISTINCT | DISTKEY | DISTRIBUTED | DISTSTYLE | DETACH | DOWNSTREAM | DOUBLE | DROP | DYNAMIC
    | ELSE | EMPTY | ENABLE | ENCODE | ENCODING | END | ENFORCED | ERROR | ESCAPE | EVEN | EVENT | EXCEPT | EXCLUDE | EXCLUDING | EXECUTE | EXISTS | EXPLAIN | EXTERNAL | EXTRACT
    | FALSE | FETCH | FIELDS | FILES | FILTER | FINAL | FIRST | FLOAT | FOLLOWING | FOR | FOREIGN | FORMAT | FROM | FULL | FUNCTION | FUNCTIONS
    | GENERATED | GLOBAL | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUP | GROUPING | GROUPS | GZIP
    | HAVING | HEADER | HOUR
    | ICEBERG | IDENTITY | IF | IGNORE | IMMEDIATE | IMMUTABLE | IN | INCLUDE | INCLUDING | INCREMENT | INFORMATION | INITIAL | INITIALLY | INNER | INPUT | INPUTFORMAT | INTERLEAVED | INSERT | INTERSECT | INTERVAL | INTO | INVOKER | IO | IS | ISOLATION | ILIKE
    | JAVA | JAVASCRIPT | JOIN | JSON
    | KEEP | KEY | KEYS
    | LAMBDA | LANGUAGE | LAST | LATERAL | LEADING | LEFT | LEVEL | LIBRARY | LIKE | LIMIT | LINES | LISTAGG | LOCAL | LOCATION | LOCK | LOGICAL
    | MAP | MASKING | MATCH | MATCHED | MATCHES | MATCH_CONDITION | MATERIALIZED | MAX | MEASURES | MEMORIZABLE | MERGE | MINHASH | MINUS_KW | MINUTE | MODEL | MONTH
    | NATURAL | NCHAR | NEXT | NO | NONE | NOORDER | NORELY | NORMALIZE | NOT | NOVALIDATE | NULL | NULLS
    | OBJECT | OF | OFFSET | OMIT | ON | ONE | ONLY | OPTION | OPTIONS | OR | ORDER | ORDINALITY | OUTER | OUTPUT | OUTPUTFORMAT | OVER | OVERFLOW
    | PARTITION | PARTITIONED | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERIOD | PERMUTE | PIVOT | PLACING | POLICY | POSITION | PRECEDING | PRECISION | PREPARE | PRIOR | PROCEDURE | PRIMARY | PRIVILEGES | PROPERTIES | PRUNE | PYTHON
    | QUALIFY | QUOTES
    | RANGE | READ | RECURSIVE | REGEXP | REFERENCE | REFERENCES | REFRESH | RELY | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNING | RETURNS | REVOKE | RIGHT | RLIKE | RLS | ROLE | ROLES | ROLLBACK | ROLLUP | ROW | ROWS | RUNNING
    | SAMPLE | SCALA | SCALAR | SECOND | SCHEMA | SCHEMAS | SECURE | SECURITY | SEED | SEEK | SELECT | SEMI | SEQUENCE | SERDE | SERDEPROPERTIES | SERIALIZABLE | SESSION | SET | SETS | SHOW | SIMILAR | SNAPSHOT | SOME | SORTKEY | SQL | STAGE | START | STATEMENT | STATS | STORED | STREAM | STRICT | STRUCT | SUBSET | SUBSTRING | SYSTEM
    | TABLE | TABLES | TABLESAMPLE | TAG | TEMP | TEMPLATE | TEMPORARY | TERMINATED | TEXT | THEN | TIES | TIME | TIMESTAMP | TO | TOP | TRAILING | TRANSACTION | TRANSIENT | TRIM | TRUE | TRUNCATE | TUPLE | TYPE
    | UESCAPE | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL | UNION | UNIQUE | UNKNOWN | UNLOAD | UNMATCHED | UNNEST | UNPIVOT | UNSET | UNSIGNED | UPDATE | USE | USER | USING
    | VACUUM | VALIDATE | VALUE | VALUES | VARYING | VECTOR | VERBOSE | VERSION | VIEW | VOLATILE
    | WAREHOUSE | WHEN | WHERE | WINDOW | WITH | WITHIN | WITHOUT | WORK | WRAPPER | WRITE
    | XZ
    | YEAR | YES
    | ZONE | ZSTD
    ;

functionName options {logical='Vec<Identifier>';}
    : qualifiedName | LEFT | RIGHT | IF | REPLACE | ILIKE |  INSERT | LIKE | LISTAGG | NORMALIZE | TRIM | COLLATE | PERCENTILE_CONT | PERCENTILE_DISC | REGEXP | RLIKE | GROUPING
    ;

namedParameter options {logical='Field';}
    :
    name=identifier type_ (DEFAULT expression)?
    ;

stageFileSpec
    : FILE_FORMAT '=>' (string | qualifiedName)
    | PATTERN '=>' string
    ;


field
    : expression (AS identifier)?
    ;

processingMode
    : RUNNING
    | FINAL
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string options {logical='String';}
    :
      STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    | DOLLAR_QUOTED_STRING                  #dollarQuotedStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval
    | TIME ZONE string
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

interval options {logical='Expr';}
    : INTERVAL string intervalField?
    ;
intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;
typeIdentifier options {logical='Identifier';}
    : identifier
    ;
type_ options {logical='DataType,GenericDataType,Field';}
    : nonnullableType (NOT NULL)?                                                   #typeNotNull
    | NULL                                                                          #typeNull
    ;
nonnullableType options {logical='DataType,GenericDataType';}
    : DOLLAR INTEGER_VALUE                                                             #functionSignatureGenericType // SDF yml only
    | DOUBLE PRECISION                                                              #doublePrecisionType
    | (CHARACTER | CHAR | NCHAR) VARYING (LPAREN INTEGER_VALUE RPAREN)?                                    #characterVarying
    | base_=TIMESTAMP ((WITHOUT | WITH LOCAL?) TIME ZONE)? (LPAREN precision = typeParameter RPAREN)?      #dateTimeType
    | base_=TIME (LPAREN precision = typeParameter RPAREN)?                               #dateTimeType
    | FUNCTION LPAREN type_ (COMMA type_)* RPAREN                                           #lambdaType
    // Internal use only, for defining UDTF return type in function yaml. TODO replace with OBJECT(...)
    | STRUCT LT rowField (COMMA rowField)* tail=COMMA? GT                                   #legacyStructType
    | OBJECT LPAREN rowField (COMMA rowField)* tail=COMMA? RPAREN                           #structuredObjectType
    | MAP LPAREN keyType=type_ COMMA valueType=type_ tail=COMMA? RPAREN                     #legacyMapType
    | typeIdentifier
     (LPAREN typeParameter (COMMA typeParameter)* tail=COMMA? RPAREN)?                      #primitiveType
    ;
rowField
    : identifier type_;


typeParameter options
{logical='GenericDataType,DataType,GenericInteger,u64,TimePrecision';}
    :
    INTEGER_VALUE
    | type_
    ;

whenClause options {logical='WhenClause';}
    : WHEN condition=expression THEN result=expression
    ;

filter options {logical='Expr';}
    : FILTER LPAREN WHERE booleanExpression RPAREN
    ;


over options {logical='WindowSpecification';}
    : OVER (windowName=identifier | LPAREN windowSpecification RPAREN)
    ;
windowFrame options {logical='WindowFrame';}
    :
    (MEASURES measureDefinition (COMMA measureDefinition)* tail+=COMMA?)?
      frameExtent
      (AFTER MATCH skipTo)?
      (INITIAL | SEEK)?
      (PATTERN LPAREN rowPattern RPAREN)?
      (SUBSET subsetDefinition (COMMA subsetDefinition)* tail+=COMMA?)?
      (DEFINE variableDefinition (COMMA variableDefinition)* tail+=COMMA?)?
    ;

frameExtent options {logical='WindowFrame';}
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=GROUPS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    | frameType=GROUPS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound options {logical='WindowFrameBound';}
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
    ;
rowPattern
    : patternPrimary patternQuantifier?                 #quantifiedPrimary
    | rowPattern rowPattern                             #patternConcatenation
    | rowPattern '|' rowPattern                         #patternAlternation
    ;

patternPrimary
    : identifier                                        #patternVariable
    | LPAREN RPAREN                                           #emptyPattern
    | PERMUTE LPAREN rowPattern (COMMA rowPattern)* tail=COMMA? RPAREN  #patternPermutation
    | LPAREN rowPattern RPAREN                                #groupedPattern
    | '^'                                               #partitionStartAnchor
    | DOLLAR                                               #partitionEndAnchor
    | '{-' rowPattern '-}'                              #excludedPattern
    ;

patternQuantifier
    : ASTERISK (reluctant=QUESTION_MARK)?                                                       #zeroOrMoreQuantifier
    | PLUS (reluctant=QUESTION_MARK)?                                                           #oneOrMoreQuantifier
    | QUESTION_MARK (reluctant=QUESTION_MARK)?                                                  #zeroOrOneQuantifier
    | '{' exactly=INTEGER_VALUE '}' (reluctant=QUESTION_MARK)?                                  #rangeQuantifier
    | '{' (atLeast=INTEGER_VALUE)? COMMA (atMost=INTEGER_VALUE)? tail=COMMA? '}' (reluctant=QUESTION_MARK)?   #rangeQuantifier
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

privilege
    : CREATE | SELECT | DELETE | INSERT | UPDATE
    ;
qualifiedName options {logical='TableReference,Vec<Identifier>,Identifier,PartSpans';}
    : identifier (DOT pathComponent)*                                       #qualifiedNameDefault
    | IDENTIFIER_KW LPAREN stringifiedQualifiedName=string RPAREN      #identifierFunction
    ;

pathExpression options {logical='TableReference,Expr,Vec<Identifier>,PartSpans';}
    : qualifiedName
    ;

nonquotedIdentifier
    : IDENTIFIER
    | nonReserved
    ;
dashedIdentifier options {logical='Vec<Identifier>,Identifier';}
    : nonquotedIdentifier '-' nonquotedIdentifier
    | dashedIdentifier '-' nonquotedIdentifier
    | nonquotedIdentifier '-' INTEGER_VALUE
    | dashedIdentifier '-' INTEGER_VALUE
    | nonquotedIdentifier '-' DECIMAL_VALUE nonquotedIdentifier
    | dashedIdentifier '-' DECIMAL_VALUE nonquotedIdentifier
    ;

maybeDashedIdentifier
    : nonquotedIdentifier
    | dashedIdentifier
    ;


queryPeriod
    : FOR rangeType AS OF end=valueExpression
    ;

rangeType
    : TIMESTAMP
    | VERSION
    ;

principal
    : identifier            #unspecifiedPrincipal
    | USER identifier       #userPrincipal
    | ROLE identifier       #rolePrincipal
    ;

identifier options {logical='Identifier,Option<UnquotedIdentifier>';}
    : strictIdentifier      #strictIdentifierDefault
    | strictNonReserved     #strictNonReservedIdentifier
    ;
strictIdentifier options {logical='Identifier,*Option<UnquotedIdentifier>';}
    : <logical='Option<UnquotedIdentifier>'>
      IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierDefault
    | nonReserved            #unquotedIdentifier
    |
      // Note:
      // - Snowflake does not support escaping the backtick character in backquoted identifiers
      // - Backquoted identifiers are still case-normalized
      // - The backquotes themselves are retained as part of the identifier
      // Thus, they behave same as unquoted identifiers.
      BACKQUOTED_IDENTIFIER  #unquotedIdentifier
    ;
quotedIdentifier options {logical='Identifier';}
    : QUOTED_IDENTIFIER
    ;
pathComponent options {logical='Identifier';}
    : identifier
    ;

standaloneIdentifier options {logical='Identifier';}
    : identifier EOF
    ;

identifierList options {logical='Vec<Identifier>,Vec<String>';}
    : LPAREN identifierSeq RPAREN
    ;

identifierSeq options {logical='Vec<Identifier>,Vec<String>';}
    : identifier (COMMA identifier)*
    ;

number options {logical='Expr,usize';}
    : MINUS? DECIMAL_VALUE  #decimalLiteral
    | MINUS? DOUBLE_VALUE   #doubleLiteral
    | <logical='usize'>
      MINUS? INTEGER_VALUE  #integerLiteral
    ;

// parse Snowflake show function result
snowflakeShowFunctionType options {logical='GenericDataType';}
    : ANY                                                                 #snowflakeFunctionArgumentAny
    | ROW LPAREN snowflakeShowFunctionType RPAREN                               #snowflakeFunctionArgumentArray
    | OBJECT LPAREN identifier snowflakeShowFunctionType RPAREN                 #snowflakeFunctionArgumentObject
    | MAP LPAREN snowflakeShowFunctionType COMMA snowflakeShowFunctionType RPAREN #snowflakeFunctionArgumentMap
    | VECTOR LPAREN snowflakeShowFunctionType COMMA number RPAREN                 #snowflakeFunctionArgumentVector
    | FLOAT                                                               #snowflakeFunctionArgumentFloat
    | type_                                                               #snowflakeFunctionArgumentDefault
    ;

snowflakeShowFunctionArguments options {logical='SignatureFromShowFunctions';}
    : functionName LPAREN (required+=snowflakeShowFunctionType | LBRACKET optional+=snowflakeShowFunctionType RBRACKET)? (COMMA required+=snowflakeShowFunctionType | '[,' optional+=snowflakeShowFunctionType RBRACKET)* RPAREN RETURN return_=snowflakeShowFunctionType
    ;

snowflakeShowFunctionArgumentsList options {logical='Vec<SignatureFromShowFunctions>';}
    : snowflakeShowFunctionArguments (COMMA snowflakeShowFunctionArguments)*
    ;

strictNonReserved
    : ASOF 
    | CROSS 
    | FULL 
    | INNER 
    | JOIN 
    | LATERAL
    | LEFT 
    | MATCH_CONDITION
    | NATURAL 
    | RIGHT 
    | USING
    ;
nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    :
      ABORT | ABSENT | ACCESS | ADD | ADMIN | AFTER | ANALYZE | ANTI | APPEND_ONLY | ARRAY | ARRAYAGG | ARRAY_AGG | ASC | AT | ATTACH | AUTHORIZATION | AUTO
    | AUTOINCREMENT
    | BACKUP | BEFORE | BEGIN | BERNOULLI | BLOCK | BOTH | BZIP2
    | CALL | CALLED | CALLER | CANCEL | CASCADE | CASE | CASE_INSENSITIVE | CASE_SENSITIVE | CAST | CATALOGS | CHANGES | CHAR | CHARACTER | CLONE
    | CLOSE | CLUSTER | COLLATE | COLUMNS | COMMENT | COMMIT | COMMITTED | COMPOUND | COMPRESSION | CONDITIONAL | CONNECTION | CONNECT_BY_ROOT
    | CONSTRAINT | COPARTITION | COPY | COUNT | CUBE | CURRENT_ROLE
    | DATA | DATABASE | DATASHARE | DAY | DEALLOCATE | DECLARE | DECODE | DEFAULT | DEFAULTS | DEFERRABLE | DEFERRED | DEFINE | DEFINER | DELIMITED
    | DELIMITER | DENY | DESC | DESCRIBE | DESCRIPTOR | DETACH | DIRECTORY | DISABLE | DISTKEY | DISTRIBUTED | DISTSTYLE | DOUBLE | DOWNSTREAM | DYNAMIC
    | EMPTY | ENABLE | ENCODE | ENCODING | END | ENFORCED | ERROR | ESCAPE | EVEN | EVENT | EXCEPT | EXCLUDE | EXCLUDING | EXECUTE | EXPLAIN | EXTERNAL | EXTRACT
    | FALSE | FETCH | FIELDS | FILES | FILE_FORMAT | FILTER | FINAL | FIRST | FIRST_VALUE | FLOAT | FOREIGN | FORMAT | FORMAT_NAME | FUNCTION | FUNCTIONS
    | GENERATED | GLOBAL | GRACE | GRANTED | GRANTS | GRAPHVIZ | GROUPING | GROUPS | GZIP
    | HEADER | HOUR
    | ICEBERG | IDENTIFIER_KW | IDENTITY | IF | IGNORE | IMMEDIATE | IMMUTABLE | INCLUDE | INCLUDING | INFORMATION | INITIAL | INITIALLY | INPUT
    | INPUTFORMAT | INTERLEAVED | INTERVAL | INVOKER | IO | ISOLATION
    | JAVA | JAVASCRIPT | JSON | JSON_ARRAY | JSON_EXISTS | JSON_OBJECT | JSON_QUERY | JSON_VALUE
    | KEEP | KEY | KEYS
    | LAG | LAMBDA | LANGUAGE | LAST | LAST_VALUE | LEADING | LEVEL | LIBRARY | LIMIT | LINES | LISTAGG | LOCAL | LOCATION | LOCK | LOGICAL
    | MAP | MASKING | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MAX | MEASURES | MEMORIZABLE | MERGE | MINHASH | MINUTE | MOD | MODEL | MONTH
    | NAME | NCHAR | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NOORDER | NORELY | NORMALIZE | NOVALIDATE | NULLS
    | OBJECT | OFFSET | OMIT | ONE | ONLY | OPTION | OPTIONS | ORDINALITY | OUTER | OUTPUT | OUTPUTFORMAT | OVER | OVERFLOW | OWNER
    | PARTITION | PARTITIONED | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERCENTILE_CONT | PERCENTILE_DISC | PERIOD | PERMUTE | PIVOT | PLACING | POLICY | POSITION
    | PRECEDING | PRECISION | PREPARE | PRIMARY | PRIOR | PRIVILEGES | PROCEDURE | PROPERTIES | PRUNE | PYTHON
    | QUOTES
    | RANGE | READ | RECURSIVE | REFERENCE | REFERENCES | REFRESH | RELY | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RESTRICTED | RETURN | RETURNING | RETURNS | RLS
    | ROLE | ROLES | ROLLBACK | ROLLUP | RUNNING
    | SCALA | SCALAR | SCHEMA | SCHEMAS | SECOND | SECURE | SECURITY | SEED | SEEK | SEMI | SEQUENCE | SERDE | SERDEPROPERTIES | SERIALIZABLE | SESSION | SETS | SHOW
    | SIMILAR | SKIP_KW | SNAPSHOT | SORTKEY | SQL | STAGE | STATEMENT | STATS | STORED | STREAM | STRICT | STRING_KW | STRUCT | SUBSET | SUBSTRING | SYSTEM | SYSTEM_TIME
    | TABLES | TAG | TARGET_LAG | TEMP | TEMPLATE | TEMPORARY | TERMINATED | TEXT | TIES | TIME | TIMESTAMP | TOP | TRAILING
    | TRANSACTION | TRANSIENT | TRIM | TRUE | TRUNCATE | TRY_CAST | TUPLE | TYPE
    | UESCAPE | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL | UNKNOWN | UNLOAD | UNMATCHED | UNNEST | UNPIVOT | UNSET | UNSIGNED | USE | USER | UTF16 | UTF32 | UTF8
    | VACUUM | VALIDATE | VALUE | VARYING | VECTOR | VERBOSE | VERSION | VIEW | VOLATILE
    | WAREHOUSE | WHEN | WINDOW | WITHIN | WITHOUT | WORK | WRAPPER | WRITE
    | XZ
    | YEAR | YES
    | ZONE | ZSTD
    ;

ABORT: 'ABORT';
ABSENT: 'ABSENT';
ACCESS: 'ACCESS';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANTI: 'ANTI';
ANY: 'ANY';
APPEND_ONLY: 'APPEND_ONLY';
ARRAY: 'ARRAY';
ARRAYAGG: 'ARRAYAGG';
ARRAY_AGG: 'ARRAY_AGG';
AS: 'AS';
ASC: 'ASC';
ASOF: 'ASOF';
AT: 'AT';
ATTACH: 'ATTACH';
AUTHORIZATION: 'AUTHORIZATION';
AUTO: 'AUTO';
AUTOINCREMENT: 'AUTOINCREMENT';
BACKUP: 'BACKUP';
BEFORE: 'BEFORE';
BEGIN: 'BEGIN';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BLOCK: 'BLOCK';
BOTH: 'BOTH';
BY: 'BY';
BZIP2: 'BZIP2';
CALL: 'CALL';
CALLED: 'CALLED';
CALLER: 'CALLER';
CANCEL: 'CANCEL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CASE_SENSITIVE: 'CASE_SENSITIVE';
CASE_INSENSITIVE: 'CASE_INSENSITIVE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
CHANGES: 'CHANGES';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
CLONE: 'CLONE';
CLOSE: 'CLOSE';
CLUSTER: 'CLUSTER';
COLLATE: 'COLLATE';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMA: ',';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPOUND: 'COMPOUND';
COMPRESSION: 'COMPRESSION';
CONDITIONAL: 'CONDITIONAL';
CONNECT: 'CONNECT';
CONNECTION: 'CONNECTION';
CONNECT_BY_ROOT: 'CONNECT_BY_ROOT';
CONSTRAINT: 'CONSTRAINT';
COPARTITION: 'COPARTITION';
COPY: 'COPY';
COUNT: 'COUNT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_ROLE: 'CURRENT_ROLE';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATASHARE: 'DATASHARE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DECLARE: 'DECLARE';
DECODE: 'DECODE';
DEFAULT: 'DEFAULT';
DEFAULTS: 'DEFAULTS';
DEFINE: 'DEFINE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DELIMITED: 'DELIMITED';
DELIMITER: 'DELIMITER';
DENY: 'DENY';
DEFERRABLE: 'DEFERRABLE';
DEFERRED: 'DEFERRED';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DESCRIPTOR: 'DESCRIPTOR';
DIRECTORY: 'DIRECTORY';
DISABLE: 'DISABLE';
DISTINCT: 'DISTINCT';
DISTKEY: 'DISTKEY';
DISTRIBUTED: 'DISTRIBUTED';
DISTSTYLE: 'DISTSTYLE';
DETACH: 'DETACH';
DOWNSTREAM: 'DOWNSTREAM';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
DYNAMIC: 'DYNAMIC';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
ENABLE: 'ENABLE';
ENCODE: 'ENCODE';
ENCODING: 'ENCODING';
END: 'END';
ENFORCED: 'ENFORCED';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EVEN: 'EVEN';
EVENT: 'EVENT';
EXCEPT: 'EXCEPT';
EXCLUDE: 'EXCLUDE';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FETCH: 'FETCH';
FIELDS: 'FIELDS';
FILE_FORMAT: 'FILE_FORMAT';
FILES: 'FILES';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FIRST_VALUE: 'FIRST_VALUE';
FLOAT: 'FLOAT';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FOREIGN: 'FOREIGN';
FORMAT: 'FORMAT';
FORMAT_NAME: 'FORMAT_NAME';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GENERATED: 'GENERATED';
GLOBAL: 'GLOBAL';
GRACE: 'GRACE';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
GROUPS: 'GROUPS';
GZIP: 'GZIP';
HAVING: 'HAVING';
HEADER: 'HEADER';
HOUR: 'HOUR';
ICEBERG: 'ICEBERG';
IDENTIFIER_KW: 'IDENTIFIER';
IDENTITY: 'IDENTITY';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IMMUTABLE: 'IMMUTABLE';
IN: 'IN';
INCLUDE: 'INCLUDE';
INCLUDING: 'INCLUDING';
INCREMENT: 'INCREMENT';
INFORMATION: 'INFORMATION';
INITIAL: 'INITIAL';
INITIALLY: 'INITIALLY';
INNER: 'INNER';
INPUT: 'INPUT';
INPUTFORMAT: 'INPUTFORMAT';
INTERLEAVED: 'INTERLEAVED';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
ILIKE: 'ILIKE';
JAVA: 'JAVA';
JAVASCRIPT: 'JAVASCRIPT';
JOIN: 'JOIN';
JSON: 'JSON';
JSON_ARRAY: 'JSON_ARRAY';
JSON_EXISTS: 'JSON_EXISTS';
JSON_OBJECT: 'JSON_OBJECT';
JSON_QUERY: 'JSON_QUERY';
JSON_VALUE: 'JSON_VALUE';
KEEP: 'KEEP';
KEY: 'KEY';
KEYS: 'KEYS';
LAG: 'LAG';
LAMBDA: 'LAMBDA';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LAST_VALUE: 'LAST_VALUE';
LATERAL: 'LATERAL';
LEADING: 'LEADING';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIBRARY: 'LIBRARY';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LISTAGG: 'LISTAGG';
LOCAL: 'LOCAL';
LOCATION: 'LOCATION';
LOCK: 'LOCK';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MASKING: 'MASKING';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_CONDITION: 'MATCH_CONDITION';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MAX: 'MAX';
MEASURES: 'MEASURES';
MEMORIZABLE: 'MEMORIZABLE';
MERGE: 'MERGE';
MINHASH: 'MINHASH';
MINUS_KW:'MINUS';
MINUTE: 'MINUTE';
MOD: 'MOD';
MODEL: 'MODEL';
MONTH: 'MONTH';
NAME: 'NAME';
NATURAL: 'NATURAL';
NCHAR: 'NCHAR';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NOORDER: 'NOORDER';
NORELY: 'NORELY';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NOVALIDATE: 'NOVALIDATE';
NULL: 'NULL';
NULLS: 'NULLS';
OBJECT: 'OBJECT';
OF: 'OF';
OFFSET: 'OFFSET';
OMIT: 'OMIT';
ON: 'ON';
ONE: 'ONE';
ONLY: 'ONLY';
OPTION: 'OPTION';
OPTIONS: 'OPTIONS';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OUTPUTFORMAT: 'OUTPUTFORMAT';
OVER: 'OVER';
OVERFLOW: 'OVERFLOW';
OWNER: 'OWNER';
PARTITION: 'PARTITION';
PARTITIONED: 'PARTITIONED';
PARTITIONS: 'PARTITIONS';
PASSING: 'PASSING';
PAST: 'PAST';
PATH: 'PATH';
PATTERN: 'PATTERN';
PER: 'PER';
PERCENTILE_CONT: 'PERCENTILE_CONT';
PERCENTILE_DISC: 'PERCENTILE_DISC';
PERIOD: 'PERIOD';
PERMUTE: 'PERMUTE';
PIVOT: 'PIVOT';
PLACING: 'PLACING';
POLICY: 'POLICY';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PRIOR: 'PRIOR';
PROCEDURE: 'PROCEDURE';
PRIMARY: 'PRIMARY';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
PYTHON: 'PYTHON';
QUALIFY: 'QUALIFY';
QUOTES: 'QUOTES';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
REGEXP: 'REGEXP';
REFERENCE: 'REFERENCE';
REFERENCES: 'REFERENCES';
REFRESH: 'REFRESH';
RELY: 'RELY';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RESTRICTED: 'RESTRICTED';
RETURN: 'RETURN';
RETURNING: 'RETURNING';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
RLS: 'RLS';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
RUNNING: 'RUNNING';
SAMPLE: 'SAMPLE';
SCALA: 'SCALA';
SCALAR: 'SCALAR';
SECOND: 'SECOND';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECURE: 'SECURE';
SECURITY: 'SECURITY';
SEED: 'SEED';
SEEK: 'SEEK';
SELECT: 'SELECT';
SEMI: 'SEMI';
SEQUENCE: 'SEQUENCE';
SERDE: 'SERDE';
SERDEPROPERTIES: 'SERDEPROPERTIES';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SIMILAR: 'SIMILAR';
// Note: 'SKIP' is a reserved word in ANTLR itself
SKIP_KW: 'SKIP';
SNAPSHOT: 'SNAPSHOT';
SOME: 'SOME';
SORTKEY: 'SORTKEY';
SQL: 'SQL';
STAGE: 'STAGE';
START: 'START';
STATEMENT: 'STATEMENT';
STATS: 'STATS';
STORED: 'STORED';
STREAM: 'STREAM';
STRICT: 'STRICT';
STRUCT: 'STRUCT';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
SYSTEM_TIME: 'SYSTEM_TIME';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TAG: 'TAG';
TEMP: 'TEMP';
TEMPLATE: 'TEMPLATE';
TEMPORARY: 'TEMPORARY';
TERMINATED: 'TERMINATED';
TEXT: 'TEXT';
STRING_KW: 'STRING';
THEN: 'THEN';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TOP: 'TOP';
TRAILING: 'TRAILING';
TARGET_LAG: 'TARGET_LAG';
TRANSACTION: 'TRANSACTION';
TRANSIENT: 'TRANSIENT';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TUPLE: 'TUPLE';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNCONDITIONAL: 'UNCONDITIONAL';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNKNOWN: 'UNKNOWN';
UNLOAD: 'UNLOAD';
UNMATCHED: 'UNMATCHED';
UNNEST: 'UNNEST';
UNPIVOT: 'UNPIVOT';
UNSET: 'UNSET';
UNSIGNED: 'UNSIGNED';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
UTF16: 'UTF16';
UTF32: 'UTF32';
UTF8: 'UTF8';
VACUUM: 'VACUUM';
VALIDATE: 'VALIDATE';
VALUE: 'VALUE';
VALUES: 'VALUES';
VARYING: 'VARYING';
VECTOR: 'VECTOR';
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
VOLATILE: 'VOLATILE';
WAREHOUSE: 'WAREHOUSE';
WHEN: 'WHEN';
WHERE: 'WHERE';
WINDOW: 'WINDOW';
WITH: 'WITH';
WITHIN: 'WITHIN';
WITHOUT: 'WITHOUT';
WORK: 'WORK';
WRAPPER: 'WRAPPER';
WRITE: 'WRITE';
XZ: 'XZ';
YEAR: 'YEAR';
YES: 'YES';
ZONE: 'ZONE';
ZSTD: 'ZSTD';

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
DOT: '.';
EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';
QUESTION_MARK: '?';
SEMI_COLON: ';';
COLON: ':';
DOLLAR: '$';

BITWISE_SHIFT_LEFT: '<<';
POSIX: '~';

ESCAPE_SEQUENCE:
	'\\' .
	;

STRING
    : '\'' (~('\'' | '\\' ) | ESCAPE_SEQUENCE | '\'\'' )* '\''
    ;
UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;
DOLLAR_QUOTED_STRING
    : '$$' .*? '$$'
    ;


// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :
      'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;
IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | DOLLAR)*
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' (LETTER | DIGIT | '_' | '-' | '.' | DOLLAR)+ '`'
    ;

STAGE_NAME
    : '@' (DIGIT | LETTER | '_' | '-' | '/' | '.' | '=')+
    ;

VARIABLE
    : DOLLAR IDENTIFIER
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;
SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

SLASH_SLASH_COMMENT
    : '//' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;
BRACKETED_COMMENT
    : '/*' ( BRACKETED_COMMENT | . )*? '*/'      -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for unpaired tokens. Used to process partial inputs.
UNPAIRED_TOKEN
    : '/*'
    | '"'
    | '\''
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
