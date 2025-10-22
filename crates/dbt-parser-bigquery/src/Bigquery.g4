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

grammar Bigquery;


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

statementBlock
    : statement (SEMI_COLON statement)* SEMI_COLON
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
        dest=maybeDashedPathExpression
        (LPAREN tableElements tail+=COMMA? RPAREN)?
        connectionSpec?
        (WITH PARTITION COLUMNS (partitionColumns)?)?
        OPTIONS properties                                             #bigqueryCreateExternalTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE
        (OR REPLACE)? (TEMPORARY | TEMP)?
        TABLE (IF NOT EXISTS)?
        dest=maybeDashedPathExpression
        (((LIKE|COPY) source=maybeDashedPathExpression)
        |(CLONE source=maybeDashedPathExpression (FOR SYSTEM_TIME AS OF expression)?)
        )?
        (LPAREN tableElements tail=COMMA? RPAREN)?
        (DEFAULT COLLATE string)?
        (PARTITION BY expression)?
        (CLUSTER BY identifier (COMMA identifier)*)?
        (OPTIONS properties)?
        (AS query)?                                                    #bigqueryCreateTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>'>
      CREATE SNAPSHOT TABLE (IF NOT EXISTS)? dest=maybeDashedPathExpression
        CLONE source=maybeDashedPathExpression
        (FOR SYSTEM_TIME AS OF expression)?
        (OPTIONS properties)?                                          #createSnapshotTable // bigquery only
    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      INSERT INTO? dest=maybeDashedPathExpression columnAliases? query                  #insertInto
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)?
        dest=maybeDashedPathExpression
        (PARTITION BY expression)?
        (CLUSTER BY identifier (COMMA identifier)*)?
        (OPTIONS properties)?
        AS query                                    #bigqueryCreateMaterializedView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? VIEW
        (IF NOT EXISTS)?
        dest=maybeDashedPathExpression
        columnAliases?
        (OPTIONS properties)?
        AS query                                                       #bigqueryCreateView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      (DESCRIBE | DESC) tableName=qualifiedName                             #showColumns
    | SHOW COLUMNS FROM tableName=qualifiedName                             #showColumns

    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      MERGE INTO? name=qualifiedName (AS? identifier)?
      USING aliasedRelation
      ON booleanExpression
      mergeCase+                                                       #merge
    // Functions:
    | <logical='Vec<FunctionRegistration>,Option<TargetName>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      (RETURNS type_)?
      AS body=expression
      (OPTIONS LPAREN columnOptionList RPAREN)?                                #createSqlFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? (TEMP | TEMPORARY)? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      RETURNS type_
      (DETERMINISTIC | NOT DETERMINISTIC)?
      LANGUAGE identifier
      (
        (   (OPTIONS LPAREN columnOptionList RPAREN)? AS body=string)
          | (AS body=string (OPTIONS LPAREN columnOptionList RPAREN)? )
      )                                                                #createJSFunction
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? FUNCTION (IF NOT EXISTS)?
      name=qualifiedName
      LPAREN ( namedParameter (COMMA namedParameter )* tail=COMMA? )? RPAREN
      RETURNS type_
      REMOTE connectionSpec
      (OPTIONS LPAREN columnOptionList RPAREN)?                              #createRemoteFunction

    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? MODEL (IF NOT EXISTS)?
        dest=maybeDashedPathExpression
        (TRANSFORM querySelectItems)?
        (INPUT identifier type_ OUTPUT identifier type_)?
        (REMOTE WITH CONNECTION '`' identifier '`')?
        (OPTIONS properties)?
        AS (
            query
            | TRAINING_DATA AS LPAREN query RPAREN
            | CUSTOM_HOLIDAY AS LPAREN query RPAREN
        )                                                              #bigqueryCreateModel

    // Procedural (https://cloud.google.com/bigquery/docs/reference/standard-sql/procedural-language):

    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      DECLARE identifier (COMMA identifier)*
              (type_)? (DEFAULT expression)?                           #declare
    | EXECUTE IMMEDIATE expression
        (INTO identifier (COMMA identifier)* )?
        (USING identifier (COMMA identifier)* )?                           #executeImmediate
    | BEGIN statementBlock END                                         #begin
    | BEGIN statementBlock EXCEPTION WHEN ERROR THEN statementBlock END     #beginException
    | CASE (expression)?
        (WHEN booleanExpression THEN statementBlock)+
        (ELSE statementBlock)?
      END CASE                                                         #case
    | IF booleanExpression THEN statementBlock
        (ELSEIF booleanExpression THEN statementBlock)*
        (ELSE statementBlock)?
      END IF                                                           #if
    | LOOP statementBlock END LOOP                                     #loop
    | REPEAT statementBlock UNTIL booleanExpression END REPEAT         #repeat
    | WHILE booleanExpression DO statementBlock END WHILE              #while
    | BREAK                                                            #break
    | LEAVE                                                           #leave
    | CONTINUE                                                        #continue
    | ITERATE                                                          #continue
    | FOR identifier IN LPAREN query RPAREN
        DO statementBlock
      END FOR                                                          #forIn
    | RAISE (USING MESSAGE EQ string)?                                  #raise
    | RETURN                                                           #return
    | BEGIN (TRANSACTION)?                                             #beginTransaction
    | COMMIT (TRANSACTION)?                                            #commitTransaction
    | ROLLBACK (TRANSACTION)?                                          #rollbackTransaction

    /*
    * Catchall for all other statements
    *
    * This section must remain at the bottom!
    */
    | SET ~SEMI_COLON*                                                        #set
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
namedExpressionSeq options {logical='Vec<Expr>';}
    : namedExpression (COMMA namedExpression)*
    ;

namedExpression options {logical='Expr';}
    : expression (AS? (name=identifier | identifierList))?
    ;

unpivotNullClause
    : (INCLUDE | EXCLUDE) NULLS
    ;



locationSpec
    :
    LOCATION string
    ;

connectionSpec
    : WITH CONNECTION qualifiedName
    ;

query options {logical='Arc<LogicalPlan>,Vec<CteInfo>';}
    :  with? queryNoWith
    ;

with options {logical='Vec<CteInfo>';}
    : WITH RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

tableElement
    : columnDefinition
    ;

columnDefinition options {logical='Field';}
    : fieldDefinition
    (WITH properties)?
    ;
fieldDefinition options {logical='Field';}
    : columnName columnSchemaWithMetadata
    ;

columnName options {logical='Identifier';}
    : identifier
    ;

columnNameComponent options {logical='Identifier';}
    : pathComponent
    ;
columnSchemaWithMetadata options {logical='DataType';}
    : columnSchema (NOT? NULL)?
    (OPTIONS LPAREN columnOptionList? RPAREN)?
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
    | structDefinition                     #columnSchemaStruct
    | ARRAY LT arrayElementSchema GT     #columnSchemaArray
    ;

fieldList
    : fieldDefinition (COMMA fieldDefinition)* tail=COMMA?
    ;

arrayElementSchema options {logical='DataType';}
    : type_
    | structDefinition (NOT NULL)?
    ;

structDefinition options {logical='DataType';}
    : STRUCT LT fieldList GT
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
    : queryLimitTarget
      (LIMIT limit=limitRowCount (OFFSET offset=rowCount)?)?
    ;
queryLimitTarget options {logical='Arc<LogicalPlan>';}
    : queryTerm
      orderBy?                  #queryLimitTargetDefault
    ;
limitRowCount options {logical='Option<usize>';}
    : rowCount
    ;

rowCount options {logical='usize';}
    : INTEGER_VALUE
    ;

queryTerm options {logical='Arc<LogicalPlan>';}
    : setOperation
    ;

setOperation options {logical='Arc<LogicalPlan>';}
    : left=setOperationIntersect (setOperator right+=setOperationIntersect)*
    ;

setOperator options {logical='SetOperator';}
    // TODO support "UNION ... CORRESPONDING" syntax
    : (INNER | (LEFT | FULL)? OUTER?) (UNION | EXCEPT | INTERSECT) setQuantifier (BY NAME)?
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
    | inlineTable                          #inlineTableDefault1
    | LPAREN query_=query RPAREN                        #subquery
    ;

sortItem options {logical='SortExpr';}
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification options {logical='Arc<LogicalPlan>';}
    : SELECT
      setQuantifier?
      (AS (STRUCT|VALUE))? // Bigquery only
      querySelectItems
      (FROM relation)?
      (WHERE where_=booleanExpression)?
      aggregationClause?
      (HAVING having=booleanExpression)?
      (QUALIFY qualify=booleanExpression)?
      (WINDOW windowDefinition (COMMA windowDefinition)* tail+=COMMA?)?
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
    : name=identifier AS LPAREN query RPAREN
    ;

selectItemAlias options {logical='Identifier';}
    : columnName
    ;

selectItem options {logical='Vec<Expr>,*IntrinsicAlias';}
    : <logical='IntrinsicAlias'>
      expression (AS? selectItemAlias)?                          #selectSingle
    | multiSelect                                                #selectMulti
    ;

multiSelect options {logical='Vec<Expr>';}
    : selectStar
      (EXCEPT identifierList )?
      (REPLACE LPAREN replaceDefinition (COMMA replaceDefinition)* RPAREN )?
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
        // TODO joinCriteria on CROSS JOIN is allowed only when joining with UNNEST
        joinCriteria?
      | COMMA right=noJoinRelation
      | joinType JOIN right=noJoinRelation
        // TODO joinCriteria is required unless joining with UNNEST
        joinCriteria?
      | NATURAL joinType JOIN right=noJoinRelation
      )
                                                            #joinRelation
    | noJoinRelation
                                                            #relationDefault
    ;

noJoinRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelation
    // TODO this incorrectly allows `.. FROM (t1) JOIN t2` and `.. FROM (t1, t2), t3`
    | LPAREN joinedRelation RPAREN
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

sampledRelationTarget options {logical='Arc<LogicalPlan>';}
    : target=pivotedRelation
    ;

sampledRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelationTarget sampleOperator?
    ;
sampleOperator
    : TABLESAMPLE sampleMethod LPAREN percentage=expression PERCENT_KW RPAREN
    ;
sampleMethod
    : BERNOULLI
    | SYSTEM
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


variableDefinition options {logical='Identifier';}
    : identifier AS expression
    ;

pivotedRelationTarget options {logical='Arc<LogicalPlan>';}
    : target=relationPrimary
    ;

pivotedRelation options {logical='Arc<LogicalPlan>';}
    : pivotedRelationTarget pivotOperator*
    ;
pivotAggregates options {logical='Vec<Expr>';}
    : namedExpressionSeq
    ;

pivotFrom options {logical='Vec<Expr>';}
    : expression
    ;

pivotInto options {logical='Expr,PivotColumnName';}
    : namedExpression       #pivotIntoNamedExpression
    ;

pivotAsAlias options {logical='Option<AliasAndColumnAlias>';}
    : (AS? alias=identifier)?
    ;

singleColumnUnpivot options {logical='Vec<Expr>';}
    : valuesColumn=identifier FOR nameColumn=identifier IN LPAREN columnsToUnpivot RPAREN
    ;

columnsToUnpivot options {logical='Vec<Column>,Vec<ScalarValue>';}
    : unpivotCol+=identifier (AS? unpivotAlias)? (COMMA unpivotCol+=identifier (AS? unpivotAlias)?)* tail=COMMA?
    ;

unpivotAlias options {logical='ScalarValue';}
    : number | string
    ;

multiColumnUnpivot options {logical='Vec<Expr>';}
    : valueColumnSet FOR nameColumn=identifier IN LPAREN columnSetsToUnpivot RPAREN
    ;

valueColumnSet options {logical='Vec<Identifier>';}
    : LPAREN valueColumn+=identifier (COMMA valueColumn+=identifier)* tail=COMMA? RPAREN
    ;

unpivotColumnSet options {logical='Vec<Column>,ScalarValue';}
    : LPAREN unpivotColumns+=identifier (COMMA unpivotColumns+=identifier)* RPAREN (AS? unpivotAlias)?
    ;

columnSetsToUnpivot options {logical='Vec<Column>,Vec<ScalarValue>';}
    : unpivotColumnSet (COMMA unpivotColumnSet)* COMMA?
    ;

columnUnpivot options {logical='Vec<Expr>';}
    : singleColumnUnpivot       #singleColumnUnpivotDefault
    | multiColumnUnpivot        #multiColumnUnpivotDefault
    ;

pivotIntos options {logical='Vec<PivotColumnName>';}
    : pivotInto (COMMA pivotInto)* COMMA?      #pivotIntosDefault
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

aliasedRelationTarget options {logical='Arc<LogicalPlan>,TableReference,*Span';}
    : <logical='Span'>
    tableNameRef=maybeDashedPathExpression       #tableName
    | LPAREN query RPAREN                        #subqueryRelation
    ;

aliasedRelation options {logical='Arc<LogicalPlan>';}
    : aliasedRelationTarget
    (AS? alias=identifier)?
    ;

columnAliases options {logical='Vec<Identifier>';}
    : identifier
        (OPTIONS properties)?
    | LPAREN identifier
        (OPTIONS properties)?
        (COMMA identifier
            (OPTIONS properties)?
        )* tail=COMMA? RPAREN
    ;

partitionColumn options {logical='Field';}
    : identifier type_
    ;

partitionColumns options {logical='Vec<Field>';}
    : LPAREN partitionColumn (COMMA partitionColumn)* tail=COMMA? RPAREN
    ;

relationPrimary options {logical='Arc<LogicalPlan>,TableReference,*Span';}
    : <logical='Span'>
    aliasedRelation                                                                 #aliased
    | UNNEST LPAREN expression (COMMA expression)* tail=COMMA? RPAREN
      (AS? alias1=identifier)? (WITH OFFSET (AS? alias2=identifier)?)?               #unnest
    | tableFunctionCall                                                              #tableFunctionInvocation
    ;
tableFunctionCall options {logical='Arc<LogicalPlan>';}
    :
    functionName LPAREN (tableFunctionArgument (COMMA tableFunctionArgument)* tail+=COMMA?)?
      tableFunctionArgumentCopartition? RPAREN over?                                                  #defaultTableFunctionCall
    ;

tableFunctionArgumentCopartition
    : COPARTITION copartitionTables (COMMA copartitionTables)* tail+=COMMA?
    ;
tableFunctionArgumentName options {logical='Identifier';}
    : identifier
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
      TABLE qualifiedName                                                 #tableArgumentTable
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
    left=valueExpression pred=predicate?              #predicated

    | NOT booleanExpression                             #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;



// workaround for https://github.com/antlr/antlr4/issues/780
predicate options {logical='Expr,*Argument';}
    :
      comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier LPAREN query RPAREN               #quantifiedComparison
    |
      NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN LPAREN expression (COMMA expression)* tail=COMMA? RPAREN            #inList
    | NOT? IN UNNEST valueExpression                                      #inUnnest
    | NOT? IN LPAREN query RPAREN                                               #inSubquery
    | NOT? LIKE ( ALL | ANY | SOME ) LPAREN pattern+=valueExpression (COMMA pattern+=valueExpression)* RPAREN                  #quantifiedLike
    | NOT? LIKE pattern=valueExpression                                 #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    | IS NOT? TRUE                                                        #truePredicate
    | IS NOT? FALSE                                                       #falsePredicate
    | IS NOT? UNKNOWN                                                     #unknownPredicate
    ;
valueExpression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
      <logical='usize,f64,String,Identifier,Vec<Expr>,Argument,IntrinsicAlias'>
      primaryExpression                                                                 #valueExpressionDefault
    |
      operator=(MINUS | PLUS | POSIX) valueExpression                                   #arithmeticUnary
    |
      left=valueExpression operator=(ASTERISK | SLASH
      ) right=valueExpression                                                           #arithmeticBinary
    |
      left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    // | left=valueExpression operator=(BITWISE_SHIFT_LEFT | BITWISE_SHIFT_RIGHT) // disable >> because it is conflict with composite type defintion like array<array<int>>
    | left=valueExpression operator=BITWISE_SHIFT_LEFT
      right=valueExpression                                                             #arithmeticBinary //DataFusion only
    | left=valueExpression operator=(BITWISE_AND | BITWISE_OR | BITWISE_XOR)
      right=valueExpression                                                             #arithmeticBinary
    ;

primaryExpression options {logical='Expr,Argument,TableReference,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
    NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | <logical='usize,f64,String'>
      number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | <logical='String'>
      string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    /* Unfortunately this does not work due to bug in Antlr-rust */
    // | BINARY_TRIPLE_QUOTED_LITERAL                                                        #binaryLiteral
    | identifier string                                                                    #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    | <logical='Vec<Expr>'>
      LPAREN expression (COMMA expression)+ tail=COMMA? RPAREN                                    #rowConstructor
    | STRUCT
      ( LT rowField (COMMA rowField)* GT )?
      LPAREN (field (COMMA field )*)? RPAREN                                                        #structConstructor
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS LPAREN query RPAREN                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | <logical='IntrinsicAlias'>
      CAST LPAREN expression AS type_ (FORMAT string)? RPAREN                                    #cast
    | SAFE_CAST LPAREN expression AS type_ (FORMAT string)? RPAREN                               #cast
    | TRIM LPAREN (trimsSpecification? trimChar=valueExpression? FROM)
        trimSource=valueExpression RPAREN                                                    #trim
    | TRIM LPAREN (trimsSpecification trimChar=valueExpression? FROM?)?
        trimSource=valueExpression RPAREN                                                    #trim // only
    | TRIM LPAREN trimSource=valueExpression COMMA trimChar=valueExpression tail=COMMA? RPAREN    #trim
    | SUBSTRING LPAREN valueExpression FROM valueExpression (FOR valueExpression)? RPAREN       #substring
    | NORMALIZE LPAREN valueExpression (COMMA normalForm)? tail=COMMA? RPAREN                     #normalize
    | EXTRACT LPAREN datePart FROM expression ( AT TIME ZONE string )? RPAREN                   #bigqueryExtract
    | COALESCE LPAREN callArgument (COMMA callArgument)* tail=COMMA? RPAREN                          #coalesce
    | dateDiffCall                                                                                   #dateDiff
    | COUNT LPAREN ASTERISK RPAREN functionCallTail                                          #countStar
    |
      functionCallHead functionName
        LPAREN
        (
            (setQuantifier? callArgument (COMMA callArgument)*)? functionExtraArguments
        )
        RPAREN
        functionCallTail                                                                     #functionCall

    | identifier over                                                                     #measure
    | <logical='Argument'>
      identifier '->' expression                                                          #lambda
    | LPAREN (identifier (COMMA identifier)*)? tail=COMMA? RPAREN '->' expression                 #lambda
    | LPAREN query RPAREN                                                                       #subqueryExpression
    | ARRAY
        (LT type_ GT)?
        LBRACKET (expression (COMMA expression)*)? tail=COMMA? RBRACKET                               #bigqueryArrayConstructor
    | value=primaryExpression LBRACKET index=valueExpression RBRACKET                               #subscript
    | <logical='TableReference,IntrinsicAlias'>
      base_=primaryExpression DOT fieldName=columnNameComponent                                     #dereference
    | <logical='Identifier,TableReference,Argument,IntrinsicAlias'>
      columnName                                                                          #columnReference
    | <logical='Identifier,IntrinsicAlias'>
      LPAREN expression RPAREN                                                                  #parenthesizedExpression
    | LBRACKET( expression (COMMA expression)* tail=COMMA? )? RBRACKET                                #array
    | VARIABLE                                                                            #variable
    | ARRAY LPAREN query RPAREN                                                                 #arraySubquery // bigquery only
    ;

functionCallHead
    : (SAFE DOT)?
    ;

functionCallTail options {logical='FunctionCallConstraint';}
    : over?
    ;
callArgument options {logical='Expr,Argument,Vec<Expr>,Arguments';}
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;
functionExtraArguments options {logical='FunctionCallConstraint';}
    : nullTreatment? havingArgument? (ORDER BY sortItem (COMMA sortItem)*)? limitArgument?
    ;

dateDiffCall options {logical='Expr';}
    : ( DATE_DIFF | DATETIME_DIFF | TIMESTAMP_DIFF )
      LPAREN left=expression COMMA right=expression COMMA datePart RPAREN
    ;

datePart options {logical='DatePart';}
    : MICROSECOND
    | MILLISECOND
    | SECOND
    | MINUTE
    | HOUR
    | DAY
    | DAYOFWEEK
    | DAYOFYEAR
    | WEEK ( LPAREN ( SUNDAY | MONDAY | TUESDAY | WEDNESDAY | THURSDAY | FRIDAY | SATURDAY ) RPAREN )?
    | ISOWEEK
    | MONTH
    | QUARTER
    | YEAR
    | ISOYEAR
    | DATETIME
    | DATE
    | TIME
    ;

functionName options {logical='Vec<Identifier>';}
    : qualifiedName | LEFT | RIGHT | IF | REPLACE | GROUPING
    ;
havingArgument
    : HAVING (MIN|MAX) expression
    ;

limitArgument
    : LIMIT limit=limitRowCount
    ;

namedParameter options {logical='Field';}
    :
    name=identifier (type_ | ANY TYPE)
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
      QUOTED_STRING                         #quotedStringLiteral
    | TRIPLE_QUOTED_STRING                  #tripleQuotedStringLiteral
    | RAW_QUOTED_STRING                     #rawStringLiteral
    | RAW_TRIPLE_QUOTED_STRING              #rawTripleQuotedStringLiteral
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
    : INTERVAL expression from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | QUARTER | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND
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
    | ARRAY LT type_ GT                                                           #legacyArrayType
    | STRUCT LT rowField (COMMA rowField)* tail=COMMA? GT                           #legacyStructType
    | INTERVAL                                                                              #intervalType
    | nonnullableType COLLATE string                                                          #bigqueryType
    | typeIdentifier
     (LPAREN typeParameter (COMMA typeParameter)* tail=COMMA? RPAREN)?                      #primitiveType
    ;

rowField
    : type_
    | identifier type_;


typeParameter options
{logical='u64,GenericInteger';}
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

mergeCase:
      WHEN MATCHED (AND condition=booleanExpression)? THEN
        (mergeUpdateClause | DELETE)       #mergeCaseMatched
    | WHEN NOT MATCHED (BY TARGET)? (AND condition=booleanExpression)? THEN
        (mergeInsertClause | DELETE)       #mergeCaseNotMatched
    | WHEN NOT MATCHED BY SOURCE (AND condition=booleanExpression)? THEN
        (mergeUpdateClause | DELETE)       #mergeCaseNotMatchedBySource
    ;

mergeUpdateClause:
    UPDATE SET targets+=expression EQ values+=expression
          (COMMA targets+=expression EQ values+=expression)*
    ;

mergeInsertClause:
    INSERT (LPAREN targets+=expression (COMMA targets+=expression)* tail+=COMMA? RPAREN)?
        VALUES LPAREN values+=expression (COMMA values+=expression)* tail+=COMMA? RPAREN
    ;

over options {logical='WindowSpecification';}
    : OVER (windowName=identifier | LPAREN windowSpecification RPAREN)
    ;
windowFrame options {logical='WindowFrame';}
    :
      frameExtent
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

maybeDashedIdentifier options {logical='Identifier';}
    : identifier
    | dashedIdentifier
    ;

dashedPathExpression options {logical='TableReference,Vec<Identifier>,PartSpans';}
    : dashedIdentifier ( DOT pathComponent )*
    ;

maybeDashedPathExpression options {logical='TableReference,Expr,Vec<Identifier>,PartSpans';}
    : pathExpression
    | dashedPathExpression
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

identifier options {logical='Identifier,*Option<UnquotedIdentifier>';}
    : <logical='Option<UnquotedIdentifier>'>
      IDENTIFIER             #unquotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

pathComponent options {logical='Identifier';}
    : identifier
    // Any keyword is allowed
    | ALL | AND | ANY | ARRAY | AS | ASC | AT
    | BETWEEN | BY
    | CASE | CAST | COLLATE | CREATE | CROSS | CUBE | CURRENT
    | DEFAULT | DEFINE | DESC | DISTINCT
    | ELSE | END | ESCAPE | EXCEPT | EXCLUDE | EXISTS | EXTRACT
    | FALSE | FETCH | FOLLOWING | FOR | FROM | FULL
    | GROUP | GROUPING | GROUPS
    | HAVING
    | IF | IGNORE | IN | INNER | INTERSECT | INTERVAL | INTO | IS
    | JOIN
    | LATERAL | LEFT | LIKE | LIMIT
    | MATCH_RECOGNIZE | MERGE | MINUS_KW
    | NATURAL | NO | NOT | NULL | NULLS
    | OF | ON | OR | ORDER | OUTER | OVER
    | PARTITION | PRECEDING
    | QUALIFY
    | RANGE | RECURSIVE | RESPECT | RIGHT | ROLLUP | ROWS
    | SELECT | SET | SOME | STRUCT
    | TABLESAMPLE | THEN | TO | TRUE
    | UNBOUNDED | UNION | UNNEST | USING
    | WHEN | WHERE | WINDOW | WITH
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
    | MINUS? HEXADECIMAL_VALUE #hexadecimalLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    :
      ABORT | ABSENT | ADD | ADMIN | AFTER | ALTER | ANALYZE | ANTI | ATTACH | AUTHORIZATION | AUTO
    | BACKUP | BEGIN | BERNOULLI | BOTH | BREAK | BZIP2
    | CALL | CANCEL | CASCADE | CASE_INSENSITIVE | CASE_SENSITIVE | CATALOGS | CHARACTER | CLONE | CLOSE | CLUSTER | COALESCE | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED
    | COMPOUND | COMPRESSION | CONDITIONAL | CONNECT | CONNECTION | CONSTRAINT | CONTINUE | COPARTITION | COPY | COUNT | CURRENT_ROLE | CUSTOM_HOLIDAY
    | DATA | DATABASE | DATASHARE | DATE | DATETIME | DATE_DIFF | DATETIME_DIFF | DAY | DAYOFWEEK | DAYOFYEAR | DEALLOCATE | DECLARE | DEFAULTS | DEFINER | DELETE | DELIMITED
    | DELIMITER | DENY | DESCRIBE | DESCRIPTOR | DETACH | DETERMINISTIC | DISTKEY | DISTRIBUTED | DISTSTYLE | DO | DOUBLE | DROP
    | ELSEIF | EMPTY | ENCODE | ENCODING | ERROR | EVEN | EXCEPTION | EXCLUDING | EXECUTE | EXPLAIN | EXTERNAL
    | FIELDS | FILTER | FINAL | FIRST | FORMAT | FRIDAY | FUNCTION | FUNCTIONS
    | GENERATED | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GZIP
    | HEADER | HOUR
    | IDENTITY | ILIKE | IMMEDIATE | INCLUDE | INCLUDING | INITIAL | INPUT | INPUTFORMAT | INSERT | INTERLEAVED | INVOKER | IO | ISOLATION | ISOWEEK | ISOYEAR | ITERATE
    | JSON
    | KEEP | KEY | KEYS
    | LAMBDA | LANGUAGE | LAST | LEADING | LEAVE | LEVEL | LIBRARY | LINES | LISTAGG | LOCAL | LOCATION | LOCK | LOGICAL | LOOP
    | MAP | MASKING | MATCH | MATCHED | MATCHES | MATERIALIZED | MAX | MEASURES | MESSAGE | MICROSECOND | MILLISECOND | MIN | MINUS_KW | MINUTE | MONDAY
    | MODEL | MONTH
    | NAME | NEXT | NFC | NFD | NFKC | NFKD | NONE | NORMALIZE
    | OBJECT | OFFSET | OMIT | ONE | ONLY | OPTION | OPTIONS | OUTPUT | OUTPUTFORMAT | OVERFLOW
    | PARTITIONED | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER | PERCENT_KW | PERIOD | PERMUTE | PIVOT | POSITION | PRECISION | PREPARE | PRIOR | PRIVILEGES
    | PROCEDURE | PROPERTIES | PRUNE
    | QUARTER | QUOTES
    | RAISE | READ | REFRESH | REMOTE | RENAME | REPEAT | REPEATABLE | REPLACE | RESET | RESTRICT | RETURN | RETURNING | RETURNS | REVOKE | RLS | ROLE | ROLES | ROLLBACK | ROW
    | RUNNING
    | SAFE | SAFE_CAST | SATURDAY | SCALAR | SCHEMA | SCHEMAS | SECOND | SECURITY | SEEK | SEMI | SERDE | SERDEPROPERTIES | SERIALIZABLE | SESSION | SETS | SHOW | SIMILAR
    | SNAPSHOT | SORTKEY | SOURCE | START | STATS | STORED | STRING_KW | SUBSET | SUBSTRING | SUNDAY | SYSTEM | SYSTEM_TIME
    | TABLE | TABLES | TARGET | TEMP | TEMPORARY | TERMINATED | TEXT | THURSDAY | TIES | TIME | TIMESTAMP | TIMESTAMP_DIFF | TOP | TRAILING | TRAINING_DATA
    | TRANSACTION | TRANSFORM | TRIM | TRUNCATE | TRY_CAST | TUESDAY | TUPLE | TYPE
    | UESCAPE | UNCOMMITTED | UNCONDITIONAL | UNKNOWN | UNLOAD | UNMATCHED | UNPIVOT | UNSIGNED | UNTIL | UPDATE | USE | USER | UTF16 | UTF32 | UTF8
    | VACUUM | VALIDATE | VALUE | VALUES | VARYING | VERBOSE | VERSION | VIEW
    | WEDNESDAY | WEEK | WHILE | WITHOUT | WORK | WRAPPER | WRITE
    | XZ
    | YEAR | YES
    | ZONE | ZSTD
    ;

ABORT: 'ABORT';
ABSENT: 'ABSENT';
ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANTI: 'ANTI';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
ATTACH: 'ATTACH';
AUTHORIZATION: 'AUTHORIZATION';
AUTO: 'AUTO';
BACKUP: 'BACKUP';
BEGIN: 'BEGIN';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BOTH: 'BOTH';
BREAK: 'BREAK';
BY: 'BY';
BZIP2: 'BZIP2';
CALL: 'CALL';
CANCEL: 'CANCEL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CASE_SENSITIVE: 'CASE_SENSITIVE';
CASE_INSENSITIVE: 'CASE_INSENSITIVE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
CHARACTER: 'CHARACTER';
CLONE: 'CLONE';
CLOSE: 'CLOSE';
CLUSTER: 'CLUSTER';
COALESCE: 'COALESCE';
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
CONSTRAINT: 'CONSTRAINT';
CONTINUE: 'CONTINUE';
COPARTITION: 'COPARTITION';
COPY: 'COPY';
COUNT: 'COUNT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_ROLE: 'CURRENT_ROLE';
CUSTOM_HOLIDAY: 'CUSTOM_HOLIDAY';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATASHARE: 'DATASHARE';
DATE: 'DATE';
DATETIME: 'DATETIME';
DAY: 'DAY';
DAYOFWEEK: 'DAYOFWEEK';
DAYOFYEAR: 'DAYOFYEAR';
DATETIME_DIFF: 'DATETIME_DIFF';
DATE_DIFF: 'DATE_DIFF';
DEALLOCATE: 'DEALLOCATE';
DECLARE: 'DECLARE';
DEFAULT: 'DEFAULT';
DEFAULTS: 'DEFAULTS';
DEFINE: 'DEFINE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DELIMITED: 'DELIMITED';
DELIMITER: 'DELIMITER';
DENY: 'DENY';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DESCRIPTOR: 'DESCRIPTOR';
DETERMINISTIC: 'DETERMINISTIC';
DISTINCT: 'DISTINCT';
DISTKEY: 'DISTKEY';
DISTRIBUTED: 'DISTRIBUTED';
DISTSTYLE: 'DISTSTYLE';
DETACH: 'DETACH';
DO: 'DO';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
ELSEIF: 'ELSEIF';
EMPTY: 'EMPTY';
ENCODE: 'ENCODE';
ENCODING: 'ENCODING';
END: 'END';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EVEN: 'EVEN';
EXCEPT: 'EXCEPT';
EXCEPTION: 'EXCEPTION';
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
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FRIDAY: 'FRIDAY';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GENERATED: 'GENERATED';
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
IDENTITY: 'IDENTITY';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IN: 'IN';
INCLUDE: 'INCLUDE';
INCLUDING: 'INCLUDING';
INITIAL: 'INITIAL';
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
ISOWEEK: 'ISOWEEK';
ISOYEAR: 'ISOYEAR';
ITERATE: 'ITERATE';
ILIKE: 'ILIKE';
JOIN: 'JOIN';
JSON: 'JSON';
KEEP: 'KEEP';
KEY: 'KEY';
KEYS: 'KEYS';
LAMBDA: 'LAMBDA';
LANGUAGE: 'LANGUAGE';
LEAVE: 'LEAVE';
LAST: 'LAST';
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
LOOP: 'LOOP';
MAP: 'MAP';
MASKING: 'MASKING';
MATCH: 'MATCH';
MATCHED: 'MATCHED';
MATCHES: 'MATCHES';
MATCH_RECOGNIZE: 'MATCH_RECOGNIZE';
MATERIALIZED: 'MATERIALIZED';
MAX: 'MAX';
MEASURES: 'MEASURES';
MERGE: 'MERGE';
MESSAGE: 'MESSAGE';
MICROSECOND: 'MICROSECOND';
MILLISECOND: 'MILLISECOND';
MIN: 'MIN';
MINUS_KW:'MINUS';
MINUTE: 'MINUTE';
MODEL: 'MODEL';
MONDAY: 'MONDAY';
MONTH: 'MONTH';
NAME: 'NAME';
NATURAL: 'NATURAL';
NEXT: 'NEXT';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
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
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OUTPUTFORMAT: 'OUTPUTFORMAT';
OVER: 'OVER';
OVERFLOW: 'OVERFLOW';
PARTITION: 'PARTITION';
PARTITIONED: 'PARTITIONED';
PARTITIONS: 'PARTITIONS';
PASSING: 'PASSING';
PAST: 'PAST';
PATH: 'PATH';
PATTERN: 'PATTERN';
PER: 'PER';
PERCENT_KW: 'PERCENT';
PERIOD: 'PERIOD';
PERMUTE: 'PERMUTE';
PIVOT: 'PIVOT';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PRIOR: 'PRIOR';
PROCEDURE: 'PROCEDURE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
QUALIFY: 'QUALIFY';
QUARTER: 'QUARTER';
QUOTES: 'QUOTES';
RAISE: 'RAISE';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
REFRESH: 'REFRESH';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNING: 'RETURNING';
REMOTE: 'REMOTE';
REPEAT: 'REPEAT';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
RLS: 'RLS';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
RUNNING: 'RUNNING';
SAFE: 'SAFE';
SAFE_CAST: 'SAFE_CAST';
SATURDAY: 'SATURDAY';
SCALAR: 'SCALAR';
SECOND: 'SECOND';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECURITY: 'SECURITY';
SEEK: 'SEEK';
SELECT: 'SELECT';
SEMI: 'SEMI';
SERDE: 'SERDE';
SERDEPROPERTIES: 'SERDEPROPERTIES';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SIMILAR: 'SIMILAR';
SNAPSHOT: 'SNAPSHOT';
SOME: 'SOME';
SORTKEY: 'SORTKEY';
START: 'START';
STATS: 'STATS';
STORED: 'STORED';
STRUCT: 'STRUCT';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
SUNDAY: 'SUNDAY';
SYSTEM: 'SYSTEM';
SYSTEM_TIME: 'SYSTEM_TIME';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEMP: 'TEMP';
TEMPORARY: 'TEMPORARY';
TERMINATED: 'TERMINATED';
TEXT: 'TEXT';
STRING_KW: 'STRING';
THEN: 'THEN';
THURSDAY: 'THURSDAY';
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TIMESTAMP_DIFF: 'TIMESTAMP_DIFF';
TO: 'TO';
TOP: 'TOP';
TRAILING: 'TRAILING';
TARGET: 'TARGET';
SOURCE: 'SOURCE';
TRAINING_DATA: 'TRAINING_DATA';
TRANSACTION: 'TRANSACTION';
TRANSFORM: 'TRANSFORM';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TUPLE: 'TUPLE';
TUESDAY: 'TUESDAY';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNCONDITIONAL: 'UNCONDITIONAL';
UNION: 'UNION';
UNKNOWN: 'UNKNOWN';
UNLOAD: 'UNLOAD';
UNMATCHED: 'UNMATCHED';
UNNEST: 'UNNEST';
UNPIVOT: 'UNPIVOT';
UNSIGNED: 'UNSIGNED';
UNTIL: 'UNTIL';
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
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
WEDNESDAY: 'WEDNESDAY';
WEEK: 'WEEK';
WHEN: 'WHEN';
WHERE: 'WHERE';
WHILE: 'WHILE';
WINDOW: 'WINDOW';
WITH: 'WITH';
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

BITWISE_AND: '&';
BITWISE_OR: '|';
BITWISE_XOR: '^';
BITWISE_SHIFT_LEFT: '<<';
POSIX: '~';

ESCAPE_SEQUENCE:
	'\\' .
	;

QUOTED_STRING
    : '"' ( ~["\r\n\\] | ESCAPE_SEQUENCE )* '"'
    | '\'' ( ~['\r\n\\] | ESCAPE_SEQUENCE )* '\''
    ;

TRIPLE_QUOTED_STRING
    : '"""' .*? '"""'
    | '\'\'\'' .*? '\'\'\''
    ;

RAW_QUOTED_STRING
    : 'R"' ( ~["\r\n] | '\\"' )* '"'
    | 'R\'' ( ~['\r\n] | '\\\'' )* '\''
    ;

RAW_TRIPLE_QUOTED_STRING
    : 'R"""' .*? '"""'
    | 'R\'\'\'' .*? '\'\'\''
    ;

BINARY_LITERAL
    :
      'X\'' (~'\'')* '\''
    | 'B"' ( ~'"' )* '"'
    | 'B\'' (~'\'')* '\''
    | 'B"""' .*? '"""'
    | 'B\'\'\'' .*? '\'\'\''
    | 'RB"' ( ~'"' )* '"'
    | 'RB\'' (~'\'')* '\''
    | 'RB"""' .*? '"""'
    | 'RB\'\'\'' .*? '\'\'\''
    | 'BR"' ( ~'"' )* '"'
    | 'BR\'' (~'\'')* '\''
    | 'BR"""' .*? '"""'
    | 'BR\'\'\'' .*? '\'\'\''
    ;


// BINARY_TRIPLE_QUOTED_LITERAL
//     : 'B"""' .*? '"""'
//     | 'B\'\'\'' .*? '\'\'\''
//     ;

INTEGER_VALUE
    : DIGIT+
    ;

HEXADECIMAL_VALUE
    : '0X' [0-9a-fA-F]+
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
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( '\\' [a-zA-Z_`\\] | ~[`\\\n] )* '`'
    ;

VARIABLE
    : '@' IDENTIFIER
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

BIG_QUERY_SIMPLE_COMMENT
    : '#' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;


BRACKETED_COMMENT
    : '/*' ( BRACKETED_COMMENT | . )*? '*/'      -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

OTHER_WS
    // U+00A0 No-Break Space (NBSP)
    // U+202F Narrow No-Break Space (NNBSP)
    : [\u00A0\u202F] -> channel(HIDDEN)
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
