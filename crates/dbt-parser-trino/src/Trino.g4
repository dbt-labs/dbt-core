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

grammar Trino;


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
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)?
        TABLE (IF NOT EXISTS)?
        dest=qualifiedName
        (
            columnAliases
        )?
        (COMMENT comment=string)?
        (WITH properties)? AS (query | LPAREN query RPAREN)
        (WITH (NO)? DATA)?                                             #createTableAsSelect
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE RECURSIVE TABLE (IF NOT EXISTS)?
        dest=qualifiedName columnAliases?
        (COMMENT comment=string)?
        (WITH properties)? AS (query | LPAREN query RPAREN)  #createRecursiveTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,Properties'>
      CREATE (OR REPLACE)?
        TABLE (IF NOT EXISTS)?
        dest=qualifiedName
        (LPAREN tableElements tail=COMMA? RPAREN)?
         (COMMENT comment=string)?
         (WITH properties)?                                            #createTable
    | <logical='Arc<LogicalPlan>,Option<TargetName>'>
      INSERT INTO dest=qualifiedName columnAliases? (query | LPAREN query RPAREN) #insertInto
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? MATERIALIZED VIEW
        (IF NOT EXISTS)?
        dest=qualifiedName
        (GRACE PERIOD interval)?
        (COMMENT comment=string)?
        (WITH properties)? AS (LPAREN query RPAREN | query )                                    #createMaterializedView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? VIEW
        dest=qualifiedName
        (COMMENT comment=string)?
        (SECURITY (DEFINER | INVOKER))? AS query                       #createView
    | <logical='Arc<LogicalPlan>,Option<TargetName>,UpstreamEntities'>
      DESCRIBE tableName=qualifiedName                                      #showColumns
    | SHOW COLUMNS FROM tableName=qualifiedName                             #showColumns
    | <logical='Option<TargetName>,Vec<FunctionRegistration>'>
      CREATE (OR REPLACE)? EXTERNAL? FUNCTION ~SEMI_COLON*                    #createFunction

    /*
    * Catchall for all other statements
    *
    * This section must remain at the bottom!
    */
    | MERGE ~SEMI_COLON*                                                      #merge
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
    | <logical='Option<TargetName>,Arc<LogicalPlan>,UpstreamEntities'>
      EXPLAIN ('(' explainOption (',' explainOption)* ')')? statement         #explain
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
    (COMMENT comment=string)?
    ;
fieldDefinition options {logical='Field';}
    : columnName columnSchemaWithMetadata
    ;

columnName options {logical='Identifier';}
    : identifier
    ;

columnNameComponent options {logical='Identifier';}
    : columnName
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
    : queryLimitTarget
      (OFFSET offset=rowCount rowOrRows?)?
      ((LIMIT limit=limitRowCount) | (FETCH FIRST fetchFirst=rowCount ROWS ONLY))?
    ;
queryLimitTarget options {logical='Arc<LogicalPlan>';}
    : queryTerm
      orderBy?                  #queryLimitTargetDefault
    ;
rowOrRows
    : ROW
    | ROWS
    ;

limitRowCount options {logical='Option<usize>';}
    : ALL
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
    : (UNION | EXCEPT | MINUS_KW) setQuantifier?
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
    | LPAREN query_=queryNoWith RPAREN                  #subquery
    ;

sortItem options {logical='SortExpr';}
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification options {logical='Arc<LogicalPlan>';}
    : SELECT
      setQuantifier?
      querySelectItems
      (FROM relation (COMMA relation)* tail+=COMMA?)?
      (WHERE where_=booleanExpression)?
      aggregationClause?
      (HAVING having=booleanExpression)?
      (WINDOW windowDefinition (COMMA windowDefinition)* tail+=COMMA?)?
    ;

querySelectItems
    : selectItem (COMMA selectItem)* tail=COMMA?
    ;

aggregationClause options {logical='GroupBy';}
    : GROUP BY groupBy
    ;

groupBy options {logical='GroupBy';}
    : setQuantifier? groupingElement (COMMA groupingElement)* tail=COMMA?       #groupByDefault
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
      expression (AS? selectItemAlias)?                          #selectSingle
    | multiSelect                                                #selectMulti
    ;

multiSelect options {logical='Vec<Expr>';}
    : selectStar
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
      | NATURAL joinType JOIN right=noJoinRelation
      )
                                                            #joinRelation
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
    : target=patternRecognition
    ;

sampledRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelationTarget sampleOperator?
    ;
sampleOperator
    : TABLESAMPLE sampleMethod LPAREN percentage=expression RPAREN
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

patternRecognitionTarget options {logical='Arc<LogicalPlan>';}
    :
    aliasedRelation
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

aliasedRelationTarget options {logical='Arc<LogicalPlan>';}
    : relationPrimary
    ;
aliasedRelation options {logical='Arc<LogicalPlan>';}
    : aliasedRelationTarget
    (AS? alias=identifier columnAliases?)?
    ;
columnAliases options {logical='Vec<Identifier>';}
    : LPAREN identifierSeq tail=COMMA? RPAREN
    ;


relationPrimary options {logical='Arc<LogicalPlan>,TableReference,*Span';}
    : <logical='Span'>
      tableNameRef=pathExpression queryPeriod?                        #tableName
    | LPAREN query RPAREN                                                   #subqueryRelation
    | UNNEST LPAREN expression (COMMA expression)* tail=COMMA? RPAREN (WITH ORDINALITY)?  #unnest
    | LATERAL LPAREN query RPAREN                                           #lateral
    | TABLE LPAREN tableFunctionCall RPAREN                                 #tableFunctionInvocation
    | LPAREN rel=relation RPAREN                                                #parenthesizedRelation
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
    | NOT? IN LPAREN query RPAREN                                               #inSubquery
    | NOT? (LIKE | ILIKE) pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | NOT? SIMILAR TO pattern=valueExpression (ESCAPE escape=valueExpression)?      #similarTo
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    | IS NOT? UNKNOWN                                                     #unknownPredicate
    ;
valueExpression options {logical='Expr,Argument,usize,f64,String,Identifier,Vec<Expr>,IntrinsicAlias';}
    :
      <logical='usize,f64,String,Identifier,Vec<Expr>,Argument,IntrinsicAlias'>
      primaryExpression                                                                 #valueExpressionDefault
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
    NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | <logical='usize,f64,String'>
      number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | <logical='String'>
      string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | identifier string                                                                    #typeConstructor
    | DOUBLE PRECISION string                                                             #typeConstructor
    | <logical='Vec<Expr>'>
      LPAREN expression (COMMA expression)+ tail=COMMA? RPAREN                                    #rowConstructor
    | ROW LPAREN expression (COMMA expression)* RPAREN                                            #rowConstructor
    | POSITION LPAREN needle=valueExpression IN haystack=valueExpression RPAREN                                 #position
    | name=LISTAGG LPAREN setQuantifier? agg_expr=expression (COMMA expression)? tail+=COMMA?
        (ON OVERFLOW listAggOverflowBehavior)? RPAREN
        (WITHIN GROUP LPAREN ORDER BY sortItem (COMMA sortItem)* tail+=COMMA? RPAREN)?            #listagg
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
    | SUBSTRING LPAREN valueExpression FROM valueExpression (FOR valueExpression)? RPAREN       #substring
    | NORMALIZE LPAREN valueExpression (COMMA normalForm)? tail=COMMA? RPAREN                     #normalize
    | EXTRACT LPAREN identifier FROM valueExpression RPAREN                                     #extract
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
    | ARRAY LBRACKET (expression (COMMA expression)*)? tail=COMMA? RBRACKET                           #arrayConstructor
    | value=primaryExpression LBRACKET index=valueExpression RBRACKET                               #subscript
    | <logical='TableReference,IntrinsicAlias'>
      base_=primaryExpression DOT fieldName=columnNameComponent                                     #dereference
    | <logical='Identifier,TableReference,Argument,IntrinsicAlias'>
      columnName                                                                          #columnReference
    | <logical='Identifier,IntrinsicAlias'>
      LPAREN expression RPAREN                                                                  #parenthesizedExpression
    | JSON_EXISTS LPAREN jsonPathInvocation (jsonExistsErrorBehavior ON ERROR)? RPAREN          #jsonExists
    | JSON_VALUE LPAREN
        jsonPathInvocation
        (RETURNING type_)?
        (emptyBehavior=jsonValueBehavior ON EMPTY)?
        (errorBehavior=jsonValueBehavior ON ERROR)?
      RPAREN                                                                                 #jsonValue
    | JSON_QUERY LPAREN
        jsonPathInvocation
        (RETURNING type_ (FORMAT jsonRepresentation)?)?
        (jsonQueryWrapperBehavior WRAPPER)?
        ((KEEP | OMIT) QUOTES (ON SCALAR STRING_KW)?)?
        (emptyBehavior=jsonQueryBehavior ON EMPTY)?
        (errorBehavior=jsonQueryBehavior ON ERROR)?
      RPAREN                                                                                 #jsonQuery
    | JSON_OBJECT LPAREN
        (
          jsonObjectMember (COMMA jsonObjectMember)* tail=COMMA?
          (NULL ON NULL | ABSENT ON NULL)?
          (WITH UNIQUE KEYS? | WITHOUT UNIQUE KEYS?)?
        )?
        (RETURNING type_ (FORMAT jsonRepresentation)?)?
      RPAREN                                                                                 #jsonObject
    | JSON_ARRAY LPAREN
        (
          jsonValueExpression (COMMA jsonValueExpression)* tail=COMMA?
          (NULL ON NULL | ABSENT ON NULL)?
        )?
        (RETURNING type_ (FORMAT jsonRepresentation)?)?
     RPAREN                                                                                  #jsonArray
    | LBRACKET( expression (COMMA expression)* tail=COMMA? )? RBRACKET                                #array
    | VARIABLE                                                                            #variable
    ;

functionCallHead
    : processingMode?
    ;
functionCallTail options {logical='FunctionCallConstraint';}
    : filter? (nullTreatment? over)?
    ;
callArgument options {logical='Expr,Argument,Vec<Expr>,Arguments';}
    : expression                    #positionalArgument
    ;
functionExtraArguments options {logical='FunctionCallConstraint';}
    : (ORDER BY sortItem (COMMA sortItem)*)?
    ;
functionName options {logical='Vec<Identifier>';}
    : qualifiedName | LEFT | RIGHT | IF | REPLACE | GROUPING
    ;

field
    : expression (AS identifier)?
    ;

jsonPathInvocation options {logical='Vec<Expr>';}
    : jsonValueExpression COMMA path=string tail+=COMMA?
        (PASSING jsonArgument (COMMA jsonArgument)*)? tail+=COMMA?
    ;

jsonValueExpression options {logical='Expr';}
    : expression (FORMAT jsonRepresentation)?
    ;

jsonRepresentation
    : JSON (ENCODING (UTF8 | UTF16 | UTF32))? // TODO add implementation-defined JSON representation option
    ;

jsonArgument options {logical='Expr';}
    : jsonValueExpression AS identifier
    ;

jsonExistsErrorBehavior
    : TRUE
    | FALSE
    | UNKNOWN
    | ERROR
    ;

jsonValueBehavior
    : ERROR
    | NULL
    | DEFAULT expression
    ;

jsonQueryWrapperBehavior
    : WITHOUT ARRAY?
    | WITH (CONDITIONAL | UNCONDITIONAL)? ARRAY?
    ;

jsonQueryBehavior
    : ERROR
    | NULL
    | EMPTY ARRAY
    | EMPTY OBJECT
    ;

jsonObjectMember
    : KEY? expression VALUE jsonValueExpression
    | expression ':' jsonValueExpression
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
    : INTERVAL sign=(PLUS | MINUS)? (
          string
        | INTEGER_VALUE
        ) from=intervalField (TO to=intervalField)?
    ;
intervalField
    : YEAR
    | MONTH | DAY | HOUR | M | MIN | MINUTE | S | SEC | SECOND
    | YEARS
    | MONTHS | DAYS | HOURS | MINUTES | SECONDS | WEEK
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
    | ROW LPAREN rowField (COMMA rowField)* tail=COMMA? RPAREN                              #rowType
    | INTERVAL from=intervalField (TO to=intervalField)?                            #intervalType
    | base_=TIMESTAMP (LPAREN precision = typeParameter RPAREN)? (WITHOUT TIME ZONE)?     #dateTimeType
    | base_=TIMESTAMP (LPAREN precision = typeParameter RPAREN)? WITH TIME ZONE           #dateTimeType
    | base_=TIME (LPAREN precision = typeParameter RPAREN)? (WITHOUT TIME ZONE)?          #dateTimeType
    | base_=TIME (LPAREN precision = typeParameter RPAREN)? WITH TIME ZONE                #dateTimeType
    | DOUBLE PRECISION                                                              #doublePrecisionType
    | MAP LT keyType=type_ COMMA valueType=type_ tail=COMMA? GT                     #legacyMapType
    | ARRAY LT type_ GT                                                           #legacyArrayType
    | typeIdentifier
     (LPAREN typeParameter (COMMA typeParameter)* tail=COMMA? RPAREN)?                      #primitiveType
    | FUNCTION LPAREN type_ (COMMA type_)* RPAREN                                           #lambdaType
    ;
rowField
    : type_
    | identifier type_;


typeParameter options
{logical='GenericDataType,DataType,GenericInteger,u64,TimeUnit';}
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

explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
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
    | quotedIdentifier       #quotedIdentifierDefault
    | nonReserved            #unquotedIdentifier
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

prestoShowFunctionType options {logical='GenericDataType';}
    : ROW LPAREN prestoShowFunctionRowField (COMMA prestoShowFunctionRowField)* RPAREN #prestoFunctionArgumentStruct
    | MAP LPAREN key=prestoShowFunctionType COMMA value=prestoShowFunctionType RPAREN  #prestoFunctionArgumentMap
    | ARRAY LPAREN prestoShowFunctionType RPAREN                                     #prestoFunctionArgumentArray
    | FUNCTION LPAREN prestoShowFunctionType (COMMA prestoShowFunctionType)* RPAREN    #prestoFunctionArgumentLambda
    | INTEGER                                                                  #prestoFunctionArgumentInteger
    | type_                                                                    #prestoFunctionArgumentDefault
    ;

prestoShowFunctionRowField
    : prestoShowFunctionType
    | identifier prestoShowFunctionType;

prestoShowFunctionTypes options {logical='Vec<GenericDataType>';}
    : prestoShowFunctionType (COMMA prestoShowFunctionType)*
    | EOF
    ;
nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    :
      ABORT | ABSENT | ADD | ADMIN | AFTER | ALL | ANALYZE | ANTI |ANY | ARRAY | ASC | AT | ATTACH | AUTHORIZATION
    | BEGIN | BERNOULLI | BOTH | BZIP2
    | CALL | CANCEL | CASCADE | CATALOGS | CHARACTER | CLONE | CLOSE | CLUSTER | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | COMPRESSION | CONDITIONAL | CONNECTION | COPARTITION | COPY | COUNT | CURRENT
    | DATA | DATABASE | DATASHARE | DATE | DAY | DAYS | DECLARE | DEFAULT | DEFINE | DEFINER | DELIMITER | DENY | DESC | DESCRIBE | DESCRIPTOR | DETACH | DISTRIBUTED | DOUBLE
    | EMPTY | ENCODING | END | ERROR | EXCLUDING | EXECUTE | EXPLAIN | EXTERNAL
    | FETCH | FILTER | FINAL | FIRST
    | FOLLOWING | FORMAT | FUNCTIONS
    | GRACE | GRANT | GRANTED | GRANTS | GRAPHVIZ | GROUPS | GZIP
    | HOUR | HOURS
    | IF
    | IGNORE
    | INCLUDING | INITIAL | INPUT | INTEGER | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | KEEP | KEY | KEYS
    | LAMBDA | LAST | LATERAL | LEADING | LEVEL | LIBRARY | LIMIT | LOCAL | LOCATION | LOCK | LOGICAL
    | M | MAP | MASKING | MATCH | MATCHED | MATCHES | MATCH_RECOGNIZE | MATERIALIZED | MAX | MEASURES | MERGE
    | MIN | MINUS_KW | MINUTE | MINUTES | MODEL | MONTH | MONTHS
    | NEXT | NFC | NFD | NFKC | NFKD | NO | NONE | NULLS
    | OBJECT | OF | OFFSET | OMIT | ONE | ONLY | OPTION | ORDINALITY | OUTPUT | OVER | OVERFLOW
    | PARTITION | PARTITIONS | PASSING | PAST | PATH | PATTERN | PER
    | PERIOD | PERMUTE | POSITION | PRECEDING | PRECISION | PRIVILEGES | PROCEDURE | PROPERTIES | PRUNE
    | QUOTES
    | RANGE | READ | REFRESH | RENAME | REPEATABLE | REPLACE | RESET
    | RESPECT
    | RESTRICT | RETURNING
    | REVOKE | RLS | ROLE | ROLES | ROLLBACK | ROLLUP | ROW | ROWS | RUNNING
    | S
    | SCALAR | SCHEMA | SCHEMAS | SEC | SECOND | SECONDS | SECURITY | SEEK | SEMI | SERIALIZABLE | SESSION | SET | SETS
    | SHOW | SNAPSHOT | SOME | START | STATS | STRING_KW | STRUCT | SUBSET | SUBSTRING | SYSTEM | SYSTEM_TIME
    | TABLES | TABLESAMPLE | TEMP | TEMPORARY | TEXT | TIES | TIME | TIMESTAMP | TO | TRAILING | TRANSACTION | TRUNCATE | TRY_CAST | TUPLE | TYPE
    | UNBOUNDED | UNCOMMITTED | UNCONDITIONAL
    | UNIQUE
    | UNKNOWN | UNLOAD | UNMATCHED | UPDATE | USE | USER | UTF16 | UTF32 | UTF8
    | VACUUM | VALIDATE | VALUE
    | VARYING | VERBOSE | VERSION | VIEW
    | WINDOW
    | WITHIN
    | WITHOUT | WORK | WRAPPER | WRITE | WEEK
    | XZ
    | YEAR | YEARS
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
DATE: 'DATE';
DAY: 'DAY';
DAYS: 'DAYS';
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
DISTINCT: 'DISTINCT';
DISTKEY: 'DISTKEY';
DISTRIBUTED: 'DISTRIBUTED';
DISTSTYLE: 'DISTSTYLE';
DETACH: 'DETACH';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
ENCODE: 'ENCODE';
ENCODING: 'ENCODING';
END: 'END';
ERROR: 'ERROR';
ESCAPE: 'ESCAPE';
EVEN: 'EVEN';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FETCH: 'FETCH';
FILTER: 'FILTER';
FINAL: 'FINAL';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
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
HOURS: 'HOURS';
IDENTITY: 'IDENTITY';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INITIAL: 'INITIAL';
INNER: 'INNER';
INPUT: 'INPUT';
INPUTFORMAT: 'INPUTFORMAT';
INTEGER: 'INTEGER';
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
LAMBDA: 'LAMBDA';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEADING: 'LEADING';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIBRARY: 'LIBRARY';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LISTAGG: 'LISTAGG';
LOCAL: 'LOCAL';
LOCATION: 'LOCATION';
LOCK: 'LOCK';
LOGICAL: 'LOGICAL';
M: 'M';
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
MIN: 'MIN';
MINUS_KW:'MINUS';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MODEL: 'MODEL';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
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
ORDINALITY: 'ORDINALITY';
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
PERIOD: 'PERIOD';
PERMUTE: 'PERMUTE';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRECISION: 'PRECISION';
PREPARE: 'PREPARE';
PRIOR: 'PRIOR';
PROCEDURE: 'PROCEDURE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
QUOTES: 'QUOTES';
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
RETURNING: 'RETURNING';
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
S: 'S';
SCALAR: 'SCALAR';
SEC: 'SEC';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
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
// Note: 'SKIP' is a reserved word in ANTLR itself
SKIP_KW: 'SKIP';
SNAPSHOT: 'SNAPSHOT';
SOME: 'SOME';
SORTKEY: 'SORTKEY';
START: 'START';
STATS: 'STATS';
STORED: 'STORED';
STRUCT: 'STRUCT';
SUBSET: 'SUBSET';
SUBSTRING: 'SUBSTRING';
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
TIES: 'TIES';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TOP: 'TOP';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
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
VERBOSE: 'VERBOSE';
VERSION: 'VERSION';
VIEW: 'VIEW';
WEEK: 'WEEK';
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
YEARS: 'YEARS';
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

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;
UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
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
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
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
