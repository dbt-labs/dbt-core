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

grammar Databricks;


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
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,Properties'>
      ( CREATE (OR REFRESH)? (TEMP | TEMPORARY)? EXTERNAL? LIVE? STREAMING? TABLE (IF NOT EXISTS)?
      | (CREATE OR)? REPLACE TABLE
      ) dest=identifierReference (LPAREN tableElements tail=COMMA? RPAREN)? tableProvider?
        createTableClauses
        AS? query                                                      #createTableAsSelect
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,Properties'>
      (CREATE (TEMP | TEMPORARY)? EXTERNAL? TABLE (IF NOT EXISTS)? | (CREATE OR)? REPLACE TABLE) dest=identifierReference (LPAREN tableElements tail=COMMA? RPAREN)? tableProvider?
        createTableClauses                                            #createTable
    | CREATE TABLE (IF NOT EXISTS)? target=qualifiedName
        LIKE source=qualifiedName
        (tableProvider |
        rowFormat |
        createFileFormat |
        locationSpec |
        (TBLPROPERTIES tableProps=properties))*                      #createTableLike
    | ctes? dmlStatementNoWith                                         #dmlStatement
    | <logical='Arc<LogicalPlan>,Option<TargetName>,Option<TableType>,UpstreamEntities'>
      CREATE (OR REPLACE)? (GLOBAL? (TEMP | TEMPORARY))? MATERIALIZED? VIEW (IF NOT EXISTS)?
        dest=identifierReference
        (LPAREN columnDefinitionForView (COMMA columnDefinitionForView)* tail=COMMA? RPAREN)?
        ( tableProvider
        | (commentSpec |
         schemaBinding |
         (PARTITIONED ON identifierList) |
         (TBLPROPERTIES properties))* AS query
        )                                                             #createView
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
    | ALTER VIEW from=qualifiedName RENAME TO to=qualifiedName         #renameView
    | ALTER VIEW from=qualifiedName SET AUTHORIZATION principal        #setViewAuthorization
    | CREATE ROLE ~SEMI_COLON*                                                #createRole
    | GRANT ~SEMI_COLON*                                                      #grant
    | REVOKE ~SEMI_COLON*                                                     #revoke
    | <logical='Option<TargetName>,Arc<LogicalPlan>,UpstreamEntities'>
      EXPLAIN (EXTENDED | CODEGEN | COST | FORMATTED)? statement              #explain
    | SHOW ~SEMI_COLON*                                                       #show
    | RESET ~SEMI_COLON*                                                      #reset
    | COMMIT ~SEMI_COLON*                                                     #commit
    | ROLLBACK ~SEMI_COLON*                                                   #rollback
    | EXECUTE ~SEMI_COLON*                                                    #execute
    | UPDATE ~SEMI_COLON*                                                     #update

    | CREATE DATABASE ~SEMI_COLON*                                            #createFoo
    | CREATE CATALOG ~SEMI_COLON*                                             #createFoo
    | ALTER ~SEMI_COLON*                                                      #alter
    | USE ~SEMI_COLON*                                                        #use
    | OPTIMIZE ~SEMI_COLON*                                                   #optimize
    ;

tableElements
    : tableElement (COMMA tableElement)*
    ;
identifierReference options {logical='TableReference,Vec<Identifier>';}
    : IDENTIFIER_KW LPAREN expression RPAREN
    | qualifiedName
    ;
identifierCommentList
    : LPAREN identifierComment (COMMA identifierComment)* RPAREN
    ;

identifierComment
    : identifier commentSpec?
    ;

schemaBinding
    : WITH SCHEMA (BINDING | COMPENSATION | EVOLUTION | TYPE EVOLUTION)
    ;

createTableClauses options {logical='Properties';}
    :((OPTIONS options=properties) |
     (PARTITIONED BY LPAREN fields+=partitionField (COMMA fields+=partitionField)* RPAREN) |
     skewSpec |
     clusterBySpec |
     bucketSpec |
     rowFormat |
     createFileFormat |
     locationSpec |
     commentSpec |
     (TBLPROPERTIES tableProps=properties))*
    ;

tableProvider options {logical='Properties';}
    : USING qualifiedName
      (OPTIONS LPAREN identifier DOUBLEQUOTED_STRING (',' identifier DOUBLEQUOTED_STRING)*  RPAREN)?
    ;
partitionField
    : transform  #partitionTransform
    | colType    #partitionColumn
    ;

transform
    : qualifiedName                                                                             #identityTransform
    | transformName=identifier
      LPAREN argument+=transformArgument (COMMA argument+=transformArgument)* RPAREN   #applyTransform
    ;

transformArgument
    : qualifiedName
    | constant
    ;

colType
    : colName=identifier type_ (NOT NULL)? commentSpec?
    ;

skewSpec
    : SKEWED BY identifierList
      ON (constantList | nestedConstantList)
      (STORED AS DIRECTORIES)?
    ;

clusterBySpec
    : CLUSTER BY LPAREN qualifiedName (COMMA qualifiedName)* RPAREN
    ;

bucketSpec
    : CLUSTERED BY identifierList
      (SORTED BY sortItem (COMMA sortItem)*)?
      INTO INTEGER_VALUE BUCKETS
    ;

constantList
    : LPAREN constant (COMMA constant)* RPAREN
    ;

nestedConstantList
    : LPAREN constantList (COMMA constantList)* RPAREN
    ;

rowFormat
    : ROW FORMAT SERDE name=string (WITH SERDEPROPERTIES props=properties)?       #rowFormatSerde
    | ROW FORMAT DELIMITED
      (FIELDS TERMINATED BY fieldsTerminatedBy=string (ESCAPED BY escapedBy=string)?)?
      (COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy=string)?
      (MAP KEYS TERMINATED BY keysTerminatedBy=string)?
      (LINES TERMINATED BY linesSeparatedBy=string)?
      (NULL DEFINED AS nullDefinedAs=string)?                                       #rowFormatDelimited
    ;

createFileFormat
    : STORED AS fileFormat
    | STORED BY storageHandler
    ;

fileFormat
    : INPUTFORMAT inFmt=string OUTPUTFORMAT outFmt=string    #tableFileFormat
    | identifier                                             #genericFileFormat
    ;

storageHandler
    : string (WITH SERDEPROPERTIES properties)?
    ;

locationSpec
    : LOCATION string
    ;

literalType options {logical='Identifier';}
    : DATE
    | TIMESTAMP | TIMESTAMP_LTZ | TIMESTAMP_NTZ
    | INTERVAL
    | X_KW
    | unsupportedType=identifier
    ;

dmlStatementNoWith
    : insertInto query                                                             #singleInsertQuery
    | FROM relation multiInsertQueryBody+                                             #multiInsertQuery
    | DELETE FROM identifierReference tableAlias whereClause?                      #deleteFromTable
    | UPDATE identifierReference tableAlias setClause whereClause?                 #updateTable
    | MERGE (WITH SCHEMA EVOLUTION)? INTO target=identifierReference targetAlias=tableAlias
        USING (source=identifierReference |
          LPAREN sourceQuery=query RPAREN) sourceAlias=tableAlias
        ON mergeCondition=booleanExpression
        matchedClause*
        notMatchedClause*
        notMatchedBySourceClause*                                                  #mergeIntoTable
    ;

ctes
    : WITH namedQuery (COMMA namedQuery)*
    ;

insertInto
    : INSERT OVERWRITE TABLE? identifierReference optionsClause? (partitionSpec (IF NOT EXISTS)?)?  ((BY NAME) | identifierList)? #insertOverwriteTable
    | INSERT INTO TABLE? identifierReference optionsClause? partitionSpec? (IF NOT EXISTS)? ((BY NAME) | identifierList)?   #insertIntoTable
    | INSERT INTO TABLE? identifierReference optionsClause? REPLACE whereClause                                             #insertIntoReplaceWhere
    | INSERT OVERWRITE LOCAL? DIRECTORY path=string rowFormat? createFileFormat?                     #insertOverwriteHiveDir
    | INSERT OVERWRITE LOCAL? DIRECTORY (path=string)? tableProvider (OPTIONS options=properties)? #insertOverwriteDir
    ;

multiInsertQueryBody
    : insertInto fromStatementBody
    ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

whereClause
    : WHERE booleanExpression
    ;

setClause
    : SET assignmentList
    ;

matchedClause
    : WHEN MATCHED (AND matchedCond=booleanExpression)? THEN matchedAction
    ;

notMatchedClause
    : WHEN NOT MATCHED (BY TARGET)? (AND notMatchedCond=booleanExpression)? THEN notMatchedAction
    ;

notMatchedBySourceClause
    : WHEN NOT MATCHED BY SOURCE (AND notMatchedBySourceCond=booleanExpression)? THEN notMatchedBySourceAction
    ;

optionsClause
    : WITH options=properties
    ;

partitionSpec
    : PARTITION LPAREN partitionVal (COMMA partitionVal)* RPAREN
    ;

lateralView options {logical='Arc<LogicalPlan>';}
    : LATERAL VIEW (OUTER)? tableFunctionCall tblName=identifier? (AS colNames=identifierSeq)?
    ;

fromStatementBody
    : transformClause
      whereClause?
      queryOrganization
    | selectClause
      lateralView*
      whereClause?
      aggregationClause?
      havingClause?
      windowClause?
      queryOrganization
    ;

queryOrganization
    : (ORDER BY order+=sortItem (COMMA order+=sortItem)*)?
      (CLUSTER BY clusterBy+=expression (COMMA clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (COMMA distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (COMMA sort+=sortItem)*)?
      windowClause?
      (LIMIT (ALL | limit=expression))?
      (OFFSET offset=expression)?
    ;

assignmentList
    : assignment (COMMA assignment)*
    ;

assignment
    : key=qualifiedName EQ value=expression
    ;

matchedAction
    : DELETE
    | UPDATE SET ASTERISK
    | UPDATE SET assignmentList
    ;

notMatchedAction
    : INSERT ASTERISK
    | INSERT LPAREN columns=multipartIdentifierList RPAREN
        VALUES LPAREN expression (COMMA expression)* RPAREN
    ;

notMatchedBySourceAction
    : DELETE
    | UPDATE SET assignmentList
    ;

partitionVal
    : identifier (EQ constant)?
    | identifier EQ DEFAULT
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

transformClause
    : (SELECT kind=TRANSFORM LPAREN setQuantifier? expressionSeq RPAREN
            | kind=MAP setQuantifier? expressionSeq
            | kind=REDUCE setQuantifier? expressionSeq)
      inRowFormat=rowFormat?
      (RECORDWRITER recordWriter=string)?
      USING script=string
      (AS (identifierSeq | colTypeList | (LPAREN (identifierSeq | colTypeList) RPAREN)))?
      outRowFormat=rowFormat?
      (RECORDREADER recordReader=string)?
    ;

selectClause
    : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
    ;

havingClause
    : HAVING booleanExpression
    ;

multipartIdentifierList
    : qualifiedName (COMMA qualifiedName)*
    ;

expressionSeq
    : expression (COMMA expression)*
    ;

colTypeList
    : colType (COMMA colType)*
    ;

hint
    : HENT_START hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HENT_END
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier LPAREN parameters+=primaryExpression (COMMA parameters+=primaryExpression)* RPAREN
    ;



query options {logical='Arc<LogicalPlan>,Vec<CteInfo>';}
    :  with? queryNoWith
    ;

with options {logical='Vec<CteInfo>';}
    : WITH namedQuery (COMMA namedQuery)*
    ;

tableElement
    : columnDefinition
    | qualifiedName+
    | tableConstraint
    ;
tableConstraint
    : (CONSTRAINT identifier)?
    ( PRIMARY KEY identifierList
    | FOREIGN KEY identifierList REFERENCES qualifiedName identifierList?
    )
    ;

columnDefinition options {logical='Field';}
    : fieldDefinition

    ;
columnDefinitionForView options {logical='Identifier';}
    : name=columnName
    type_?
    (COMMENT string)?
    ;
fieldDefinitions options {logical='DataType';}
    : fieldDefinition (COMMA fieldDefinition)* tail=COMMA?
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
    : columnSchema colDefinitionOption*
    ;

colDefinitionOption
    : NOT NULL
    | defaultExpression
    | generationExpression
    | commentSpec
    ;

generationExpression
    : GENERATED ALWAYS AS (LPAREN expression RPAREN | IDENTITY)
    ;

defaultExpression
    : DEFAULT expression
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
    : qualifiedName
    | string
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
      (LIMIT (ALL | limit=limitRowCount))?
      (OFFSET offset=rowCount)?
    ;
queryLimitTarget options {logical='Arc<LogicalPlan>';}
    : queryTerm
      orderBy?                  
      (CLUSTER BY clusterBy+=expression (COMMA clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (COMMA distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (COMMA sort+=sortItem)*)?
      windowClause?             #queryLimitTargetDatabricks
    ;

windowClause
    : WINDOW windowDefinition (COMMA windowDefinition)*
    ;

limitRowCount options {logical='Option<usize>';}
    : rowCount
    ;

rowCount options {logical='usize';}
    : expression
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
    | LPAREN query_=query RPAREN                        #subquery
    ;

sortItem options {logical='SortExpr';}
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification options {logical='Arc<LogicalPlan>';}
    : SELECT
      setQuantifier?
      querySelectItems
      (FROM relation)?
      (WHERE where_=booleanExpression)?
      aggregationClause?
      (HAVING having=booleanExpression)?
      (QUALIFY qualify=booleanExpression)?
      (WINDOW windowDefinition (COMMA windowDefinition)* tail+=COMMA?)?
    ;

querySelectItems
    : selectItem (COMMA selectItem)* tail=COMMA?
    ;

aggregationClause options {logical='GroupBy';}
    : GROUP BY groupBy
    ;

groupBy options {logical='GroupBy';}
    : ALL                                                                          #groupByAll
    |                groupingElement (COMMA groupingElement)*                      #groupByDefault
    | expression (COMMA expression)*  WITH kind=( ROLLUP | CUBE )                  #groupByWith
    ;

groupingElement options {logical='Expr';}
    : groupingAnalytics                                                            #grpElementAnalytics
    | expression                                                                   #grpElementExpression
    ;

groupingAnalytics options {logical='GroupingSet';}
    : kind=(ROLLUP | CUBE) LPAREN groupingSet (COMMA groupingSet)* RPAREN          #grpAnalyticsSugar
    | GROUPING SETS LPAREN grpSetsElement (COMMA grpSetsElement)* RPAREN           #grpAnalyticsSets
    ;

grpSetsElement options {logical='GroupingSet';}
    : groupingAnalytics                                                            #grpSetsElementAnalytics
    | groupingSet                                                                  #grpSetsElementSet
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
    : ORDER BY (ALL | sortItem (COMMA sortItem)* tail+=COMMA?)
    ;

namedQuery options {logical='Arc<LogicalPlan>';}
    : name=identifier (columnAliases)? AS? LPAREN query RPAREN
    ;

selectItemAlias options {logical='Vec<Identifier>';}
    : identifier
    | columnAliases
    ;

selectItem options {logical='Vec<Expr>,*IntrinsicAlias';}
    : <logical='IntrinsicAlias'>
      expression (AS? selectItemAlias)?                          #selectSingle
    | multiSelect                                                #selectMulti
    ;

structItem options {logical='Vec<Expr>';}
    : expression (AS? identifier)?                               #structItemSingle
    | multiSelect                                                #structItemMulti
    ;

multiSelect options {logical='Vec<Expr>';}
    : selectStar
      (EXCEPT LPAREN exceptCols=multipartIdentifierList RPAREN)?
    ;

selectStar options {logical='Vec<Expr>';}
    :
       LPAREN selectStar RPAREN
    |
      primaryExpression DOT ASTERISK
    | ASTERISK
    ;

relation options {logical='Arc<LogicalPlan>';}
    : target=pivotedRelation
    ;

joinType options {logical='JoinType';}
    : INNER?
    | CROSS
    | LEFT OUTER?
    | LEFT? SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING LPAREN identifier (COMMA identifier)* tail=COMMA? RPAREN
    ;

sampledRelationTarget options {logical='Arc<LogicalPlan>';}
    : target=relationPrimary
    ;

sampledRelation options {logical='Arc<LogicalPlan>';}
    : sampledRelationTarget sample?
    ;

sample
    : TABLESAMPLE LPAREN sampleMethod? RPAREN (REPEATABLE LPAREN seed=INTEGER_VALUE RPAREN)?
    ;
sampleOperator
    : TABLESAMPLE LPAREN sampleMethod? RPAREN (REPEATABLE LPAREN seed=INTEGER_VALUE RPAREN)?
    ;
sampleMethod
    : negativeSign=MINUS? percentage=(INTEGER_VALUE | DECIMAL_VALUE) PERCENT_KW   #sampleByPercentile
    | expression ROWS                                                             #sampleByRows
    | sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE
        (ON (identifier | qualifiedName LPAREN RPAREN))?                 #sampleByBucket
    | bytes=expression                                                            #sampleByBytes
    ;

trimsSpecification
    : LEADING
    | TRAILING
    | BOTH
    ;

variableDefinition options {logical='Identifier';}
    : identifier AS expression
    ;

pivotedRelationTarget options {logical='Arc<LogicalPlan>';}
    : target=lateralViewRelation
    ;

lateralViewRelation options {logical='Arc<LogicalPlan>';}
    : lateralViewRelationTarget lateralView?
    ;

lateralViewRelationTarget options {logical='Arc<LogicalPlan>';}
    : extensibleRelation (COMMA extensibleRelation)*                #lateralViewRelationTargetDefault
    | lateralViewRelationTarget lateralView                         #lateralViewRelationTargetIncremental
    ;

extensibleRelation options {logical='Arc<LogicalPlan>';}
    : extensibleRelationTarget relationExtension?
    ;

extensibleRelationTarget options {logical='Arc<LogicalPlan>';}
    : LATERAL? aliasedRelation                                      #extensibleRelationTargetDefault
    | extensibleRelationTarget relationExtension                    #extensibleRelationTargetIncremental
    ;

relationExtension options {logical='Arc<LogicalPlan>';}
    : joinRelation             #relationExtensionJoin
    | pivotOperator            #relationExtensionPivot
    ;

joinRelation options {logical='Arc<LogicalPlan>';}
    : (joinType) JOIN LATERAL? right=aliasedRelation joinCriteria?  #joinRelationDefault
    | NATURAL joinType JOIN LATERAL? right=aliasedRelation          #joinRelationNatural
    ;
pivotedRelation options {logical='Arc<LogicalPlan>';}
    : pivotedRelationTarget pivotOperator*
    ;
pivotAggregates options {logical='Vec<Expr>';}
    : namedExpressionSeq
    ;

pivotFrom options {logical='Vec<Expr>';}
    : identifiers+=identifier
    | LPAREN identifiers+=identifier (COMMA identifiers+=identifier)* RPAREN
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
    : identifier
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
                                                                            #pivot
    | UNPIVOT unpivotNullClause? LPAREN columnUnpivot
      RPAREN pivotAsAlias                                                   #unpivot
    ;

aliasedRelationTarget options {logical='Arc<LogicalPlan>';}
    : STREAM        identifierReference           #streamTableTarget
    | STREAM LPAREN identifierReference RPAREN    #streamTableTarget
    | sampledRelation              #sampledRelationDefault
    | inlineTable                  #inlineTableDefault
    | STREAM? tableFunctionCall            #functionTableDefault
    ;

temporalClause
    : FOR? (SYSTEM_VERSION | VERSION) AS OF version
    | FOR? (SYSTEM_TIME | TIMESTAMP) AS OF timestamp=valueExpression
    ;

version
    : INTEGER_VALUE
    | string
    ;
aliasedRelation options {logical='Arc<LogicalPlan>';}
    : aliasedRelationTarget
    (AS? alias=strictIdentifier columnAliases?)?
    ;
columnAliases options {logical='Vec<Identifier>';}
    : LPAREN identifierSeq tail=COMMA? RPAREN
    ;

relationPrimary options {logical='Arc<LogicalPlan>,TableReference,*Span';}
    : <logical='Span'>
      tableNameRef=identifierReference temporalClause? optionsClause?            #tableName
    | LPAREN query RPAREN                                           #subqueryRelation
    | LPAREN rel=extensibleRelation RPAREN                              #parenthesizedRelation
    ;
tableFunctionCall options {logical='Arc<LogicalPlan>';}
    :
    functionName LPAREN (tableFunctionArgument (COMMA tableFunctionArgument)* tail+=COMMA?)?
      RPAREN                                                                                          #defaultTableFunctionCall
    ;

tableFunctionArgumentName options {logical='Identifier';}
    : identifier
    ;
tableFunctionArgument options {logical='Argument';}
    : (tableFunctionArgumentName '=>')? (tableArgument | expression)
    ;
tableArgument options {logical='Argument';}
    : tableArgumentRelation
        (PARTITION BY (LPAREN (expression (COMMA expression)* tail+=COMMA?)? RPAREN | expression))?
    ;
tableArgumentRelation options {logical='Argument,*Span';}
    : <logical='Span'>
      TABLE LPAREN qualifiedName RPAREN (AS? identifier columnAliases?)?  #tableArgumentTable
    | TABLE LPAREN query RPAREN (AS? identifier columnAliases?)?          #tableArgumentQuery
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
    | (NOT | BANG) booleanExpression                    #logicalNot
    | booleanExpression AND booleanExpression           #and
    | booleanExpression OR booleanExpression            #or
    ;


// workaround for https://github.com/antlr/antlr4/issues/780
comparisonPredicate options {logical='Expr,*Argument';}
    : comparisonOperator right=valueExpression                            #comparison
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
    | NOT? (REGEXP|RLIKE) valueExpression                               #regexp
    // TODO support quantified LIKE with UNNEST(array) on rhs
    | NOT? LIKE ( ALL | ANY | SOME ) LPAREN pattern+=valueExpression (COMMA pattern+=valueExpression)* RPAREN                  #quantifiedLike
    | NOT? (LIKE | ILIKE) pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
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
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    |
      <logical='IntrinsicAlias'>
      operator=(MINUS | PLUS | POSIX) valueExpression                                   #arithmeticUnary
    |
      <logical='IntrinsicAlias'>
      left=valueExpression operator=(ASTERISK | SLASH
      | PERCENT
      | DIV
      ) right=valueExpression                                                           #arithmeticBinary
    |
      <logical='IntrinsicAlias'>
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
    <logical='Identifier,TableReference'>
    name=(CURRENT_DATE | CURRENT_TIMESTAMP | CURRENT_USER | USER | SESSION_USER)             #currentLike
    | <logical='usize,f64,String,IntrinsicAlias'>
      constant                                                                              #constantDefault
    | <logical='Vec<Expr>'>
      LPAREN expression (COMMA expression)+ tail=COMMA? RPAREN                                    #rowConstructor
    | STRUCT LPAREN (argument+=structItem (COMMA argument+=structItem)*)? RPAREN        #structConstructor
    | POSITION LPAREN needle=valueExpression IN haystack=valueExpression RPAREN                                 #position
    | LISTAGG LPAREN setQuantifier? agg_exprs+=expression (COMMA agg_expr+=expression)? tail+=COMMA? RPAREN
        (WITHIN GROUP LPAREN ORDER BY sortItem (COMMA sortItem)* tail+=COMMA? RPAREN)?
        (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                     #listagg
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS LPAREN query RPAREN                                                                #exists
    | CASE operand=expression whenClause+ (ELSE elseExpression=expression)? END           #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | <logical='IntrinsicAlias'>
      CAST LPAREN expression AS type_ RPAREN                                                     #cast
    | <logical='IntrinsicAlias'>
      TRY_CAST LPAREN expression AS type_ RPAREN                                                 #cast
    | primaryExpression collateClause                                                      #collate
    | primaryExpression ':' jsonPath                                                        #jsonExtract
    | TRIM LPAREN (trimsSpecification? trimChar=valueExpression? FROM)
        trimSource=valueExpression RPAREN                                                    #trim
    | TRIM LPAREN (trimsSpecification trimChar=valueExpression? FROM?)?
        trimSource=valueExpression RPAREN                                                    #trim // only
    | TRIM LPAREN trimSource=valueExpression COMMA trimChar=valueExpression tail=COMMA? RPAREN    #trim
    | SUBSTRING LPAREN valueExpression FROM valueExpression (FOR valueExpression)? RPAREN       #substring
    | SUBSTR LPAREN valueExpression FROM valueExpression (FOR valueExpression)? RPAREN       #substring
    | EXTRACT LPAREN identifier FROM valueExpression RPAREN                                     #extract
    | FIRST LPAREN expression (IGNORE NULLS)? RPAREN                                  #first
    | ANY_VALUE LPAREN expression (IGNORE NULLS)? RPAREN                              #anyValue
    | LAST LPAREN expression (IGNORE NULLS)? RPAREN                                   #last
    | FROM_JSON LPAREN expression COMMA string RPAREN                                 #fromJson
    | <logical='IntrinsicAlias'> NAMED_STRUCT LPAREN (callArgument (COMMA callArgument)*)? RPAREN                        #namedStruct
    | MAP_FROM_ENTRIES LPAREN callArgument RPAREN                                             #mapFromEntries
    | ARRAYS_ZIP LPAREN (callArgument (COMMA callArgument)*)? RPAREN                             #arraysZip
    | <logical='IntrinsicAlias'> DECODE LPAREN (callArgument COMMA callArgument (COMMA callArgument)+)? RPAREN           #decode
    | COUNT LPAREN ASTERISK RPAREN functionCallTail                                          #countStar
    |
      <logical='IntrinsicAlias'>
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
    | <logical='IntrinsicAlias'>
      primaryExpression '?::' type_                                                       #tryCastOperator
    | <logical='IntrinsicAlias'>
      primaryExpression '::' type_                                                        #castOperator
    | value=primaryExpression LBRACKET index=valueExpression RBRACKET                               #subscript
    | <logical='TableReference,IntrinsicAlias'>
      base_=primaryExpression DOT fieldName=columnNameComponent                                     #dereference
    | <logical='Identifier,TableReference,Argument,IntrinsicAlias'>
      columnName                                                                          #columnReference
    | <logical='Identifier,IntrinsicAlias'>
      LPAREN expression RPAREN                                                                  #parenthesizedExpression
    | LBRACKET( expression (COMMA expression)* tail=COMMA? )? RBRACKET                                #array
    | VARIABLE                                                                            #variable
    | PERCENTILE_CONT LPAREN number RPAREN
      WITHIN GROUP LPAREN ORDER BY expression (ASC|DESC)? RPAREN
      (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                       #percentileContFunction
    | PERCENTILE_DISC LPAREN number RPAREN
      WITHIN GROUP LPAREN ORDER BY expression (ASC|DESC)? RPAREN
      (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                       #percentileDiscFunction
    | MODE LPAREN RPAREN
      WITHIN GROUP LPAREN ORDER BY expression (ASC|DESC)? RPAREN
      (OVER LPAREN (PARTITION BY expression (COMMA expression)* )? RPAREN)?                       #modeFunction
    | OVERLAY LPAREN input=valueExpression PLACING replace=valueExpression
      FROM position=valueExpression (FOR length=valueExpression)? RPAREN                         #overlay

    ;

functionCallHead
    :
    ;
functionCallTail options {logical='FunctionCallConstraint';}
    :  filter?
       nullTreatment? over?
    ;
callArgument options {logical='Expr,Argument,Vec<Expr>,Arguments,IntrinsicAlias';}
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    | multiSelect                   #multiArgument
    ;
functionExtraArguments options {logical='FunctionCallConstraint';}
    :
    ;

constant options {logical='Expr,usize,f64,String';}
    : NULL                                                                                     #nullLiteral
    | QUESTION_MARK                                                                                 #posParameterLiteral
    | COLON identifier                                                                         #namedParameterLiteral
    | interval                                                                                 #intervalLiteral
    | literalType string                                                                       #typeConstructor
    | <logical='usize,f64,String'>
      number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | <logical='String'>
      string                                                                                   #stringLiteral
    | <logical='String'>
      string string+                                                                           #stringConcatination

    ;
jsonPath options {logical='String';}
    : jsonPathElement1 jsonPathElement2*
    ;

jsonPathElement1
    : identifier
    | LBRACKET string RBRACKET
    | LBRACKET ASTERISK RBRACKET
    | LBRACKET INTEGER_VALUE RBRACKET
    ;

jsonPathElement2
    : '.' identifier
    | LBRACKET string RBRACKET
    | LBRACKET ASTERISK RBRACKET
    | LBRACKET INTEGER_VALUE RBRACKET
    ;

functionName options {logical='Vec<Identifier>';}
    : IDENTIFIER_KW LPAREN expression RPAREN
    | identFunc=IDENTIFIER_KW   // IDENTIFIER itself is also a valid function name.
    | qualifiedName
    | FILTER
    | LEFT
    | RIGHT
    | REGEXP
    ;

field
    : expression (AS identifier)?
    ;


nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

string options {logical='String';}
    :
      STRING                                #basicStringLiteral
    | DOUBLEQUOTED_STRING                  #doubleQuotedStringLiteral
    ;

timeZoneSpecifier
    : TIME ZONE interval
    | TIME ZONE string
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    | DOUBLE_EQ | NSEQ
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

standaloneInterval options {logical='Expr';}
    : interval EOF
    ;

interval options {logical='Expr';}
    : INTERVAL ( intervalValue intervalValueField )+
    ;

intervalValue options {logical='Expr';}
    : (PLUS | MINUS)?
      (INTEGER_VALUE | DECIMAL_VALUE | string)
    ;

intervalValueField options {logical='IntervalValueField';}
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND | NANOSECOND
    | YEARS | MONTHS | WEEKS | DAYS | HOURS | MINUTES | SECONDS | MILLISECONDS | MICROSECONDS | NANOSECONDS
    ;
intervalTypeField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;
typeIdentifier options {logical='Identifier';}
    : BOOLEAN
    | TINYINT | BYTE
    | SMALLINT | SHORT
    | INT | INTEGER
    | BIGINT | LONG
    | FLOAT | REAL
    | DOUBLE
    | DATE
    | TIMESTAMP | TIMESTAMP_NTZ | TIMESTAMP_LTZ
    | STRING_KW collateClause?
    | CHARACTER | CHAR
    | VARCHAR
    | BINARY
    | DECIMAL | DEC | NUMERIC
    | VOID
    | INTERVAL
    | VARIANT
    | ARRAY | STRUCT | MAP
    | unsupportedType=identifier
    ;

collateClause
    : COLLATE collationName=identifier
    ;

type_ options {logical='DataType,GenericDataType,Field';}
    : nonnullableType (NOT NULL)?                                                   #typeNotNull
    | NULL                                                                          #typeNull
    ;
nonnullableType options {logical='DataType,GenericDataType';}
    : DOLLAR INTEGER_VALUE                                                             #functionSignatureGenericType // SDF yml only
    | STRUCT (LT rowField (COMMA rowField)* tail=COMMA? GT | NEQ)                             #rowType
    | INTERVAL from=intervalTypeField (TO to=intervalTypeField)?                            #intervalType
    | MAP LT keyType=type_ COMMA valueType=type_ tail=COMMA? GT                     #legacyMapType
    | ARRAY LT type_ GT                                                           #legacyArrayType
    | FUNCTION LPAREN type_ (COMMA type_)* RPAREN                                           #lambdaType
    | typeIdentifier
     (LPAREN typeParameter (COMMA typeParameter)* tail=COMMA? RPAREN)?                      #primitiveType
    ;
rowField
    : identifier ':'? type_ (NOT NULL)? commentSpec?
    ;

commentSpec
    : COMMENT string
    ;

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
      frameExtent
    ;
frameExtent options {logical='WindowFrame';}
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound options {logical='WindowFrameBound';}
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame
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

identifier options {logical='Identifier,Option<UnquotedIdentifier>';}
    : strictIdentifier      #strictIdentifierDefault
    | strictNonReserved     #strictNonReservedIdentifier
    ;
strictIdentifier options {logical='Identifier,*Option<UnquotedIdentifier>';}
    : <logical='Option<UnquotedIdentifier>'>
      IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierDefault
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

quotedIdentifier options {logical='Identifier';}
    : BACKQUOTED_IDENTIFIER
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
    | MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? BIGINT_VALUE             #bigIntLiteral
    | MINUS? SMALLINT_VALUE           #smallIntLiteral
    | MINUS? TINYINT_VALUE            #tinyIntLiteral
    | MINUS? FLOAT_VALUE              #floatLiteral
    | MINUS? BIGDECIMAL_VALUE         #bigDecimalLiteral
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
strictNonReserved
    // This rule must only contain tokens. It should be disjoint from nonReserved.
    : ANTI
    | CROSS
    | EXCEPT
    | FULL
    | INNER
    | INTERSECT
    | JOIN
    | LATERAL
    | LEFT
    | MINUS_KW
    | NATURAL
    | ON
    | RIGHT
    | SEMI
    | UNION
    | USING
    ;
nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    :
      ADD | AFTER | ALL | ALTER | ALWAYS | ANALYZE | AND | ANY | ANY_VALUE | ARCHIVE | ARRAY | ARRAYS_ZIP | AS | ASC | AT | AUTHORIZATION
    | BEGIN | BETWEEN | BIGINT | BINARY | BINDING | BOOLEAN | BOTH | BUCKET | BUCKETS | BY | BYTE
    | CACHE | CALLED | CASCADE | CASE | CAST | CATALOG | CATALOGS | CHANGE | CHAR | CHARACTER | CHECK | CLEAR | CLUSTER | CLUSTERED | CODEGEN | COLLATE | COLLATION | COLLECTION
    | COLUMN | COLUMNS | COMMENT | COMMIT | COMPACT | COMPACTIONS | COMPENSATION | COMPUTE | CONCATENATE | CONSTRAINT | CONTAINS | COST | COUNT | CREATE | CUBE | CURRENT
    | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | CURRENT_USER
    | DATA | DATABASE | DATABASES | DATE | DATEADD | DATEDIFF | DATE_ADD | DATE_DIFF | DAY | DAYOFYEAR | DAYS | DBPROPERTIES | DEC | DECIMAL | DECLARE | DECODE | DEFAULT
    | DEFINED | DEFINER | DELETE | DELIMITED | DESC | DESCRIBE | DETERMINISTIC | DFS | DIRECTORIES | DIRECTORY | DISTINCT | DISTRIBUTE | DIV | DO | DOUBLE | DROP
    | ELSE | END | ESCAPE | ESCAPED | EVOLUTION | EXCHANGE | EXCLUDE | EXECUTE | EXISTS | EXPLAIN | EXPORT | EXTENDED | EXTERNAL | EXTRACT
    | FALSE | FETCH | FIELDS | FILEFORMAT | FILTER | FIRST | FLOAT | FOLLOWING | FOR | FOREIGN | FORMAT | FORMATTED | FROM | FROM_JSON | FUNCTION | FUNCTIONS
    | GENERATED | GLOBAL | GRANT | GROUP | GROUPING
    | HAVING | HOUR | HOURS
    | IDENTIFIER_KW | IDENTITY | IF | IGNORE | ILIKE | IMMEDIATE | IMPORT | IN | INCLUDE | INDEX | INDEXES | INPATH | INPUT | INPUTFORMAT | INSERT | INT | INTEGER | INTERVAL | INTO | INVOKER | IS | ITEMS
    | KEY | KEYS
    | LANGUAGE | LAST | LAZY | LEADING | LIKE | LIMIT | LINES | LIST | LISTAGG | LIVE | LOAD | LOCAL | LOCATION | LOCK | LOCKS | LOGICAL | LONG
    | MACRO | MAP | MAP_FROM_ENTRIES| MATCHED | MATERIALIZED | MERGE | MICROSECOND | MICROSECONDS | MILLISECOND | MILLISECONDS | MINUTE | MINUTES | MODE | MODIFIES | MONTH | MONTHS | MSCK
    | NAME | NAMESPACE | NAMESPACES | NAMED_STRUCT | NANOSECOND | NANOSECONDS | NO | NONE | NOT | NULL | NULLS | NUMERIC
    | OF | OFFSET | ONLY | OPTIMIZE | OPTION | OPTIONS | OR | ORDER | OUT | OUTER | OUTPUTFORMAT | OVER | OVERLAPS | OVERLAY | OVERWRITE
    | PARTITION | PARTITIONED | PARTITIONS | PERCENTILE_CONT | PERCENTILE_DISC | PERCENT_KW | PIVOT | PLACING | POSITION | PRECEDING | PRIMARY | PRINCIPALS | PROPERTIES | PRUNE | PURGE
    | QUALIFY | QUARTER | QUERY
    | RANGE | READS | REAL | RECORDREADER | RECORDWRITER | RECOVER | RECURSIVE | REDUCE | REFERENCE | REFERENCES | REFRESH | REGEXP | RENAME | REPAIR | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNS | REVOKE | RLIKE | ROLE | ROLES | ROLLBACK | ROLLUP | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECONDS | SECURITY | SELECT | SEPARATED | SERDE | SERDEPROPERTIES | SESSION_USER | SET | SETS | SHORT | SHOW | SINGLE | SKEWED | SMALLINT | SOME | SORT | SORTED | SOURCE | SPECIFIC | SQL | START | STATISTICS | STORED | STRATIFY | STREAM | STREAMING
    | STRING_KW
    | STRUCT | SUBSTR | SUBSTRING | SYNC | SYSTEM_TIME | SYSTEM_VERSION
    | TABLE | TABLES | TABLESAMPLE | TARGET | TBLPROPERTIES | TEMP | TEMPORARY | TERMINATED | THEN | TIME | TIMEDIFF | TIMESTAMP | TIMESTAMPADD | TIMESTAMPDIFF | TIMESTAMP_LTZ | TIMESTAMP_NTZ | TINYINT | TO | TOUCH | TRAILING | TRANSACTION | TRANSACTIONS | TRANSFORM | TRIM | TRUE | TRUNCATE | TRY_CAST | TYPE
    | UNARCHIVE | UNBOUNDED | UNCACHE | UNIQUE | UNKNOWN | UNLOCK | UNPIVOT | UNSET | UPDATE | USE | USER
    | VALUES | VAR | VARCHAR | VARIANT | VERSION | VIEW | VIEWS | VOID
    | WEEK | WEEKS | WHEN | WHERE | WHILE | WINDOW | WITH | WITHIN
    | X_KW
    | YEAR | YEARS
    | ZONE
    ;

ADD: 'ADD';
AFTER: 'AFTER';
ALL: 'ALL';
ALTER: 'ALTER';
ALWAYS: 'ALWAYS';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANTI: 'ANTI';
ANY: 'ANY';
ANY_VALUE: 'ANY_VALUE';
ARCHIVE: 'ARCHIVE';
ARRAY: 'ARRAY';
ARRAYS_ZIP: 'ARRAYS_ZIP';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
AUTHORIZATION: 'AUTHORIZATION';
BEGIN: 'BEGIN';
BETWEEN: 'BETWEEN';
BIGINT: 'BIGINT';
BINARY: 'BINARY';
X_KW: 'X';
BINDING: 'BINDING';
BOOLEAN: 'BOOLEAN';
BOTH: 'BOTH';
BUCKET: 'BUCKET';
BUCKETS: 'BUCKETS';
BY: 'BY';
BYTE: 'BYTE';
CACHE: 'CACHE';
CALLED: 'CALLED';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
CHANGE: 'CHANGE';
CHAR: 'CHAR';
CHARACTER: 'CHARACTER';
CHECK: 'CHECK';
CLEAR: 'CLEAR';
CLUSTER: 'CLUSTER';
CLUSTERED: 'CLUSTERED';
CODEGEN: 'CODEGEN';
COLLATE: 'COLLATE';
COLLATION: 'COLLATION';
COLLECTION: 'COLLECTION';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMA: ',';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMPACT: 'COMPACT';
COMPACTIONS: 'COMPACTIONS';
COMPENSATION: 'COMPENSATION';
COMPUTE: 'COMPUTE';
CONCATENATE: 'CONCATENATE';
CONSTRAINT: 'CONSTRAINT';
CONTAINS: 'CONTAINS';
COST: 'COST';
COUNT: 'COUNT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DAY: 'DAY';
DAYS: 'DAYS';
DAYOFYEAR: 'DAYOFYEAR';
DATA: 'DATA';
DATE: 'DATE';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATEADD: 'DATEADD';
DATE_ADD: 'DATE_ADD';
DATEDIFF: 'DATEDIFF';
DATE_DIFF: 'DATE_DIFF';
DBPROPERTIES: 'DBPROPERTIES';
DEC: 'DEC';
DECIMAL: 'DECIMAL';
DECLARE: 'DECLARE';
DECODE: 'DECODE';
DEFAULT: 'DEFAULT';
DEFINED: 'DEFINED';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DELIMITED: 'DELIMITED';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETERMINISTIC: 'DETERMINISTIC';
DFS: 'DFS';
DIRECTORIES: 'DIRECTORIES';
DIRECTORY: 'DIRECTORY';
DISTINCT: 'DISTINCT';
DISTRIBUTE: 'DISTRIBUTE';
DIV: 'DIV';
DO: 'DO';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
ESCAPED: 'ESCAPED';
EVOLUTION: 'EVOLUTION';
EXCEPT: 'EXCEPT';
EXCHANGE: 'EXCHANGE';
EXCLUDE: 'EXCLUDE';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXPORT: 'EXPORT';
EXTENDED: 'EXTENDED';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FETCH: 'FETCH';
FIELDS: 'FIELDS';
FILTER: 'FILTER';
FILEFORMAT: 'FILEFORMAT';
FIRST: 'FIRST';
FLOAT: 'FLOAT';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FOREIGN: 'FOREIGN';
FORMAT: 'FORMAT';
FORMATTED: 'FORMATTED';
FROM: 'FROM';
FROM_JSON: 'FROM_JSON';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GENERATED: 'GENERATED';
GLOBAL: 'GLOBAL';
GRANT: 'GRANT';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HOUR: 'HOUR';
HOURS: 'HOURS';
IDENTIFIER_KW: 'IDENTIFIER';
IDENTITY: 'IDENTITY';
IF: 'IF';
IGNORE: 'IGNORE';
IMMEDIATE: 'IMMEDIATE';
IMPORT: 'IMPORT';
IN: 'IN';
INCLUDE: 'INCLUDE';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
INNER: 'INNER';
INPATH: 'INPATH';
INPUT: 'INPUT';
INPUTFORMAT: 'INPUTFORMAT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INT: 'INT';
INTEGER: 'INTEGER';
INTO: 'INTO';
INVOKER: 'INVOKER';
IS: 'IS';
ITEMS: 'ITEMS';
ILIKE: 'ILIKE';
JOIN: 'JOIN';
KEY: 'KEY';
KEYS: 'KEYS';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LATERAL: 'LATERAL';
LAZY: 'LAZY';
LEADING: 'LEADING';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LIST: 'LIST';
LISTAGG: 'LISTAGG';
LIVE: 'LIVE';
LOAD: 'LOAD';
LOCAL: 'LOCAL';
LOCATION: 'LOCATION';
LOCK: 'LOCK';
LOCKS: 'LOCKS';
LOGICAL: 'LOGICAL';
LONG: 'LONG';
MACRO: 'MACRO';
MAP: 'MAP';
MAP_FROM_ENTRIES: 'MAP_FROM_ENTRIES';
MATCHED: 'MATCHED';
MATERIALIZED: 'MATERIALIZED';
MERGE: 'MERGE';
MICROSECOND: 'MICROSECOND';
MICROSECONDS: 'MICROSECONDS';
MILLISECOND: 'MILLISECOND';
MILLISECONDS: 'MILLISECONDS';
MINUS_KW:'MINUS';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MODE: 'MODE';
MODIFIES: 'MODIFIES';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
MSCK: 'MSCK';
NAME: 'NAME';
NAMESPACE: 'NAMESPACE';
NAMESPACES: 'NAMESPACES';
NAMED_STRUCT: 'NAMED_STRUCT';
NANOSECOND: 'NANOSECOND';
NANOSECONDS: 'NANOSECONDS';
NATURAL: 'NATURAL';
NO: 'NO';
NONE: 'NONE';
NOT: 'NOT';
NULL: 'NULL';
NULLS: 'NULLS';
NUMERIC: 'NUMERIC';
OF: 'OF';
OFFSET: 'OFFSET';
ON: 'ON';
ONLY: 'ONLY';
OPTIMIZE: 'OPTIMIZE';
OPTION: 'OPTION';
OPTIONS: 'OPTIONS';
OR: 'OR';
ORDER: 'ORDER';
OUT: 'OUT';
OUTER: 'OUTER';
OUTPUTFORMAT: 'OUTPUTFORMAT';
OVER: 'OVER';
OVERLAPS: 'OVERLAPS';
OVERLAY: 'OVERLAY';
OVERWRITE: 'OVERWRITE';
PARTITION: 'PARTITION';
PARTITIONED: 'PARTITIONED';
PARTITIONS: 'PARTITIONS';
PERCENT_KW: 'PERCENT';
PERCENTILE_CONT: 'PERCENTILE_CONT';
PERCENTILE_DISC: 'PERCENTILE_DISC';
PIVOT: 'PIVOT';
PLACING: 'PLACING';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PRIMARY: 'PRIMARY';
PRINCIPALS: 'PRINCIPALS';
PROPERTIES: 'PROPERTIES';
PRUNE: 'PRUNE';
PURGE: 'PURGE';
QUALIFY: 'QUALIFY';
QUARTER: 'QUARTER';
QUERY: 'QUERY';
RANGE: 'RANGE';
READS: 'READS';
REAL: 'REAL';
RECORDREADER: 'RECORDREADER';
RECORDWRITER: 'RECORDWRITER';
RECOVER: 'RECOVER';
RECURSIVE: 'RECURSIVE';
REDUCE: 'REDUCE';
REGEXP: 'REGEXP';
REFERENCE: 'REFERENCE';
REFERENCES: 'REFERENCES';
REFRESH: 'REFRESH';
RENAME: 'RENAME';
REPAIR: 'REPAIR';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECURITY: 'SECURITY';
SELECT: 'SELECT';
SEMI: 'SEMI';
SEPARATED: 'SEPARATED';
SERDE: 'SERDE';
SERDEPROPERTIES: 'SERDEPROPERTIES';
SESSION_USER: 'SESSION_USER';
SET: 'SET';
SETS: 'SETS';
SHORT: 'SHORT';
SHOW: 'SHOW';
SINGLE: 'SINGLE';
SKEWED: 'SKEWED';
SMALLINT: 'SMALLINT';
SOME: 'SOME';
SORT: 'SORT';
SORTED: 'SORTED';
SOURCE: 'SOURCE';
SPECIFIC: 'SPECIFIC';
SQL: 'SQL';
START: 'START';
STATISTICS: 'STATISTICS';
STORED: 'STORED';
STRATIFY: 'STRATIFY';
STREAM: 'STREAM';
STREAMING: 'STREAMING';
STRUCT: 'STRUCT';
SUBSTR: 'SUBSTR';
SUBSTRING: 'SUBSTRING';
SYNC: 'SYNC';
SYSTEM_TIME: 'SYSTEM_TIME';
SYSTEM_VERSION: 'SYSTEM_VERSION';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TARGET: 'TARGET';
TBLPROPERTIES: 'TBLPROPERTIES';
TEMP: 'TEMP';
TEMPORARY: 'TEMPORARY';
TERMINATED: 'TERMINATED';
STRING_KW: 'STRING';
THEN: 'THEN';
TIME: 'TIME';
TIMEDIFF: 'TIMEDIFF';
TIMESTAMP: 'TIMESTAMP';
TIMESTAMPADD: 'TIMESTAMPADD';
TIMESTAMPDIFF: 'TIMESTAMPDIFF';
TIMESTAMP_LTZ: 'TIMESTAMP_LTZ';
TIMESTAMP_NTZ: 'TIMESTAMP_NTZ';
TINYINT: 'TINYINT';
TO: 'TO';
TOUCH: 'TOUCH';
TRAILING: 'TRAILING';
TRANSACTION: 'TRANSACTION';
TRANSACTIONS: 'TRANSACTIONS';
TRANSFORM: 'TRANSFORM';
TRIM: 'TRIM';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UNARCHIVE: 'UNARCHIVE';
UNBOUNDED: 'UNBOUNDED';
UNCACHE: 'UNCACHE';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNKNOWN: 'UNKNOWN';
UNLOCK: 'UNLOCK';
UNPIVOT: 'UNPIVOT';
UNSET: 'UNSET';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALUES: 'VALUES';
VAR: 'VAR';
VARCHAR: 'VARCHAR';
VARIANT: 'VARIANT';
VERSION: 'VERSION';
VIEW: 'VIEW';
VIEWS: 'VIEWS';
VOID: 'VOID';
WEEK: 'WEEK';
WEEKS: 'WEEKS';
WHEN: 'WHEN';
WHERE: 'WHERE';
WHILE: 'WHILE';
WINDOW: 'WINDOW';
WITH: 'WITH';
WITHIN: 'WITHIN';
YEAR: 'YEAR';
YEARS: 'YEARS';
ZONE: 'ZONE';

LPAREN: '(';
RPAREN: ')';
LBRACKET: '[';
RBRACKET: ']';
DOT: '.';
EQ: '=';
DOUBLE_EQ: '==';
BANG: '!';
NSEQ: '<=>';
HENT_START: '/*+';
HENT_END: '*/';
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

STRING
    : '\'' ( ~('\''|'\\') | ESCAPE_SEQUENCE )* '\''
    | 'R\'' (~'\'')* '\''
    | 'R"'(~'"')* '"'
    ;

DOUBLEQUOTED_STRING
    :'"' ( ~('"'|'\\') | ESCAPE_SEQUENCE )* '"'
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;


INTEGER_VALUE
    : DIGIT+
    ;

BIGINT_VALUE
    : DIGIT+ 'L'
    ;

SMALLINT_VALUE
    : DIGIT+ 'S'
    ;

TINYINT_VALUE
    : DIGIT+ 'Y'
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT       { crate::lexer_support::is_valid_decimal_boundary(recog) }?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS                { crate::lexer_support::is_valid_decimal_boundary(recog) }?
    ;

FLOAT_VALUE
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F'  { crate::lexer_support::is_valid_decimal_boundary(recog) }?
    ;

DOUBLE_VALUE
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D'  { crate::lexer_support::is_valid_decimal_boundary(recog) }?
    ;

BIGDECIMAL_VALUE
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' { crate::lexer_support::is_valid_decimal_boundary(recog) }?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')* (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
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
fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
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
