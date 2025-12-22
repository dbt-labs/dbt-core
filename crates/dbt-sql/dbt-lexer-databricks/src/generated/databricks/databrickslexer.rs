// Generated from crates/dbt-sql/dbt-parser-databricks/src/Databricks.g4 by ANTLR 4.13.2
#![allow(dead_code)]
#![allow(nonstandard_style)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use dbt_antlr4::atn::ATN;
use dbt_antlr4::char_stream::CharStream;
use dbt_antlr4::int_stream::IntStream;
use dbt_antlr4::tree::ParseTree;
use dbt_antlr4::lexer::{BaseLexer, Lexer, LexerRecog};
use dbt_antlr4::atn_deserializer::ATNDeserializer;
use dbt_antlr4::dfa::DFA;
use dbt_antlr4::lexer_atn_simulator::{LexerATNSimulator, ILexerATNSimulator};
use dbt_antlr4::PredictionContextCache;
use dbt_antlr4::recognizer::{Recognizer,Actions};
use dbt_antlr4::error_listener::ErrorListener;
use dbt_antlr4::TokenSource;
use dbt_antlr4::token_factory::{TokenFactory,CommonTokenFactory,TokenAware};
use dbt_antlr4::token::*;
use dbt_antlr4::rule_context::{BaseRuleContext,EmptyCustomRuleContext,EmptyContext};
use dbt_antlr4::parser_rule_context::{ParserRuleContext,BaseParserRuleContext,cast};
use dbt_antlr4::vocabulary::{Vocabulary,VocabularyImpl};

use dbt_antlr4::{Tid,TidAble,TidExt};

use std::sync::LazyLock;
use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};


	pub const T__0:i32=1; 
	pub const T__1:i32=2; 
	pub const T__2:i32=3; 
	pub const T__3:i32=4; 
	pub const ADD:i32=5; 
	pub const AFTER:i32=6; 
	pub const ALL:i32=7; 
	pub const ALTER:i32=8; 
	pub const ALWAYS:i32=9; 
	pub const ANALYZE:i32=10; 
	pub const AND:i32=11; 
	pub const ANTI:i32=12; 
	pub const ANY:i32=13; 
	pub const ANY_VALUE:i32=14; 
	pub const ARCHIVE:i32=15; 
	pub const ARRAY:i32=16; 
	pub const ARRAYS_ZIP:i32=17; 
	pub const AS:i32=18; 
	pub const ASC:i32=19; 
	pub const AT:i32=20; 
	pub const AUTHORIZATION:i32=21; 
	pub const BEGIN:i32=22; 
	pub const BETWEEN:i32=23; 
	pub const BIGINT:i32=24; 
	pub const BINARY:i32=25; 
	pub const X_KW:i32=26; 
	pub const BINDING:i32=27; 
	pub const BOOLEAN:i32=28; 
	pub const BOTH:i32=29; 
	pub const BUCKET:i32=30; 
	pub const BUCKETS:i32=31; 
	pub const BY:i32=32; 
	pub const BYTE:i32=33; 
	pub const CACHE:i32=34; 
	pub const CALLED:i32=35; 
	pub const CASCADE:i32=36; 
	pub const CASE:i32=37; 
	pub const CAST:i32=38; 
	pub const CATALOG:i32=39; 
	pub const CATALOGS:i32=40; 
	pub const CHANGE:i32=41; 
	pub const CHAR:i32=42; 
	pub const CHARACTER:i32=43; 
	pub const CHECK:i32=44; 
	pub const CLEAR:i32=45; 
	pub const CLUSTER:i32=46; 
	pub const CLUSTERED:i32=47; 
	pub const CODEGEN:i32=48; 
	pub const COLLATE:i32=49; 
	pub const COLLATION:i32=50; 
	pub const COLLECTION:i32=51; 
	pub const COLUMN:i32=52; 
	pub const COLUMNS:i32=53; 
	pub const COMMA:i32=54; 
	pub const COMMENT:i32=55; 
	pub const COMMIT:i32=56; 
	pub const COMPACT:i32=57; 
	pub const COMPACTIONS:i32=58; 
	pub const COMPENSATION:i32=59; 
	pub const COMPUTE:i32=60; 
	pub const CONCATENATE:i32=61; 
	pub const CONSTRAINT:i32=62; 
	pub const CONTAINS:i32=63; 
	pub const COST:i32=64; 
	pub const COUNT:i32=65; 
	pub const CREATE:i32=66; 
	pub const CROSS:i32=67; 
	pub const CUBE:i32=68; 
	pub const CURRENT:i32=69; 
	pub const CURRENT_DATE:i32=70; 
	pub const CURRENT_TIME:i32=71; 
	pub const CURRENT_TIMESTAMP:i32=72; 
	pub const CURRENT_USER:i32=73; 
	pub const DAY:i32=74; 
	pub const DAYS:i32=75; 
	pub const DAYOFYEAR:i32=76; 
	pub const DATA:i32=77; 
	pub const DATE:i32=78; 
	pub const DATABASE:i32=79; 
	pub const DATABASES:i32=80; 
	pub const DATEADD:i32=81; 
	pub const DATE_ADD:i32=82; 
	pub const DATEDIFF:i32=83; 
	pub const DATE_DIFF:i32=84; 
	pub const DBPROPERTIES:i32=85; 
	pub const DEC:i32=86; 
	pub const DECIMAL:i32=87; 
	pub const DECLARE:i32=88; 
	pub const DECODE:i32=89; 
	pub const DEFAULT:i32=90; 
	pub const DEFINED:i32=91; 
	pub const DEFINER:i32=92; 
	pub const DELETE:i32=93; 
	pub const DELIMITED:i32=94; 
	pub const DESC:i32=95; 
	pub const DESCRIBE:i32=96; 
	pub const DETERMINISTIC:i32=97; 
	pub const DFS:i32=98; 
	pub const DIRECTORIES:i32=99; 
	pub const DIRECTORY:i32=100; 
	pub const DISTINCT:i32=101; 
	pub const DISTRIBUTE:i32=102; 
	pub const DIV:i32=103; 
	pub const DO:i32=104; 
	pub const DOUBLE:i32=105; 
	pub const DROP:i32=106; 
	pub const ELSE:i32=107; 
	pub const END:i32=108; 
	pub const ESCAPE:i32=109; 
	pub const ESCAPED:i32=110; 
	pub const EVOLUTION:i32=111; 
	pub const EXCEPT:i32=112; 
	pub const EXCHANGE:i32=113; 
	pub const EXCLUDE:i32=114; 
	pub const EXECUTE:i32=115; 
	pub const EXISTS:i32=116; 
	pub const EXPLAIN:i32=117; 
	pub const EXPORT:i32=118; 
	pub const EXTENDED:i32=119; 
	pub const EXTERNAL:i32=120; 
	pub const EXTRACT:i32=121; 
	pub const FALSE:i32=122; 
	pub const FETCH:i32=123; 
	pub const FIELDS:i32=124; 
	pub const FILTER:i32=125; 
	pub const FILEFORMAT:i32=126; 
	pub const FIRST:i32=127; 
	pub const FLOAT:i32=128; 
	pub const FOLLOWING:i32=129; 
	pub const FOR:i32=130; 
	pub const FOREIGN:i32=131; 
	pub const FORMAT:i32=132; 
	pub const FORMATTED:i32=133; 
	pub const FROM:i32=134; 
	pub const FROM_JSON:i32=135; 
	pub const FULL:i32=136; 
	pub const FUNCTION:i32=137; 
	pub const FUNCTIONS:i32=138; 
	pub const GENERATED:i32=139; 
	pub const GLOBAL:i32=140; 
	pub const GRANT:i32=141; 
	pub const GROUP:i32=142; 
	pub const GROUPING:i32=143; 
	pub const HAVING:i32=144; 
	pub const HOUR:i32=145; 
	pub const HOURS:i32=146; 
	pub const IDENTIFIER_KW:i32=147; 
	pub const IDENTITY:i32=148; 
	pub const IF:i32=149; 
	pub const IGNORE:i32=150; 
	pub const IMMEDIATE:i32=151; 
	pub const IMPORT:i32=152; 
	pub const IN:i32=153; 
	pub const INCLUDE:i32=154; 
	pub const INDEX:i32=155; 
	pub const INDEXES:i32=156; 
	pub const INNER:i32=157; 
	pub const INPATH:i32=158; 
	pub const INPUT:i32=159; 
	pub const INPUTFORMAT:i32=160; 
	pub const INSERT:i32=161; 
	pub const INTERSECT:i32=162; 
	pub const INTERVAL:i32=163; 
	pub const INT:i32=164; 
	pub const INTEGER:i32=165; 
	pub const INTO:i32=166; 
	pub const INVOKER:i32=167; 
	pub const IS:i32=168; 
	pub const ITEMS:i32=169; 
	pub const ILIKE:i32=170; 
	pub const JOIN:i32=171; 
	pub const KEY:i32=172; 
	pub const KEYS:i32=173; 
	pub const LANGUAGE:i32=174; 
	pub const LAST:i32=175; 
	pub const LATERAL:i32=176; 
	pub const LAZY:i32=177; 
	pub const LEADING:i32=178; 
	pub const LEFT:i32=179; 
	pub const LIKE:i32=180; 
	pub const LIMIT:i32=181; 
	pub const LINES:i32=182; 
	pub const LIST:i32=183; 
	pub const LISTAGG:i32=184; 
	pub const LIVE:i32=185; 
	pub const LOAD:i32=186; 
	pub const LOCAL:i32=187; 
	pub const LOCATION:i32=188; 
	pub const LOCK:i32=189; 
	pub const LOCKS:i32=190; 
	pub const LOGICAL:i32=191; 
	pub const LONG:i32=192; 
	pub const MACRO:i32=193; 
	pub const MAP:i32=194; 
	pub const MAP_FROM_ENTRIES:i32=195; 
	pub const MATCHED:i32=196; 
	pub const MATERIALIZED:i32=197; 
	pub const MERGE:i32=198; 
	pub const MICROSECOND:i32=199; 
	pub const MICROSECONDS:i32=200; 
	pub const MILLISECOND:i32=201; 
	pub const MILLISECONDS:i32=202; 
	pub const MINUS_KW:i32=203; 
	pub const MINUTE:i32=204; 
	pub const MINUTES:i32=205; 
	pub const MODE:i32=206; 
	pub const MODIFIES:i32=207; 
	pub const MONTH:i32=208; 
	pub const MONTHS:i32=209; 
	pub const MSCK:i32=210; 
	pub const NAME:i32=211; 
	pub const NAMESPACE:i32=212; 
	pub const NAMESPACES:i32=213; 
	pub const NAMED_STRUCT:i32=214; 
	pub const NANOSECOND:i32=215; 
	pub const NANOSECONDS:i32=216; 
	pub const NATURAL:i32=217; 
	pub const NO:i32=218; 
	pub const NONE:i32=219; 
	pub const NOT:i32=220; 
	pub const NULL:i32=221; 
	pub const NULLS:i32=222; 
	pub const NUMERIC:i32=223; 
	pub const OF:i32=224; 
	pub const OFFSET:i32=225; 
	pub const ON:i32=226; 
	pub const ONLY:i32=227; 
	pub const OPTIMIZE:i32=228; 
	pub const OPTION:i32=229; 
	pub const OPTIONS:i32=230; 
	pub const OR:i32=231; 
	pub const ORDER:i32=232; 
	pub const OUT:i32=233; 
	pub const OUTER:i32=234; 
	pub const OUTPUTFORMAT:i32=235; 
	pub const OVER:i32=236; 
	pub const OVERLAPS:i32=237; 
	pub const OVERLAY:i32=238; 
	pub const OVERWRITE:i32=239; 
	pub const PARTITION:i32=240; 
	pub const PARTITIONED:i32=241; 
	pub const PARTITIONS:i32=242; 
	pub const PERCENT_KW:i32=243; 
	pub const PERCENTILE_CONT:i32=244; 
	pub const PERCENTILE_DISC:i32=245; 
	pub const PIVOT:i32=246; 
	pub const PLACING:i32=247; 
	pub const POSITION:i32=248; 
	pub const PRECEDING:i32=249; 
	pub const PRIMARY:i32=250; 
	pub const PRINCIPALS:i32=251; 
	pub const PROPERTIES:i32=252; 
	pub const PRUNE:i32=253; 
	pub const PURGE:i32=254; 
	pub const QUALIFY:i32=255; 
	pub const QUARTER:i32=256; 
	pub const QUERY:i32=257; 
	pub const RANGE:i32=258; 
	pub const READS:i32=259; 
	pub const REAL:i32=260; 
	pub const RECORDREADER:i32=261; 
	pub const RECORDWRITER:i32=262; 
	pub const RECOVER:i32=263; 
	pub const RECURSIVE:i32=264; 
	pub const REDUCE:i32=265; 
	pub const REGEXP:i32=266; 
	pub const REFERENCE:i32=267; 
	pub const REFERENCES:i32=268; 
	pub const REFRESH:i32=269; 
	pub const RENAME:i32=270; 
	pub const REPAIR:i32=271; 
	pub const REPEATABLE:i32=272; 
	pub const REPLACE:i32=273; 
	pub const RESET:i32=274; 
	pub const RESPECT:i32=275; 
	pub const RESTRICT:i32=276; 
	pub const RETURN:i32=277; 
	pub const RETURNS:i32=278; 
	pub const REVOKE:i32=279; 
	pub const RIGHT:i32=280; 
	pub const RLIKE:i32=281; 
	pub const ROLE:i32=282; 
	pub const ROLES:i32=283; 
	pub const ROLLBACK:i32=284; 
	pub const ROLLUP:i32=285; 
	pub const ROW:i32=286; 
	pub const ROWS:i32=287; 
	pub const SECOND:i32=288; 
	pub const SECONDS:i32=289; 
	pub const SCHEMA:i32=290; 
	pub const SCHEMAS:i32=291; 
	pub const SECURITY:i32=292; 
	pub const SELECT:i32=293; 
	pub const SEMI:i32=294; 
	pub const SEPARATED:i32=295; 
	pub const SERDE:i32=296; 
	pub const SERDEPROPERTIES:i32=297; 
	pub const SESSION_USER:i32=298; 
	pub const SET:i32=299; 
	pub const SETS:i32=300; 
	pub const SHORT:i32=301; 
	pub const SHOW:i32=302; 
	pub const SINGLE:i32=303; 
	pub const SKEWED:i32=304; 
	pub const SMALLINT:i32=305; 
	pub const SOME:i32=306; 
	pub const SORT:i32=307; 
	pub const SORTED:i32=308; 
	pub const SOURCE:i32=309; 
	pub const SPECIFIC:i32=310; 
	pub const SQL:i32=311; 
	pub const START:i32=312; 
	pub const STATISTICS:i32=313; 
	pub const STORED:i32=314; 
	pub const STRATIFY:i32=315; 
	pub const STREAM:i32=316; 
	pub const STREAMING:i32=317; 
	pub const STRING_AGG:i32=318; 
	pub const STRUCT:i32=319; 
	pub const SUBSTR:i32=320; 
	pub const SUBSTRING:i32=321; 
	pub const SYNC:i32=322; 
	pub const SYSTEM_TIME:i32=323; 
	pub const SYSTEM_VERSION:i32=324; 
	pub const TABLE:i32=325; 
	pub const TABLES:i32=326; 
	pub const TABLESAMPLE:i32=327; 
	pub const TARGET:i32=328; 
	pub const TBLPROPERTIES:i32=329; 
	pub const TEMP:i32=330; 
	pub const TEMPORARY:i32=331; 
	pub const TERMINATED:i32=332; 
	pub const STRING_KW:i32=333; 
	pub const THEN:i32=334; 
	pub const TIME:i32=335; 
	pub const TIMEDIFF:i32=336; 
	pub const TIMESTAMP:i32=337; 
	pub const TIMESTAMPADD:i32=338; 
	pub const TIMESTAMPDIFF:i32=339; 
	pub const TIMESTAMP_LTZ:i32=340; 
	pub const TIMESTAMP_NTZ:i32=341; 
	pub const TINYINT:i32=342; 
	pub const TO:i32=343; 
	pub const TOUCH:i32=344; 
	pub const TRAILING:i32=345; 
	pub const TRANSACTION:i32=346; 
	pub const TRANSACTIONS:i32=347; 
	pub const TRANSFORM:i32=348; 
	pub const TRIM:i32=349; 
	pub const TRUE:i32=350; 
	pub const TRUNCATE:i32=351; 
	pub const TRY_CAST:i32=352; 
	pub const TYPE:i32=353; 
	pub const UNARCHIVE:i32=354; 
	pub const UNBOUNDED:i32=355; 
	pub const UNCACHE:i32=356; 
	pub const UNION:i32=357; 
	pub const UNIQUE:i32=358; 
	pub const UNKNOWN:i32=359; 
	pub const UNLOCK:i32=360; 
	pub const UNPIVOT:i32=361; 
	pub const UNSET:i32=362; 
	pub const UPDATE:i32=363; 
	pub const USE:i32=364; 
	pub const USER:i32=365; 
	pub const USING:i32=366; 
	pub const VALUES:i32=367; 
	pub const VAR:i32=368; 
	pub const VARCHAR:i32=369; 
	pub const VARIANT:i32=370; 
	pub const VERSION:i32=371; 
	pub const VIEW:i32=372; 
	pub const VIEWS:i32=373; 
	pub const VOID:i32=374; 
	pub const WEEK:i32=375; 
	pub const WEEKS:i32=376; 
	pub const WHEN:i32=377; 
	pub const WHERE:i32=378; 
	pub const WHILE:i32=379; 
	pub const WINDOW:i32=380; 
	pub const WITH:i32=381; 
	pub const WITHIN:i32=382; 
	pub const YEAR:i32=383; 
	pub const YEARS:i32=384; 
	pub const ZONE:i32=385; 
	pub const LPAREN:i32=386; 
	pub const RPAREN:i32=387; 
	pub const LBRACKET:i32=388; 
	pub const RBRACKET:i32=389; 
	pub const DOT:i32=390; 
	pub const EQ:i32=391; 
	pub const DOUBLE_EQ:i32=392; 
	pub const BANG:i32=393; 
	pub const NSEQ:i32=394; 
	pub const HENT_START:i32=395; 
	pub const HENT_END:i32=396; 
	pub const NEQ:i32=397; 
	pub const LT:i32=398; 
	pub const LTE:i32=399; 
	pub const GT:i32=400; 
	pub const GTE:i32=401; 
	pub const PLUS:i32=402; 
	pub const MINUS:i32=403; 
	pub const ASTERISK:i32=404; 
	pub const SLASH:i32=405; 
	pub const PERCENT:i32=406; 
	pub const CONCAT:i32=407; 
	pub const QUESTION_MARK:i32=408; 
	pub const SEMI_COLON:i32=409; 
	pub const COLON:i32=410; 
	pub const DOLLAR:i32=411; 
	pub const BITWISE_AND:i32=412; 
	pub const BITWISE_OR:i32=413; 
	pub const BITWISE_XOR:i32=414; 
	pub const BITWISE_SHIFT_LEFT:i32=415; 
	pub const POSIX:i32=416; 
	pub const ESCAPE_SEQUENCE:i32=417; 
	pub const STRING:i32=418; 
	pub const DOUBLEQUOTED_STRING:i32=419; 
	pub const UNICODE_STRING:i32=420; 
	pub const INTEGER_VALUE:i32=421; 
	pub const BIGINT_VALUE:i32=422; 
	pub const SMALLINT_VALUE:i32=423; 
	pub const TINYINT_VALUE:i32=424; 
	pub const EXPONENT_VALUE:i32=425; 
	pub const DECIMAL_VALUE:i32=426; 
	pub const FLOAT_VALUE:i32=427; 
	pub const DOUBLE_VALUE:i32=428; 
	pub const BIGDECIMAL_VALUE:i32=429; 
	pub const IDENTIFIER:i32=430; 
	pub const BACKQUOTED_IDENTIFIER:i32=431; 
	pub const VARIABLE:i32=432; 
	pub const SIMPLE_COMMENT:i32=433; 
	pub const BRACKETED_COMMENT:i32=434; 
	pub const WS:i32=435; 
	pub const UNPAIRED_TOKEN:i32=436; 
	pub const UNRECOGNIZED:i32=437;
	pub const channelNames: [&'static str;0+2] = [
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	];

	pub const modeNames: [&'static str;1] = [
		"DEFAULT_MODE"
	];

	pub const ruleNames: [&'static str;441] = [
		"T__0", "T__1", "T__2", "T__3", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", 
		"ANALYZE", "AND", "ANTI", "ANY", "ANY_VALUE", "ARCHIVE", "ARRAY", "ARRAYS_ZIP", 
		"AS", "ASC", "AT", "AUTHORIZATION", "BEGIN", "BETWEEN", "BIGINT", "BINARY", 
		"X_KW", "BINDING", "BOOLEAN", "BOTH", "BUCKET", "BUCKETS", "BY", "BYTE", 
		"CACHE", "CALLED", "CASCADE", "CASE", "CAST", "CATALOG", "CATALOGS", "CHANGE", 
		"CHAR", "CHARACTER", "CHECK", "CLEAR", "CLUSTER", "CLUSTERED", "CODEGEN", 
		"COLLATE", "COLLATION", "COLLECTION", "COLUMN", "COLUMNS", "COMMA", "COMMENT", 
		"COMMIT", "COMPACT", "COMPACTIONS", "COMPENSATION", "COMPUTE", "CONCATENATE", 
		"CONSTRAINT", "CONTAINS", "COST", "COUNT", "CREATE", "CROSS", "CUBE", 
		"CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", 
		"DAY", "DAYS", "DAYOFYEAR", "DATA", "DATE", "DATABASE", "DATABASES", "DATEADD", 
		"DATE_ADD", "DATEDIFF", "DATE_DIFF", "DBPROPERTIES", "DEC", "DECIMAL", 
		"DECLARE", "DECODE", "DEFAULT", "DEFINED", "DEFINER", "DELETE", "DELIMITED", 
		"DESC", "DESCRIBE", "DETERMINISTIC", "DFS", "DIRECTORIES", "DIRECTORY", 
		"DISTINCT", "DISTRIBUTE", "DIV", "DO", "DOUBLE", "DROP", "ELSE", "END", 
		"ESCAPE", "ESCAPED", "EVOLUTION", "EXCEPT", "EXCHANGE", "EXCLUDE", "EXECUTE", 
		"EXISTS", "EXPLAIN", "EXPORT", "EXTENDED", "EXTERNAL", "EXTRACT", "FALSE", 
		"FETCH", "FIELDS", "FILTER", "FILEFORMAT", "FIRST", "FLOAT", "FOLLOWING", 
		"FOR", "FOREIGN", "FORMAT", "FORMATTED", "FROM", "FROM_JSON", "FULL", 
		"FUNCTION", "FUNCTIONS", "GENERATED", "GLOBAL", "GRANT", "GROUP", "GROUPING", 
		"HAVING", "HOUR", "HOURS", "IDENTIFIER_KW", "IDENTITY", "IF", "IGNORE", 
		"IMMEDIATE", "IMPORT", "IN", "INCLUDE", "INDEX", "INDEXES", "INNER", "INPATH", 
		"INPUT", "INPUTFORMAT", "INSERT", "INTERSECT", "INTERVAL", "INT", "INTEGER", 
		"INTO", "INVOKER", "IS", "ITEMS", "ILIKE", "JOIN", "KEY", "KEYS", "LANGUAGE", 
		"LAST", "LATERAL", "LAZY", "LEADING", "LEFT", "LIKE", "LIMIT", "LINES", 
		"LIST", "LISTAGG", "LIVE", "LOAD", "LOCAL", "LOCATION", "LOCK", "LOCKS", 
		"LOGICAL", "LONG", "MACRO", "MAP", "MAP_FROM_ENTRIES", "MATCHED", "MATERIALIZED", 
		"MERGE", "MICROSECOND", "MICROSECONDS", "MILLISECOND", "MILLISECONDS", 
		"MINUS_KW", "MINUTE", "MINUTES", "MODE", "MODIFIES", "MONTH", "MONTHS", 
		"MSCK", "NAME", "NAMESPACE", "NAMESPACES", "NAMED_STRUCT", "NANOSECOND", 
		"NANOSECONDS", "NATURAL", "NO", "NONE", "NOT", "NULL", "NULLS", "NUMERIC", 
		"OF", "OFFSET", "ON", "ONLY", "OPTIMIZE", "OPTION", "OPTIONS", "OR", "ORDER", 
		"OUT", "OUTER", "OUTPUTFORMAT", "OVER", "OVERLAPS", "OVERLAY", "OVERWRITE", 
		"PARTITION", "PARTITIONED", "PARTITIONS", "PERCENT_KW", "PERCENTILE_CONT", 
		"PERCENTILE_DISC", "PIVOT", "PLACING", "POSITION", "PRECEDING", "PRIMARY", 
		"PRINCIPALS", "PROPERTIES", "PRUNE", "PURGE", "QUALIFY", "QUARTER", "QUERY", 
		"RANGE", "READS", "REAL", "RECORDREADER", "RECORDWRITER", "RECOVER", "RECURSIVE", 
		"REDUCE", "REGEXP", "REFERENCE", "REFERENCES", "REFRESH", "RENAME", "REPAIR", 
		"REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", "RETURN", "RETURNS", 
		"REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROW", 
		"ROWS", "SECOND", "SECONDS", "SCHEMA", "SCHEMAS", "SECURITY", "SELECT", 
		"SEMI", "SEPARATED", "SERDE", "SERDEPROPERTIES", "SESSION_USER", "SET", 
		"SETS", "SHORT", "SHOW", "SINGLE", "SKEWED", "SMALLINT", "SOME", "SORT", 
		"SORTED", "SOURCE", "SPECIFIC", "SQL", "START", "STATISTICS", "STORED", 
		"STRATIFY", "STREAM", "STREAMING", "STRING_AGG", "STRUCT", "SUBSTR", "SUBSTRING", 
		"SYNC", "SYSTEM_TIME", "SYSTEM_VERSION", "TABLE", "TABLES", "TABLESAMPLE", 
		"TARGET", "TBLPROPERTIES", "TEMP", "TEMPORARY", "TERMINATED", "STRING_KW", 
		"THEN", "TIME", "TIMEDIFF", "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", 
		"TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TINYINT", "TO", "TOUCH", "TRAILING", 
		"TRANSACTION", "TRANSACTIONS", "TRANSFORM", "TRIM", "TRUE", "TRUNCATE", 
		"TRY_CAST", "TYPE", "UNARCHIVE", "UNBOUNDED", "UNCACHE", "UNION", "UNIQUE", 
		"UNKNOWN", "UNLOCK", "UNPIVOT", "UNSET", "UPDATE", "USE", "USER", "USING", 
		"VALUES", "VAR", "VARCHAR", "VARIANT", "VERSION", "VIEW", "VIEWS", "VOID", 
		"WEEK", "WEEKS", "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", 
		"YEAR", "YEARS", "ZONE", "LPAREN", "RPAREN", "LBRACKET", "RBRACKET", "DOT", 
		"EQ", "DOUBLE_EQ", "BANG", "NSEQ", "HENT_START", "HENT_END", "NEQ", "LT", 
		"LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", "CONCAT", 
		"QUESTION_MARK", "SEMI_COLON", "COLON", "DOLLAR", "BITWISE_AND", "BITWISE_OR", 
		"BITWISE_XOR", "BITWISE_SHIFT_LEFT", "POSIX", "ESCAPE_SEQUENCE", "STRING", 
		"DOUBLEQUOTED_STRING", "UNICODE_STRING", "INTEGER_VALUE", "BIGINT_VALUE", 
		"SMALLINT_VALUE", "TINYINT_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", 
		"FLOAT_VALUE", "DOUBLE_VALUE", "BIGDECIMAL_VALUE", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
		"VARIABLE", "EXPONENT", "DIGIT", "LETTER", "DECIMAL_DIGITS", "SIMPLE_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNPAIRED_TOKEN", "UNRECOGNIZED"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;417] = [
		None, Some("'=>'"), Some("'->'"), Some("'?::'"), Some("'::'"), Some("'ADD'"), 
		Some("'AFTER'"), Some("'ALL'"), Some("'ALTER'"), Some("'ALWAYS'"), Some("'ANALYZE'"), 
		Some("'AND'"), Some("'ANTI'"), Some("'ANY'"), Some("'ANY_VALUE'"), Some("'ARCHIVE'"), 
		Some("'ARRAY'"), Some("'ARRAYS_ZIP'"), Some("'AS'"), Some("'ASC'"), Some("'AT'"), 
		Some("'AUTHORIZATION'"), Some("'BEGIN'"), Some("'BETWEEN'"), Some("'BIGINT'"), 
		Some("'BINARY'"), Some("'X'"), Some("'BINDING'"), Some("'BOOLEAN'"), Some("'BOTH'"), 
		Some("'BUCKET'"), Some("'BUCKETS'"), Some("'BY'"), Some("'BYTE'"), Some("'CACHE'"), 
		Some("'CALLED'"), Some("'CASCADE'"), Some("'CASE'"), Some("'CAST'"), Some("'CATALOG'"), 
		Some("'CATALOGS'"), Some("'CHANGE'"), Some("'CHAR'"), Some("'CHARACTER'"), 
		Some("'CHECK'"), Some("'CLEAR'"), Some("'CLUSTER'"), Some("'CLUSTERED'"), 
		Some("'CODEGEN'"), Some("'COLLATE'"), Some("'COLLATION'"), Some("'COLLECTION'"), 
		Some("'COLUMN'"), Some("'COLUMNS'"), Some("','"), Some("'COMMENT'"), Some("'COMMIT'"), 
		Some("'COMPACT'"), Some("'COMPACTIONS'"), Some("'COMPENSATION'"), Some("'COMPUTE'"), 
		Some("'CONCATENATE'"), Some("'CONSTRAINT'"), Some("'CONTAINS'"), Some("'COST'"), 
		Some("'COUNT'"), Some("'CREATE'"), Some("'CROSS'"), Some("'CUBE'"), Some("'CURRENT'"), 
		Some("'CURRENT_DATE'"), Some("'CURRENT_TIME'"), Some("'CURRENT_TIMESTAMP'"), 
		Some("'CURRENT_USER'"), Some("'DAY'"), Some("'DAYS'"), Some("'DAYOFYEAR'"), 
		Some("'DATA'"), Some("'DATE'"), Some("'DATABASE'"), Some("'DATABASES'"), 
		Some("'DATEADD'"), Some("'DATE_ADD'"), Some("'DATEDIFF'"), Some("'DATE_DIFF'"), 
		Some("'DBPROPERTIES'"), Some("'DEC'"), Some("'DECIMAL'"), Some("'DECLARE'"), 
		Some("'DECODE'"), Some("'DEFAULT'"), Some("'DEFINED'"), Some("'DEFINER'"), 
		Some("'DELETE'"), Some("'DELIMITED'"), Some("'DESC'"), Some("'DESCRIBE'"), 
		Some("'DETERMINISTIC'"), Some("'DFS'"), Some("'DIRECTORIES'"), Some("'DIRECTORY'"), 
		Some("'DISTINCT'"), Some("'DISTRIBUTE'"), Some("'DIV'"), Some("'DO'"), 
		Some("'DOUBLE'"), Some("'DROP'"), Some("'ELSE'"), Some("'END'"), Some("'ESCAPE'"), 
		Some("'ESCAPED'"), Some("'EVOLUTION'"), Some("'EXCEPT'"), Some("'EXCHANGE'"), 
		Some("'EXCLUDE'"), Some("'EXECUTE'"), Some("'EXISTS'"), Some("'EXPLAIN'"), 
		Some("'EXPORT'"), Some("'EXTENDED'"), Some("'EXTERNAL'"), Some("'EXTRACT'"), 
		Some("'FALSE'"), Some("'FETCH'"), Some("'FIELDS'"), Some("'FILTER'"), 
		Some("'FILEFORMAT'"), Some("'FIRST'"), Some("'FLOAT'"), Some("'FOLLOWING'"), 
		Some("'FOR'"), Some("'FOREIGN'"), Some("'FORMAT'"), Some("'FORMATTED'"), 
		Some("'FROM'"), Some("'FROM_JSON'"), Some("'FULL'"), Some("'FUNCTION'"), 
		Some("'FUNCTIONS'"), Some("'GENERATED'"), Some("'GLOBAL'"), Some("'GRANT'"), 
		Some("'GROUP'"), Some("'GROUPING'"), Some("'HAVING'"), Some("'HOUR'"), 
		Some("'HOURS'"), Some("'IDENTIFIER'"), Some("'IDENTITY'"), Some("'IF'"), 
		Some("'IGNORE'"), Some("'IMMEDIATE'"), Some("'IMPORT'"), Some("'IN'"), 
		Some("'INCLUDE'"), Some("'INDEX'"), Some("'INDEXES'"), Some("'INNER'"), 
		Some("'INPATH'"), Some("'INPUT'"), Some("'INPUTFORMAT'"), Some("'INSERT'"), 
		Some("'INTERSECT'"), Some("'INTERVAL'"), Some("'INT'"), Some("'INTEGER'"), 
		Some("'INTO'"), Some("'INVOKER'"), Some("'IS'"), Some("'ITEMS'"), Some("'ILIKE'"), 
		Some("'JOIN'"), Some("'KEY'"), Some("'KEYS'"), Some("'LANGUAGE'"), Some("'LAST'"), 
		Some("'LATERAL'"), Some("'LAZY'"), Some("'LEADING'"), Some("'LEFT'"), 
		Some("'LIKE'"), Some("'LIMIT'"), Some("'LINES'"), Some("'LIST'"), Some("'LISTAGG'"), 
		Some("'LIVE'"), Some("'LOAD'"), Some("'LOCAL'"), Some("'LOCATION'"), Some("'LOCK'"), 
		Some("'LOCKS'"), Some("'LOGICAL'"), Some("'LONG'"), Some("'MACRO'"), Some("'MAP'"), 
		Some("'MAP_FROM_ENTRIES'"), Some("'MATCHED'"), Some("'MATERIALIZED'"), 
		Some("'MERGE'"), Some("'MICROSECOND'"), Some("'MICROSECONDS'"), Some("'MILLISECOND'"), 
		Some("'MILLISECONDS'"), Some("'MINUS'"), Some("'MINUTE'"), Some("'MINUTES'"), 
		Some("'MODE'"), Some("'MODIFIES'"), Some("'MONTH'"), Some("'MONTHS'"), 
		Some("'MSCK'"), Some("'NAME'"), Some("'NAMESPACE'"), Some("'NAMESPACES'"), 
		Some("'NAMED_STRUCT'"), Some("'NANOSECOND'"), Some("'NANOSECONDS'"), Some("'NATURAL'"), 
		Some("'NO'"), Some("'NONE'"), Some("'NOT'"), Some("'NULL'"), Some("'NULLS'"), 
		Some("'NUMERIC'"), Some("'OF'"), Some("'OFFSET'"), Some("'ON'"), Some("'ONLY'"), 
		Some("'OPTIMIZE'"), Some("'OPTION'"), Some("'OPTIONS'"), Some("'OR'"), 
		Some("'ORDER'"), Some("'OUT'"), Some("'OUTER'"), Some("'OUTPUTFORMAT'"), 
		Some("'OVER'"), Some("'OVERLAPS'"), Some("'OVERLAY'"), Some("'OVERWRITE'"), 
		Some("'PARTITION'"), Some("'PARTITIONED'"), Some("'PARTITIONS'"), Some("'PERCENT'"), 
		Some("'PERCENTILE_CONT'"), Some("'PERCENTILE_DISC'"), Some("'PIVOT'"), 
		Some("'PLACING'"), Some("'POSITION'"), Some("'PRECEDING'"), Some("'PRIMARY'"), 
		Some("'PRINCIPALS'"), Some("'PROPERTIES'"), Some("'PRUNE'"), Some("'PURGE'"), 
		Some("'QUALIFY'"), Some("'QUARTER'"), Some("'QUERY'"), Some("'RANGE'"), 
		Some("'READS'"), Some("'REAL'"), Some("'RECORDREADER'"), Some("'RECORDWRITER'"), 
		Some("'RECOVER'"), Some("'RECURSIVE'"), Some("'REDUCE'"), Some("'REGEXP'"), 
		Some("'REFERENCE'"), Some("'REFERENCES'"), Some("'REFRESH'"), Some("'RENAME'"), 
		Some("'REPAIR'"), Some("'REPEATABLE'"), Some("'REPLACE'"), Some("'RESET'"), 
		Some("'RESPECT'"), Some("'RESTRICT'"), Some("'RETURN'"), Some("'RETURNS'"), 
		Some("'REVOKE'"), Some("'RIGHT'"), Some("'RLIKE'"), Some("'ROLE'"), Some("'ROLES'"), 
		Some("'ROLLBACK'"), Some("'ROLLUP'"), Some("'ROW'"), Some("'ROWS'"), Some("'SECOND'"), 
		Some("'SECONDS'"), Some("'SCHEMA'"), Some("'SCHEMAS'"), Some("'SECURITY'"), 
		Some("'SELECT'"), Some("'SEMI'"), Some("'SEPARATED'"), Some("'SERDE'"), 
		Some("'SERDEPROPERTIES'"), Some("'SESSION_USER'"), Some("'SET'"), Some("'SETS'"), 
		Some("'SHORT'"), Some("'SHOW'"), Some("'SINGLE'"), Some("'SKEWED'"), Some("'SMALLINT'"), 
		Some("'SOME'"), Some("'SORT'"), Some("'SORTED'"), Some("'SOURCE'"), Some("'SPECIFIC'"), 
		Some("'SQL'"), Some("'START'"), Some("'STATISTICS'"), Some("'STORED'"), 
		Some("'STRATIFY'"), Some("'STREAM'"), Some("'STREAMING'"), Some("'STRING_AGG'"), 
		Some("'STRUCT'"), Some("'SUBSTR'"), Some("'SUBSTRING'"), Some("'SYNC'"), 
		Some("'SYSTEM_TIME'"), Some("'SYSTEM_VERSION'"), Some("'TABLE'"), Some("'TABLES'"), 
		Some("'TABLESAMPLE'"), Some("'TARGET'"), Some("'TBLPROPERTIES'"), Some("'TEMP'"), 
		Some("'TEMPORARY'"), Some("'TERMINATED'"), Some("'STRING'"), Some("'THEN'"), 
		Some("'TIME'"), Some("'TIMEDIFF'"), Some("'TIMESTAMP'"), Some("'TIMESTAMPADD'"), 
		Some("'TIMESTAMPDIFF'"), Some("'TIMESTAMP_LTZ'"), Some("'TIMESTAMP_NTZ'"), 
		Some("'TINYINT'"), Some("'TO'"), Some("'TOUCH'"), Some("'TRAILING'"), 
		Some("'TRANSACTION'"), Some("'TRANSACTIONS'"), Some("'TRANSFORM'"), Some("'TRIM'"), 
		Some("'TRUE'"), Some("'TRUNCATE'"), Some("'TRY_CAST'"), Some("'TYPE'"), 
		Some("'UNARCHIVE'"), Some("'UNBOUNDED'"), Some("'UNCACHE'"), Some("'UNION'"), 
		Some("'UNIQUE'"), Some("'UNKNOWN'"), Some("'UNLOCK'"), Some("'UNPIVOT'"), 
		Some("'UNSET'"), Some("'UPDATE'"), Some("'USE'"), Some("'USER'"), Some("'USING'"), 
		Some("'VALUES'"), Some("'VAR'"), Some("'VARCHAR'"), Some("'VARIANT'"), 
		Some("'VERSION'"), Some("'VIEW'"), Some("'VIEWS'"), Some("'VOID'"), Some("'WEEK'"), 
		Some("'WEEKS'"), Some("'WHEN'"), Some("'WHERE'"), Some("'WHILE'"), Some("'WINDOW'"), 
		Some("'WITH'"), Some("'WITHIN'"), Some("'YEAR'"), Some("'YEARS'"), Some("'ZONE'"), 
		Some("'('"), Some("')'"), Some("'['"), Some("']'"), Some("'.'"), Some("'='"), 
		Some("'=='"), Some("'!'"), Some("'<=>'"), Some("'/*+'"), Some("'*/'"), 
		None, Some("'<'"), Some("'<='"), Some("'>'"), Some("'>='"), Some("'+'"), 
		Some("'-'"), Some("'*'"), Some("'/'"), Some("'%'"), Some("'||'"), Some("'?'"), 
		Some("';'"), Some("':'"), Some("'$'"), Some("'&'"), Some("'|'"), Some("'^'"), 
		Some("'<<'"), Some("'~'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;438]  = [
		None, None, None, None, None, Some("ADD"), Some("AFTER"), Some("ALL"), 
		Some("ALTER"), Some("ALWAYS"), Some("ANALYZE"), Some("AND"), Some("ANTI"), 
		Some("ANY"), Some("ANY_VALUE"), Some("ARCHIVE"), Some("ARRAY"), Some("ARRAYS_ZIP"), 
		Some("AS"), Some("ASC"), Some("AT"), Some("AUTHORIZATION"), Some("BEGIN"), 
		Some("BETWEEN"), Some("BIGINT"), Some("BINARY"), Some("X_KW"), Some("BINDING"), 
		Some("BOOLEAN"), Some("BOTH"), Some("BUCKET"), Some("BUCKETS"), Some("BY"), 
		Some("BYTE"), Some("CACHE"), Some("CALLED"), Some("CASCADE"), Some("CASE"), 
		Some("CAST"), Some("CATALOG"), Some("CATALOGS"), Some("CHANGE"), Some("CHAR"), 
		Some("CHARACTER"), Some("CHECK"), Some("CLEAR"), Some("CLUSTER"), Some("CLUSTERED"), 
		Some("CODEGEN"), Some("COLLATE"), Some("COLLATION"), Some("COLLECTION"), 
		Some("COLUMN"), Some("COLUMNS"), Some("COMMA"), Some("COMMENT"), Some("COMMIT"), 
		Some("COMPACT"), Some("COMPACTIONS"), Some("COMPENSATION"), Some("COMPUTE"), 
		Some("CONCATENATE"), Some("CONSTRAINT"), Some("CONTAINS"), Some("COST"), 
		Some("COUNT"), Some("CREATE"), Some("CROSS"), Some("CUBE"), Some("CURRENT"), 
		Some("CURRENT_DATE"), Some("CURRENT_TIME"), Some("CURRENT_TIMESTAMP"), 
		Some("CURRENT_USER"), Some("DAY"), Some("DAYS"), Some("DAYOFYEAR"), Some("DATA"), 
		Some("DATE"), Some("DATABASE"), Some("DATABASES"), Some("DATEADD"), Some("DATE_ADD"), 
		Some("DATEDIFF"), Some("DATE_DIFF"), Some("DBPROPERTIES"), Some("DEC"), 
		Some("DECIMAL"), Some("DECLARE"), Some("DECODE"), Some("DEFAULT"), Some("DEFINED"), 
		Some("DEFINER"), Some("DELETE"), Some("DELIMITED"), Some("DESC"), Some("DESCRIBE"), 
		Some("DETERMINISTIC"), Some("DFS"), Some("DIRECTORIES"), Some("DIRECTORY"), 
		Some("DISTINCT"), Some("DISTRIBUTE"), Some("DIV"), Some("DO"), Some("DOUBLE"), 
		Some("DROP"), Some("ELSE"), Some("END"), Some("ESCAPE"), Some("ESCAPED"), 
		Some("EVOLUTION"), Some("EXCEPT"), Some("EXCHANGE"), Some("EXCLUDE"), 
		Some("EXECUTE"), Some("EXISTS"), Some("EXPLAIN"), Some("EXPORT"), Some("EXTENDED"), 
		Some("EXTERNAL"), Some("EXTRACT"), Some("FALSE"), Some("FETCH"), Some("FIELDS"), 
		Some("FILTER"), Some("FILEFORMAT"), Some("FIRST"), Some("FLOAT"), Some("FOLLOWING"), 
		Some("FOR"), Some("FOREIGN"), Some("FORMAT"), Some("FORMATTED"), Some("FROM"), 
		Some("FROM_JSON"), Some("FULL"), Some("FUNCTION"), Some("FUNCTIONS"), 
		Some("GENERATED"), Some("GLOBAL"), Some("GRANT"), Some("GROUP"), Some("GROUPING"), 
		Some("HAVING"), Some("HOUR"), Some("HOURS"), Some("IDENTIFIER_KW"), Some("IDENTITY"), 
		Some("IF"), Some("IGNORE"), Some("IMMEDIATE"), Some("IMPORT"), Some("IN"), 
		Some("INCLUDE"), Some("INDEX"), Some("INDEXES"), Some("INNER"), Some("INPATH"), 
		Some("INPUT"), Some("INPUTFORMAT"), Some("INSERT"), Some("INTERSECT"), 
		Some("INTERVAL"), Some("INT"), Some("INTEGER"), Some("INTO"), Some("INVOKER"), 
		Some("IS"), Some("ITEMS"), Some("ILIKE"), Some("JOIN"), Some("KEY"), Some("KEYS"), 
		Some("LANGUAGE"), Some("LAST"), Some("LATERAL"), Some("LAZY"), Some("LEADING"), 
		Some("LEFT"), Some("LIKE"), Some("LIMIT"), Some("LINES"), Some("LIST"), 
		Some("LISTAGG"), Some("LIVE"), Some("LOAD"), Some("LOCAL"), Some("LOCATION"), 
		Some("LOCK"), Some("LOCKS"), Some("LOGICAL"), Some("LONG"), Some("MACRO"), 
		Some("MAP"), Some("MAP_FROM_ENTRIES"), Some("MATCHED"), Some("MATERIALIZED"), 
		Some("MERGE"), Some("MICROSECOND"), Some("MICROSECONDS"), Some("MILLISECOND"), 
		Some("MILLISECONDS"), Some("MINUS_KW"), Some("MINUTE"), Some("MINUTES"), 
		Some("MODE"), Some("MODIFIES"), Some("MONTH"), Some("MONTHS"), Some("MSCK"), 
		Some("NAME"), Some("NAMESPACE"), Some("NAMESPACES"), Some("NAMED_STRUCT"), 
		Some("NANOSECOND"), Some("NANOSECONDS"), Some("NATURAL"), Some("NO"), 
		Some("NONE"), Some("NOT"), Some("NULL"), Some("NULLS"), Some("NUMERIC"), 
		Some("OF"), Some("OFFSET"), Some("ON"), Some("ONLY"), Some("OPTIMIZE"), 
		Some("OPTION"), Some("OPTIONS"), Some("OR"), Some("ORDER"), Some("OUT"), 
		Some("OUTER"), Some("OUTPUTFORMAT"), Some("OVER"), Some("OVERLAPS"), Some("OVERLAY"), 
		Some("OVERWRITE"), Some("PARTITION"), Some("PARTITIONED"), Some("PARTITIONS"), 
		Some("PERCENT_KW"), Some("PERCENTILE_CONT"), Some("PERCENTILE_DISC"), 
		Some("PIVOT"), Some("PLACING"), Some("POSITION"), Some("PRECEDING"), Some("PRIMARY"), 
		Some("PRINCIPALS"), Some("PROPERTIES"), Some("PRUNE"), Some("PURGE"), 
		Some("QUALIFY"), Some("QUARTER"), Some("QUERY"), Some("RANGE"), Some("READS"), 
		Some("REAL"), Some("RECORDREADER"), Some("RECORDWRITER"), Some("RECOVER"), 
		Some("RECURSIVE"), Some("REDUCE"), Some("REGEXP"), Some("REFERENCE"), 
		Some("REFERENCES"), Some("REFRESH"), Some("RENAME"), Some("REPAIR"), Some("REPEATABLE"), 
		Some("REPLACE"), Some("RESET"), Some("RESPECT"), Some("RESTRICT"), Some("RETURN"), 
		Some("RETURNS"), Some("REVOKE"), Some("RIGHT"), Some("RLIKE"), Some("ROLE"), 
		Some("ROLES"), Some("ROLLBACK"), Some("ROLLUP"), Some("ROW"), Some("ROWS"), 
		Some("SECOND"), Some("SECONDS"), Some("SCHEMA"), Some("SCHEMAS"), Some("SECURITY"), 
		Some("SELECT"), Some("SEMI"), Some("SEPARATED"), Some("SERDE"), Some("SERDEPROPERTIES"), 
		Some("SESSION_USER"), Some("SET"), Some("SETS"), Some("SHORT"), Some("SHOW"), 
		Some("SINGLE"), Some("SKEWED"), Some("SMALLINT"), Some("SOME"), Some("SORT"), 
		Some("SORTED"), Some("SOURCE"), Some("SPECIFIC"), Some("SQL"), Some("START"), 
		Some("STATISTICS"), Some("STORED"), Some("STRATIFY"), Some("STREAM"), 
		Some("STREAMING"), Some("STRING_AGG"), Some("STRUCT"), Some("SUBSTR"), 
		Some("SUBSTRING"), Some("SYNC"), Some("SYSTEM_TIME"), Some("SYSTEM_VERSION"), 
		Some("TABLE"), Some("TABLES"), Some("TABLESAMPLE"), Some("TARGET"), Some("TBLPROPERTIES"), 
		Some("TEMP"), Some("TEMPORARY"), Some("TERMINATED"), Some("STRING_KW"), 
		Some("THEN"), Some("TIME"), Some("TIMEDIFF"), Some("TIMESTAMP"), Some("TIMESTAMPADD"), 
		Some("TIMESTAMPDIFF"), Some("TIMESTAMP_LTZ"), Some("TIMESTAMP_NTZ"), Some("TINYINT"), 
		Some("TO"), Some("TOUCH"), Some("TRAILING"), Some("TRANSACTION"), Some("TRANSACTIONS"), 
		Some("TRANSFORM"), Some("TRIM"), Some("TRUE"), Some("TRUNCATE"), Some("TRY_CAST"), 
		Some("TYPE"), Some("UNARCHIVE"), Some("UNBOUNDED"), Some("UNCACHE"), Some("UNION"), 
		Some("UNIQUE"), Some("UNKNOWN"), Some("UNLOCK"), Some("UNPIVOT"), Some("UNSET"), 
		Some("UPDATE"), Some("USE"), Some("USER"), Some("USING"), Some("VALUES"), 
		Some("VAR"), Some("VARCHAR"), Some("VARIANT"), Some("VERSION"), Some("VIEW"), 
		Some("VIEWS"), Some("VOID"), Some("WEEK"), Some("WEEKS"), Some("WHEN"), 
		Some("WHERE"), Some("WHILE"), Some("WINDOW"), Some("WITH"), Some("WITHIN"), 
		Some("YEAR"), Some("YEARS"), Some("ZONE"), Some("LPAREN"), Some("RPAREN"), 
		Some("LBRACKET"), Some("RBRACKET"), Some("DOT"), Some("EQ"), Some("DOUBLE_EQ"), 
		Some("BANG"), Some("NSEQ"), Some("HENT_START"), Some("HENT_END"), Some("NEQ"), 
		Some("LT"), Some("LTE"), Some("GT"), Some("GTE"), Some("PLUS"), Some("MINUS"), 
		Some("ASTERISK"), Some("SLASH"), Some("PERCENT"), Some("CONCAT"), Some("QUESTION_MARK"), 
		Some("SEMI_COLON"), Some("COLON"), Some("DOLLAR"), Some("BITWISE_AND"), 
		Some("BITWISE_OR"), Some("BITWISE_XOR"), Some("BITWISE_SHIFT_LEFT"), Some("POSIX"), 
		Some("ESCAPE_SEQUENCE"), Some("STRING"), Some("DOUBLEQUOTED_STRING"), 
		Some("UNICODE_STRING"), Some("INTEGER_VALUE"), Some("BIGINT_VALUE"), Some("SMALLINT_VALUE"), 
		Some("TINYINT_VALUE"), Some("EXPONENT_VALUE"), Some("DECIMAL_VALUE"), 
		Some("FLOAT_VALUE"), Some("DOUBLE_VALUE"), Some("BIGDECIMAL_VALUE"), Some("IDENTIFIER"), 
		Some("BACKQUOTED_IDENTIFIER"), Some("VARIABLE"), Some("SIMPLE_COMMENT"), 
		Some("BRACKETED_COMMENT"), Some("WS"), Some("UNPAIRED_TOKEN"), Some("UNRECOGNIZED")
	];

	static _shared_context_cache: LazyLock<PredictionContextCache> = LazyLock::new(|| PredictionContextCache::new());
	static VOCABULARY: LazyLock<Box<dyn Vocabulary>> = LazyLock::new(|| Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None)));



pub type LexerContext<'input> = BaseRuleContext<'input,EmptyCustomRuleContext<'input,LocalTokenFactory<'input> >>;

pub type LocalTokenFactory<'input> = dbt_antlr4::token_factory::ArenaCommonFactory<'input>;

type From<'a> = <LocalTokenFactory<'a> as TokenFactory<'a> >::From;

pub struct DatabricksLexer<'input, Input:CharStream<From<'input> >> {
	base: BaseLexer<'input,DatabricksLexerActions,Input,LocalTokenFactory<'input>>,
}

dbt_antlr4::tid! { impl<'input,Input> TidAble<'input> for DatabricksLexer<'input,Input> where Input:CharStream<From<'input> > }

impl<'input, Input:CharStream<From<'input> >> Deref for DatabricksLexer<'input,Input>{
	type Target = BaseLexer<'input,DatabricksLexerActions,Input,LocalTokenFactory<'input>>;

	fn deref(&self) -> &Self::Target {
		&self.base
	}
}

impl<'input, Input:CharStream<From<'input> >> DerefMut for DatabricksLexer<'input,Input>{
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.base
	}
}


impl<'input, Input:CharStream<From<'input> >> DatabricksLexer<'input,Input>{
    fn get_rule_names(&self) -> &'static [&'static str] {
        &ruleNames
    }
    fn get_literal_names(&self) -> &[Option<&str>] {
        &_LITERAL_NAMES
    }

    fn get_symbolic_names(&self) -> &[Option<&str>] {
        &_SYMBOLIC_NAMES
    }

    fn get_grammar_file_name(&self) -> &'static str {
        "DatabricksLexer.g4"
    }

	pub fn new_with_token_factory(input: Input, tf: &'input LocalTokenFactory<'input>) -> Self {
		dbt_antlr4::recognizer::check_version("0","51");
    	Self {
			base: BaseLexer::new_base_lexer(
				input,
				LexerATNSimulator::new_lexer_atnsimulator(
					&_ATN,
					&_decision_to_DFA,
					&_shared_context_cache,
				),
				DatabricksLexerActions{},
				tf
			)
	    }
	}
}

impl<'input, Input:CharStream<From<'input> >> DatabricksLexer<'input,Input> where &'input LocalTokenFactory<'input>:Default{
	pub fn new(input: Input) -> Self{
		DatabricksLexer::new_with_token_factory(input, <&LocalTokenFactory<'input> as Default>::default())
	}
}

pub struct DatabricksLexerActions {
}

impl DatabricksLexerActions{
}

impl<'input, Input:CharStream<From<'input> >> Actions<'input,BaseLexer<'input,DatabricksLexerActions,Input,LocalTokenFactory<'input>>> for DatabricksLexerActions{
	fn sempred(_localctx: Option<&EmptyContext<'input,LocalTokenFactory<'input>> >, rule_index: i32, pred_index: i32,
	           recog:&mut BaseLexer<'input,DatabricksLexerActions,Input,LocalTokenFactory<'input>>
	    ) -> bool {
	    	match rule_index {
			        424 =>
			        	DatabricksLexer::<'input>::EXPONENT_VALUE_sempred(None, pred_index, recog), 
			        425 =>
			        	DatabricksLexer::<'input>::DECIMAL_VALUE_sempred(None, pred_index, recog), 
			        426 =>
			        	DatabricksLexer::<'input>::FLOAT_VALUE_sempred(None, pred_index, recog), 
			        427 =>
			        	DatabricksLexer::<'input>::DOUBLE_VALUE_sempred(None, pred_index, recog), 
			        428 =>
			        	DatabricksLexer::<'input>::BIGDECIMAL_VALUE_sempred(None, pred_index, recog), 
			_ => true
		}
	}

	}

	impl<'input, Input:CharStream<From<'input> >> DatabricksLexer<'input,Input>{
		fn EXPONENT_VALUE_sempred(_localctx: Option<&LexerContext<'input>>, pred_index:i32,
							recog:&mut <Self as Deref>::Target
			) -> bool {
			match pred_index {
					0=>{
						 crate::lexer_support::is_valid_decimal_boundary(recog) 
					}
				_ => true
			}
		}
		fn DECIMAL_VALUE_sempred(_localctx: Option<&LexerContext<'input>>, pred_index:i32,
							recog:&mut <Self as Deref>::Target
			) -> bool {
			match pred_index {
					1=>{
						 crate::lexer_support::is_valid_decimal_boundary(recog) 
					}
				_ => true
			}
		}
		fn FLOAT_VALUE_sempred(_localctx: Option<&LexerContext<'input>>, pred_index:i32,
							recog:&mut <Self as Deref>::Target
			) -> bool {
			match pred_index {
					2=>{
						 crate::lexer_support::is_valid_decimal_boundary(recog) 
					}
				_ => true
			}
		}
		fn DOUBLE_VALUE_sempred(_localctx: Option<&LexerContext<'input>>, pred_index:i32,
							recog:&mut <Self as Deref>::Target
			) -> bool {
			match pred_index {
					3=>{
						 crate::lexer_support::is_valid_decimal_boundary(recog) 
					}
				_ => true
			}
		}
		fn BIGDECIMAL_VALUE_sempred(_localctx: Option<&LexerContext<'input>>, pred_index:i32,
							recog:&mut <Self as Deref>::Target
			) -> bool {
			match pred_index {
					4=>{
						 crate::lexer_support::is_valid_decimal_boundary(recog) 
					}
				_ => true
			}
		}


}

impl<'input, Input:CharStream<From<'input> >> LexerRecog<'input,BaseLexer<'input,DatabricksLexerActions,Input,LocalTokenFactory<'input>>> for DatabricksLexerActions{
}
impl<'input> TokenAware<'input> for DatabricksLexerActions{
	type TF = LocalTokenFactory<'input>;
}

impl<'input, Input:CharStream<From<'input> >> TokenSource<'input> for DatabricksLexer<'input,Input>{
	type TF = LocalTokenFactory<'input>;

    fn next_token(&mut self) -> <Self::TF as TokenFactory<'input>>::Tok {
        self.base.next_token()
    }

    fn get_line(&self) -> isize {
        self.base.get_line()
    }

    fn get_char_position_in_line(&self) -> isize {
        self.base.get_char_position_in_line()
    }

    fn get_input_stream(&mut self) -> Option<&mut dyn IntStream> {
        self.base.get_input_stream()
    }

	fn get_source_name(&self) -> String {
		self.base.get_source_name()
	}

    fn get_token_factory(&self) -> &'input Self::TF {
        self.base.get_token_factory()
    }

    fn get_dfa_string(&self) -> String {
        self.base.get_dfa_string()
    }
}


	static _ATN: LazyLock<ATN> =
	    LazyLock::new(|| ATNDeserializer::new(None).deserialize(&mut _serializedATN.iter()));
	static _decision_to_DFA: LazyLock<Vec<DFA>> = LazyLock::new(|| {
	    let mut dfa = Vec::new();
	    let size = _ATN.decision_to_state.len() as i32;
	    for i in 0..size {
	        dfa.push(DFA::new(
	            &_ATN,
	            _ATN.get_decision_state(i),
	            i,
	        ))
	    }
	    dfa
	});
	static _serializedATN: LazyLock<Vec<i32>> = LazyLock::new(|| vec![
	    4, 0, 437, 4139, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 
	    3, 2, 4, 7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 
	    7, 9, 2, 10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 
	    7, 14, 2, 15, 7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 
	    7, 19, 2, 20, 7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 
	    7, 24, 2, 25, 7, 25, 2, 26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 
	    7, 29, 2, 30, 7, 30, 2, 31, 7, 31, 2, 32, 7, 32, 2, 33, 7, 33, 2, 34, 
	    7, 34, 2, 35, 7, 35, 2, 36, 7, 36, 2, 37, 7, 37, 2, 38, 7, 38, 2, 39, 
	    7, 39, 2, 40, 7, 40, 2, 41, 7, 41, 2, 42, 7, 42, 2, 43, 7, 43, 2, 44, 
	    7, 44, 2, 45, 7, 45, 2, 46, 7, 46, 2, 47, 7, 47, 2, 48, 7, 48, 2, 49, 
	    7, 49, 2, 50, 7, 50, 2, 51, 7, 51, 2, 52, 7, 52, 2, 53, 7, 53, 2, 54, 
	    7, 54, 2, 55, 7, 55, 2, 56, 7, 56, 2, 57, 7, 57, 2, 58, 7, 58, 2, 59, 
	    7, 59, 2, 60, 7, 60, 2, 61, 7, 61, 2, 62, 7, 62, 2, 63, 7, 63, 2, 64, 
	    7, 64, 2, 65, 7, 65, 2, 66, 7, 66, 2, 67, 7, 67, 2, 68, 7, 68, 2, 69, 
	    7, 69, 2, 70, 7, 70, 2, 71, 7, 71, 2, 72, 7, 72, 2, 73, 7, 73, 2, 74, 
	    7, 74, 2, 75, 7, 75, 2, 76, 7, 76, 2, 77, 7, 77, 2, 78, 7, 78, 2, 79, 
	    7, 79, 2, 80, 7, 80, 2, 81, 7, 81, 2, 82, 7, 82, 2, 83, 7, 83, 2, 84, 
	    7, 84, 2, 85, 7, 85, 2, 86, 7, 86, 2, 87, 7, 87, 2, 88, 7, 88, 2, 89, 
	    7, 89, 2, 90, 7, 90, 2, 91, 7, 91, 2, 92, 7, 92, 2, 93, 7, 93, 2, 94, 
	    7, 94, 2, 95, 7, 95, 2, 96, 7, 96, 2, 97, 7, 97, 2, 98, 7, 98, 2, 99, 
	    7, 99, 2, 100, 7, 100, 2, 101, 7, 101, 2, 102, 7, 102, 2, 103, 7, 103, 
	    2, 104, 7, 104, 2, 105, 7, 105, 2, 106, 7, 106, 2, 107, 7, 107, 2, 
	    108, 7, 108, 2, 109, 7, 109, 2, 110, 7, 110, 2, 111, 7, 111, 2, 112, 
	    7, 112, 2, 113, 7, 113, 2, 114, 7, 114, 2, 115, 7, 115, 2, 116, 7, 
	    116, 2, 117, 7, 117, 2, 118, 7, 118, 2, 119, 7, 119, 2, 120, 7, 120, 
	    2, 121, 7, 121, 2, 122, 7, 122, 2, 123, 7, 123, 2, 124, 7, 124, 2, 
	    125, 7, 125, 2, 126, 7, 126, 2, 127, 7, 127, 2, 128, 7, 128, 2, 129, 
	    7, 129, 2, 130, 7, 130, 2, 131, 7, 131, 2, 132, 7, 132, 2, 133, 7, 
	    133, 2, 134, 7, 134, 2, 135, 7, 135, 2, 136, 7, 136, 2, 137, 7, 137, 
	    2, 138, 7, 138, 2, 139, 7, 139, 2, 140, 7, 140, 2, 141, 7, 141, 2, 
	    142, 7, 142, 2, 143, 7, 143, 2, 144, 7, 144, 2, 145, 7, 145, 2, 146, 
	    7, 146, 2, 147, 7, 147, 2, 148, 7, 148, 2, 149, 7, 149, 2, 150, 7, 
	    150, 2, 151, 7, 151, 2, 152, 7, 152, 2, 153, 7, 153, 2, 154, 7, 154, 
	    2, 155, 7, 155, 2, 156, 7, 156, 2, 157, 7, 157, 2, 158, 7, 158, 2, 
	    159, 7, 159, 2, 160, 7, 160, 2, 161, 7, 161, 2, 162, 7, 162, 2, 163, 
	    7, 163, 2, 164, 7, 164, 2, 165, 7, 165, 2, 166, 7, 166, 2, 167, 7, 
	    167, 2, 168, 7, 168, 2, 169, 7, 169, 2, 170, 7, 170, 2, 171, 7, 171, 
	    2, 172, 7, 172, 2, 173, 7, 173, 2, 174, 7, 174, 2, 175, 7, 175, 2, 
	    176, 7, 176, 2, 177, 7, 177, 2, 178, 7, 178, 2, 179, 7, 179, 2, 180, 
	    7, 180, 2, 181, 7, 181, 2, 182, 7, 182, 2, 183, 7, 183, 2, 184, 7, 
	    184, 2, 185, 7, 185, 2, 186, 7, 186, 2, 187, 7, 187, 2, 188, 7, 188, 
	    2, 189, 7, 189, 2, 190, 7, 190, 2, 191, 7, 191, 2, 192, 7, 192, 2, 
	    193, 7, 193, 2, 194, 7, 194, 2, 195, 7, 195, 2, 196, 7, 196, 2, 197, 
	    7, 197, 2, 198, 7, 198, 2, 199, 7, 199, 2, 200, 7, 200, 2, 201, 7, 
	    201, 2, 202, 7, 202, 2, 203, 7, 203, 2, 204, 7, 204, 2, 205, 7, 205, 
	    2, 206, 7, 206, 2, 207, 7, 207, 2, 208, 7, 208, 2, 209, 7, 209, 2, 
	    210, 7, 210, 2, 211, 7, 211, 2, 212, 7, 212, 2, 213, 7, 213, 2, 214, 
	    7, 214, 2, 215, 7, 215, 2, 216, 7, 216, 2, 217, 7, 217, 2, 218, 7, 
	    218, 2, 219, 7, 219, 2, 220, 7, 220, 2, 221, 7, 221, 2, 222, 7, 222, 
	    2, 223, 7, 223, 2, 224, 7, 224, 2, 225, 7, 225, 2, 226, 7, 226, 2, 
	    227, 7, 227, 2, 228, 7, 228, 2, 229, 7, 229, 2, 230, 7, 230, 2, 231, 
	    7, 231, 2, 232, 7, 232, 2, 233, 7, 233, 2, 234, 7, 234, 2, 235, 7, 
	    235, 2, 236, 7, 236, 2, 237, 7, 237, 2, 238, 7, 238, 2, 239, 7, 239, 
	    2, 240, 7, 240, 2, 241, 7, 241, 2, 242, 7, 242, 2, 243, 7, 243, 2, 
	    244, 7, 244, 2, 245, 7, 245, 2, 246, 7, 246, 2, 247, 7, 247, 2, 248, 
	    7, 248, 2, 249, 7, 249, 2, 250, 7, 250, 2, 251, 7, 251, 2, 252, 7, 
	    252, 2, 253, 7, 253, 2, 254, 7, 254, 2, 255, 7, 255, 2, 256, 7, 256, 
	    2, 257, 7, 257, 2, 258, 7, 258, 2, 259, 7, 259, 2, 260, 7, 260, 2, 
	    261, 7, 261, 2, 262, 7, 262, 2, 263, 7, 263, 2, 264, 7, 264, 2, 265, 
	    7, 265, 2, 266, 7, 266, 2, 267, 7, 267, 2, 268, 7, 268, 2, 269, 7, 
	    269, 2, 270, 7, 270, 2, 271, 7, 271, 2, 272, 7, 272, 2, 273, 7, 273, 
	    2, 274, 7, 274, 2, 275, 7, 275, 2, 276, 7, 276, 2, 277, 7, 277, 2, 
	    278, 7, 278, 2, 279, 7, 279, 2, 280, 7, 280, 2, 281, 7, 281, 2, 282, 
	    7, 282, 2, 283, 7, 283, 2, 284, 7, 284, 2, 285, 7, 285, 2, 286, 7, 
	    286, 2, 287, 7, 287, 2, 288, 7, 288, 2, 289, 7, 289, 2, 290, 7, 290, 
	    2, 291, 7, 291, 2, 292, 7, 292, 2, 293, 7, 293, 2, 294, 7, 294, 2, 
	    295, 7, 295, 2, 296, 7, 296, 2, 297, 7, 297, 2, 298, 7, 298, 2, 299, 
	    7, 299, 2, 300, 7, 300, 2, 301, 7, 301, 2, 302, 7, 302, 2, 303, 7, 
	    303, 2, 304, 7, 304, 2, 305, 7, 305, 2, 306, 7, 306, 2, 307, 7, 307, 
	    2, 308, 7, 308, 2, 309, 7, 309, 2, 310, 7, 310, 2, 311, 7, 311, 2, 
	    312, 7, 312, 2, 313, 7, 313, 2, 314, 7, 314, 2, 315, 7, 315, 2, 316, 
	    7, 316, 2, 317, 7, 317, 2, 318, 7, 318, 2, 319, 7, 319, 2, 320, 7, 
	    320, 2, 321, 7, 321, 2, 322, 7, 322, 2, 323, 7, 323, 2, 324, 7, 324, 
	    2, 325, 7, 325, 2, 326, 7, 326, 2, 327, 7, 327, 2, 328, 7, 328, 2, 
	    329, 7, 329, 2, 330, 7, 330, 2, 331, 7, 331, 2, 332, 7, 332, 2, 333, 
	    7, 333, 2, 334, 7, 334, 2, 335, 7, 335, 2, 336, 7, 336, 2, 337, 7, 
	    337, 2, 338, 7, 338, 2, 339, 7, 339, 2, 340, 7, 340, 2, 341, 7, 341, 
	    2, 342, 7, 342, 2, 343, 7, 343, 2, 344, 7, 344, 2, 345, 7, 345, 2, 
	    346, 7, 346, 2, 347, 7, 347, 2, 348, 7, 348, 2, 349, 7, 349, 2, 350, 
	    7, 350, 2, 351, 7, 351, 2, 352, 7, 352, 2, 353, 7, 353, 2, 354, 7, 
	    354, 2, 355, 7, 355, 2, 356, 7, 356, 2, 357, 7, 357, 2, 358, 7, 358, 
	    2, 359, 7, 359, 2, 360, 7, 360, 2, 361, 7, 361, 2, 362, 7, 362, 2, 
	    363, 7, 363, 2, 364, 7, 364, 2, 365, 7, 365, 2, 366, 7, 366, 2, 367, 
	    7, 367, 2, 368, 7, 368, 2, 369, 7, 369, 2, 370, 7, 370, 2, 371, 7, 
	    371, 2, 372, 7, 372, 2, 373, 7, 373, 2, 374, 7, 374, 2, 375, 7, 375, 
	    2, 376, 7, 376, 2, 377, 7, 377, 2, 378, 7, 378, 2, 379, 7, 379, 2, 
	    380, 7, 380, 2, 381, 7, 381, 2, 382, 7, 382, 2, 383, 7, 383, 2, 384, 
	    7, 384, 2, 385, 7, 385, 2, 386, 7, 386, 2, 387, 7, 387, 2, 388, 7, 
	    388, 2, 389, 7, 389, 2, 390, 7, 390, 2, 391, 7, 391, 2, 392, 7, 392, 
	    2, 393, 7, 393, 2, 394, 7, 394, 2, 395, 7, 395, 2, 396, 7, 396, 2, 
	    397, 7, 397, 2, 398, 7, 398, 2, 399, 7, 399, 2, 400, 7, 400, 2, 401, 
	    7, 401, 2, 402, 7, 402, 2, 403, 7, 403, 2, 404, 7, 404, 2, 405, 7, 
	    405, 2, 406, 7, 406, 2, 407, 7, 407, 2, 408, 7, 408, 2, 409, 7, 409, 
	    2, 410, 7, 410, 2, 411, 7, 411, 2, 412, 7, 412, 2, 413, 7, 413, 2, 
	    414, 7, 414, 2, 415, 7, 415, 2, 416, 7, 416, 2, 417, 7, 417, 2, 418, 
	    7, 418, 2, 419, 7, 419, 2, 420, 7, 420, 2, 421, 7, 421, 2, 422, 7, 
	    422, 2, 423, 7, 423, 2, 424, 7, 424, 2, 425, 7, 425, 2, 426, 7, 426, 
	    2, 427, 7, 427, 2, 428, 7, 428, 2, 429, 7, 429, 2, 430, 7, 430, 2, 
	    431, 7, 431, 2, 432, 7, 432, 2, 433, 7, 433, 2, 434, 7, 434, 2, 435, 
	    7, 435, 2, 436, 7, 436, 2, 437, 7, 437, 2, 438, 7, 438, 2, 439, 7, 
	    439, 2, 440, 7, 440, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 
	    1, 2, 1, 2, 1, 3, 1, 3, 1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 
	    5, 1, 5, 1, 5, 1, 5, 1, 6, 1, 6, 1, 6, 1, 6, 1, 7, 1, 7, 1, 7, 1, 7, 
	    1, 7, 1, 7, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 9, 1, 9, 1, 
	    9, 1, 9, 1, 9, 1, 9, 1, 9, 1, 9, 1, 10, 1, 10, 1, 10, 1, 10, 1, 11, 
	    1, 11, 1, 11, 1, 11, 1, 11, 1, 12, 1, 12, 1, 12, 1, 12, 1, 13, 1, 13, 
	    1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 13, 1, 14, 1, 14, 
	    1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 15, 1, 15, 1, 15, 1, 15, 
	    1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 
	    1, 16, 1, 16, 1, 16, 1, 17, 1, 17, 1, 17, 1, 18, 1, 18, 1, 18, 1, 18, 
	    1, 19, 1, 19, 1, 19, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 
	    1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 20, 1, 21, 1, 21, 1, 21, 
	    1, 21, 1, 21, 1, 21, 1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 
	    1, 22, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 24, 1, 24, 
	    1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 25, 1, 25, 1, 26, 1, 26, 1, 26, 
	    1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 
	    1, 27, 1, 27, 1, 27, 1, 28, 1, 28, 1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 
	    1, 29, 1, 29, 1, 29, 1, 29, 1, 29, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 
	    1, 30, 1, 30, 1, 30, 1, 31, 1, 31, 1, 31, 1, 32, 1, 32, 1, 32, 1, 32, 
	    1, 32, 1, 33, 1, 33, 1, 33, 1, 33, 1, 33, 1, 33, 1, 34, 1, 34, 1, 34, 
	    1, 34, 1, 34, 1, 34, 1, 34, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 
	    1, 35, 1, 35, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 37, 1, 37, 1, 37, 
	    1, 37, 1, 37, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 
	    1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 40, 
	    1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 41, 1, 41, 1, 41, 1, 41, 
	    1, 41, 1, 42, 1, 42, 1, 42, 1, 42, 1, 42, 1, 42, 1, 42, 1, 42, 1, 42, 
	    1, 42, 1, 43, 1, 43, 1, 43, 1, 43, 1, 43, 1, 43, 1, 44, 1, 44, 1, 44, 
	    1, 44, 1, 44, 1, 44, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 
	    1, 45, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 
	    1, 46, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 1, 48, 
	    1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 49, 1, 49, 1, 49, 
	    1, 49, 1, 49, 1, 49, 1, 49, 1, 49, 1, 49, 1, 49, 1, 50, 1, 50, 1, 50, 
	    1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 51, 1, 51, 
	    1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 
	    1, 52, 1, 52, 1, 52, 1, 53, 1, 53, 1, 54, 1, 54, 1, 54, 1, 54, 1, 54, 
	    1, 54, 1, 54, 1, 54, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 
	    1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 57, 1, 57, 
	    1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 
	    1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 
	    1, 58, 1, 58, 1, 58, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 
	    1, 59, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 
	    1, 60, 1, 60, 1, 60, 1, 61, 1, 61, 1, 61, 1, 61, 1, 61, 1, 61, 1, 61, 
	    1, 61, 1, 61, 1, 61, 1, 61, 1, 62, 1, 62, 1, 62, 1, 62, 1, 62, 1, 62, 
	    1, 62, 1, 62, 1, 62, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 64, 1, 64, 
	    1, 64, 1, 64, 1, 64, 1, 64, 1, 65, 1, 65, 1, 65, 1, 65, 1, 65, 1, 65, 
	    1, 65, 1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 67, 1, 67, 1, 67, 
	    1, 67, 1, 67, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 
	    1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 1, 69, 
	    1, 69, 1, 69, 1, 69, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 
	    1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 71, 1, 71, 1, 71, 1, 71, 
	    1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 
	    1, 71, 1, 71, 1, 71, 1, 71, 1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 
	    1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 73, 1, 73, 1, 73, 
	    1, 73, 1, 74, 1, 74, 1, 74, 1, 74, 1, 74, 1, 75, 1, 75, 1, 75, 1, 75, 
	    1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 76, 1, 76, 1, 76, 1, 76, 
	    1, 76, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 78, 1, 78, 1, 78, 1, 78, 
	    1, 78, 1, 78, 1, 78, 1, 78, 1, 78, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 
	    1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 
	    1, 80, 1, 80, 1, 80, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 
	    1, 81, 1, 81, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 
	    1, 82, 1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 
	    1, 83, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 
	    1, 84, 1, 84, 1, 84, 1, 84, 1, 85, 1, 85, 1, 85, 1, 85, 1, 86, 1, 86, 
	    1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 87, 1, 87, 1, 87, 1, 87, 
	    1, 87, 1, 87, 1, 87, 1, 87, 1, 88, 1, 88, 1, 88, 1, 88, 1, 88, 1, 88, 
	    1, 88, 1, 89, 1, 89, 1, 89, 1, 89, 1, 89, 1, 89, 1, 89, 1, 89, 1, 90, 
	    1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 91, 1, 91, 1, 91, 
	    1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 
	    1, 92, 1, 92, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 
	    1, 93, 1, 93, 1, 94, 1, 94, 1, 94, 1, 94, 1, 94, 1, 95, 1, 95, 1, 95, 
	    1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 96, 1, 96, 1, 96, 1, 96, 
	    1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 
	    1, 97, 1, 97, 1, 97, 1, 97, 1, 98, 1, 98, 1, 98, 1, 98, 1, 98, 1, 98, 
	    1, 98, 1, 98, 1, 98, 1, 98, 1, 98, 1, 98, 1, 99, 1, 99, 1, 99, 1, 99, 
	    1, 99, 1, 99, 1, 99, 1, 99, 1, 99, 1, 99, 1, 100, 1, 100, 1, 100, 1, 
	    100, 1, 100, 1, 100, 1, 100, 1, 100, 1, 100, 1, 101, 1, 101, 1, 101, 
	    1, 101, 1, 101, 1, 101, 1, 101, 1, 101, 1, 101, 1, 101, 1, 101, 1, 
	    102, 1, 102, 1, 102, 1, 102, 1, 103, 1, 103, 1, 103, 1, 104, 1, 104, 
	    1, 104, 1, 104, 1, 104, 1, 104, 1, 104, 1, 105, 1, 105, 1, 105, 1, 
	    105, 1, 105, 1, 106, 1, 106, 1, 106, 1, 106, 1, 106, 1, 107, 1, 107, 
	    1, 107, 1, 107, 1, 108, 1, 108, 1, 108, 1, 108, 1, 108, 1, 108, 1, 
	    108, 1, 109, 1, 109, 1, 109, 1, 109, 1, 109, 1, 109, 1, 109, 1, 109, 
	    1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 
	    110, 1, 110, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 
	    1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 
	    112, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 
	    1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 1, 
	    115, 1, 115, 1, 115, 1, 115, 1, 115, 1, 115, 1, 115, 1, 116, 1, 116, 
	    1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 1, 117, 1, 117, 1, 
	    117, 1, 117, 1, 117, 1, 117, 1, 117, 1, 118, 1, 118, 1, 118, 1, 118, 
	    1, 118, 1, 118, 1, 118, 1, 118, 1, 118, 1, 119, 1, 119, 1, 119, 1, 
	    119, 1, 119, 1, 119, 1, 119, 1, 119, 1, 119, 1, 120, 1, 120, 1, 120, 
	    1, 120, 1, 120, 1, 120, 1, 120, 1, 120, 1, 121, 1, 121, 1, 121, 1, 
	    121, 1, 121, 1, 121, 1, 122, 1, 122, 1, 122, 1, 122, 1, 122, 1, 122, 
	    1, 123, 1, 123, 1, 123, 1, 123, 1, 123, 1, 123, 1, 123, 1, 124, 1, 
	    124, 1, 124, 1, 124, 1, 124, 1, 124, 1, 124, 1, 125, 1, 125, 1, 125, 
	    1, 125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 
	    126, 1, 126, 1, 126, 1, 126, 1, 126, 1, 126, 1, 127, 1, 127, 1, 127, 
	    1, 127, 1, 127, 1, 127, 1, 128, 1, 128, 1, 128, 1, 128, 1, 128, 1, 
	    128, 1, 128, 1, 128, 1, 128, 1, 128, 1, 129, 1, 129, 1, 129, 1, 129, 
	    1, 130, 1, 130, 1, 130, 1, 130, 1, 130, 1, 130, 1, 130, 1, 130, 1, 
	    131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 132, 1, 132, 
	    1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 
	    133, 1, 133, 1, 133, 1, 133, 1, 133, 1, 134, 1, 134, 1, 134, 1, 134, 
	    1, 134, 1, 134, 1, 134, 1, 134, 1, 134, 1, 134, 1, 135, 1, 135, 1, 
	    135, 1, 135, 1, 135, 1, 136, 1, 136, 1, 136, 1, 136, 1, 136, 1, 136, 
	    1, 136, 1, 136, 1, 136, 1, 137, 1, 137, 1, 137, 1, 137, 1, 137, 1, 
	    137, 1, 137, 1, 137, 1, 137, 1, 137, 1, 138, 1, 138, 1, 138, 1, 138, 
	    1, 138, 1, 138, 1, 138, 1, 138, 1, 138, 1, 138, 1, 139, 1, 139, 1, 
	    139, 1, 139, 1, 139, 1, 139, 1, 139, 1, 140, 1, 140, 1, 140, 1, 140, 
	    1, 140, 1, 140, 1, 141, 1, 141, 1, 141, 1, 141, 1, 141, 1, 141, 1, 
	    142, 1, 142, 1, 142, 1, 142, 1, 142, 1, 142, 1, 142, 1, 142, 1, 142, 
	    1, 143, 1, 143, 1, 143, 1, 143, 1, 143, 1, 143, 1, 143, 1, 144, 1, 
	    144, 1, 144, 1, 144, 1, 144, 1, 145, 1, 145, 1, 145, 1, 145, 1, 145, 
	    1, 145, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 
	    146, 1, 146, 1, 146, 1, 146, 1, 147, 1, 147, 1, 147, 1, 147, 1, 147, 
	    1, 147, 1, 147, 1, 147, 1, 147, 1, 148, 1, 148, 1, 148, 1, 149, 1, 
	    149, 1, 149, 1, 149, 1, 149, 1, 149, 1, 149, 1, 150, 1, 150, 1, 150, 
	    1, 150, 1, 150, 1, 150, 1, 150, 1, 150, 1, 150, 1, 150, 1, 151, 1, 
	    151, 1, 151, 1, 151, 1, 151, 1, 151, 1, 151, 1, 152, 1, 152, 1, 152, 
	    1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 
	    154, 1, 154, 1, 154, 1, 154, 1, 154, 1, 154, 1, 155, 1, 155, 1, 155, 
	    1, 155, 1, 155, 1, 155, 1, 155, 1, 155, 1, 156, 1, 156, 1, 156, 1, 
	    156, 1, 156, 1, 156, 1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 
	    1, 157, 1, 158, 1, 158, 1, 158, 1, 158, 1, 158, 1, 158, 1, 159, 1, 
	    159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 
	    1, 159, 1, 159, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 1, 
	    160, 1, 161, 1, 161, 1, 161, 1, 161, 1, 161, 1, 161, 1, 161, 1, 161, 
	    1, 161, 1, 161, 1, 162, 1, 162, 1, 162, 1, 162, 1, 162, 1, 162, 1, 
	    162, 1, 162, 1, 162, 1, 163, 1, 163, 1, 163, 1, 163, 1, 164, 1, 164, 
	    1, 164, 1, 164, 1, 164, 1, 164, 1, 164, 1, 164, 1, 165, 1, 165, 1, 
	    165, 1, 165, 1, 165, 1, 166, 1, 166, 1, 166, 1, 166, 1, 166, 1, 166, 
	    1, 166, 1, 166, 1, 167, 1, 167, 1, 167, 1, 168, 1, 168, 1, 168, 1, 
	    168, 1, 168, 1, 168, 1, 169, 1, 169, 1, 169, 1, 169, 1, 169, 1, 169, 
	    1, 170, 1, 170, 1, 170, 1, 170, 1, 170, 1, 171, 1, 171, 1, 171, 1, 
	    171, 1, 172, 1, 172, 1, 172, 1, 172, 1, 172, 1, 173, 1, 173, 1, 173, 
	    1, 173, 1, 173, 1, 173, 1, 173, 1, 173, 1, 173, 1, 174, 1, 174, 1, 
	    174, 1, 174, 1, 174, 1, 175, 1, 175, 1, 175, 1, 175, 1, 175, 1, 175, 
	    1, 175, 1, 175, 1, 176, 1, 176, 1, 176, 1, 176, 1, 176, 1, 177, 1, 
	    177, 1, 177, 1, 177, 1, 177, 1, 177, 1, 177, 1, 177, 1, 178, 1, 178, 
	    1, 178, 1, 178, 1, 178, 1, 179, 1, 179, 1, 179, 1, 179, 1, 179, 1, 
	    180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 181, 1, 181, 1, 181, 
	    1, 181, 1, 181, 1, 181, 1, 182, 1, 182, 1, 182, 1, 182, 1, 182, 1, 
	    183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 184, 
	    1, 184, 1, 184, 1, 184, 1, 184, 1, 185, 1, 185, 1, 185, 1, 185, 1, 
	    185, 1, 186, 1, 186, 1, 186, 1, 186, 1, 186, 1, 186, 1, 187, 1, 187, 
	    1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 1, 188, 1, 
	    188, 1, 188, 1, 188, 1, 188, 1, 189, 1, 189, 1, 189, 1, 189, 1, 189, 
	    1, 189, 1, 190, 1, 190, 1, 190, 1, 190, 1, 190, 1, 190, 1, 190, 1, 
	    190, 1, 191, 1, 191, 1, 191, 1, 191, 1, 191, 1, 192, 1, 192, 1, 192, 
	    1, 192, 1, 192, 1, 192, 1, 193, 1, 193, 1, 193, 1, 193, 1, 194, 1, 
	    194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 
	    1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 194, 1, 195, 1, 
	    195, 1, 195, 1, 195, 1, 195, 1, 195, 1, 195, 1, 195, 1, 196, 1, 196, 
	    1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 
	    196, 1, 196, 1, 196, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 
	    1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 
	    198, 1, 198, 1, 198, 1, 198, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 
	    1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 
	    200, 1, 200, 1, 200, 1, 200, 1, 200, 1, 200, 1, 200, 1, 200, 1, 200, 
	    1, 200, 1, 200, 1, 200, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 
	    201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 202, 
	    1, 202, 1, 202, 1, 202, 1, 202, 1, 202, 1, 203, 1, 203, 1, 203, 1, 
	    203, 1, 203, 1, 203, 1, 203, 1, 204, 1, 204, 1, 204, 1, 204, 1, 204, 
	    1, 204, 1, 204, 1, 204, 1, 205, 1, 205, 1, 205, 1, 205, 1, 205, 1, 
	    206, 1, 206, 1, 206, 1, 206, 1, 206, 1, 206, 1, 206, 1, 206, 1, 206, 
	    1, 207, 1, 207, 1, 207, 1, 207, 1, 207, 1, 207, 1, 208, 1, 208, 1, 
	    208, 1, 208, 1, 208, 1, 208, 1, 208, 1, 209, 1, 209, 1, 209, 1, 209, 
	    1, 209, 1, 210, 1, 210, 1, 210, 1, 210, 1, 210, 1, 211, 1, 211, 1, 
	    211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 212, 
	    1, 212, 1, 212, 1, 212, 1, 212, 1, 212, 1, 212, 1, 212, 1, 212, 1, 
	    212, 1, 212, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 
	    1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 214, 1, 214, 1, 
	    214, 1, 214, 1, 214, 1, 214, 1, 214, 1, 214, 1, 214, 1, 214, 1, 214, 
	    1, 215, 1, 215, 1, 215, 1, 215, 1, 215, 1, 215, 1, 215, 1, 215, 1, 
	    215, 1, 215, 1, 215, 1, 215, 1, 216, 1, 216, 1, 216, 1, 216, 1, 216, 
	    1, 216, 1, 216, 1, 216, 1, 217, 1, 217, 1, 217, 1, 218, 1, 218, 1, 
	    218, 1, 218, 1, 218, 1, 219, 1, 219, 1, 219, 1, 219, 1, 220, 1, 220, 
	    1, 220, 1, 220, 1, 220, 1, 221, 1, 221, 1, 221, 1, 221, 1, 221, 1, 
	    221, 1, 222, 1, 222, 1, 222, 1, 222, 1, 222, 1, 222, 1, 222, 1, 222, 
	    1, 223, 1, 223, 1, 223, 1, 224, 1, 224, 1, 224, 1, 224, 1, 224, 1, 
	    224, 1, 224, 1, 225, 1, 225, 1, 225, 1, 226, 1, 226, 1, 226, 1, 226, 
	    1, 226, 1, 227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 
	    227, 1, 227, 1, 228, 1, 228, 1, 228, 1, 228, 1, 228, 1, 228, 1, 228, 
	    1, 229, 1, 229, 1, 229, 1, 229, 1, 229, 1, 229, 1, 229, 1, 229, 1, 
	    230, 1, 230, 1, 230, 1, 231, 1, 231, 1, 231, 1, 231, 1, 231, 1, 231, 
	    1, 232, 1, 232, 1, 232, 1, 232, 1, 233, 1, 233, 1, 233, 1, 233, 1, 
	    233, 1, 233, 1, 234, 1, 234, 1, 234, 1, 234, 1, 234, 1, 234, 1, 234, 
	    1, 234, 1, 234, 1, 234, 1, 234, 1, 234, 1, 234, 1, 235, 1, 235, 1, 
	    235, 1, 235, 1, 235, 1, 236, 1, 236, 1, 236, 1, 236, 1, 236, 1, 236, 
	    1, 236, 1, 236, 1, 236, 1, 237, 1, 237, 1, 237, 1, 237, 1, 237, 1, 
	    237, 1, 237, 1, 237, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 
	    1, 238, 1, 238, 1, 238, 1, 238, 1, 239, 1, 239, 1, 239, 1, 239, 1, 
	    239, 1, 239, 1, 239, 1, 239, 1, 239, 1, 239, 1, 240, 1, 240, 1, 240, 
	    1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 
	    240, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 
	    1, 241, 1, 241, 1, 241, 1, 242, 1, 242, 1, 242, 1, 242, 1, 242, 1, 
	    242, 1, 242, 1, 242, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 
	    1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 
	    243, 1, 243, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 
	    1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 
	    244, 1, 245, 1, 245, 1, 245, 1, 245, 1, 245, 1, 245, 1, 246, 1, 246, 
	    1, 246, 1, 246, 1, 246, 1, 246, 1, 246, 1, 246, 1, 247, 1, 247, 1, 
	    247, 1, 247, 1, 247, 1, 247, 1, 247, 1, 247, 1, 247, 1, 248, 1, 248, 
	    1, 248, 1, 248, 1, 248, 1, 248, 1, 248, 1, 248, 1, 248, 1, 248, 1, 
	    249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 250, 
	    1, 250, 1, 250, 1, 250, 1, 250, 1, 250, 1, 250, 1, 250, 1, 250, 1, 
	    250, 1, 250, 1, 251, 1, 251, 1, 251, 1, 251, 1, 251, 1, 251, 1, 251, 
	    1, 251, 1, 251, 1, 251, 1, 251, 1, 252, 1, 252, 1, 252, 1, 252, 1, 
	    252, 1, 252, 1, 253, 1, 253, 1, 253, 1, 253, 1, 253, 1, 253, 1, 254, 
	    1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 1, 255, 1, 
	    255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 256, 1, 256, 
	    1, 256, 1, 256, 1, 256, 1, 256, 1, 257, 1, 257, 1, 257, 1, 257, 1, 
	    257, 1, 257, 1, 258, 1, 258, 1, 258, 1, 258, 1, 258, 1, 258, 1, 259, 
	    1, 259, 1, 259, 1, 259, 1, 259, 1, 260, 1, 260, 1, 260, 1, 260, 1, 
	    260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 
	    1, 261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 
	    261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 262, 1, 262, 1, 262, 1, 262, 
	    1, 262, 1, 262, 1, 262, 1, 262, 1, 263, 1, 263, 1, 263, 1, 263, 1, 
	    263, 1, 263, 1, 263, 1, 263, 1, 263, 1, 263, 1, 264, 1, 264, 1, 264, 
	    1, 264, 1, 264, 1, 264, 1, 264, 1, 265, 1, 265, 1, 265, 1, 265, 1, 
	    265, 1, 265, 1, 265, 1, 266, 1, 266, 1, 266, 1, 266, 1, 266, 1, 266, 
	    1, 266, 1, 266, 1, 266, 1, 266, 1, 267, 1, 267, 1, 267, 1, 267, 1, 
	    267, 1, 267, 1, 267, 1, 267, 1, 267, 1, 267, 1, 267, 1, 268, 1, 268, 
	    1, 268, 1, 268, 1, 268, 1, 268, 1, 268, 1, 268, 1, 269, 1, 269, 1, 
	    269, 1, 269, 1, 269, 1, 269, 1, 269, 1, 270, 1, 270, 1, 270, 1, 270, 
	    1, 270, 1, 270, 1, 270, 1, 271, 1, 271, 1, 271, 1, 271, 1, 271, 1, 
	    271, 1, 271, 1, 271, 1, 271, 1, 271, 1, 271, 1, 272, 1, 272, 1, 272, 
	    1, 272, 1, 272, 1, 272, 1, 272, 1, 272, 1, 273, 1, 273, 1, 273, 1, 
	    273, 1, 273, 1, 273, 1, 274, 1, 274, 1, 274, 1, 274, 1, 274, 1, 274, 
	    1, 274, 1, 274, 1, 275, 1, 275, 1, 275, 1, 275, 1, 275, 1, 275, 1, 
	    275, 1, 275, 1, 275, 1, 276, 1, 276, 1, 276, 1, 276, 1, 276, 1, 276, 
	    1, 276, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 
	    277, 1, 278, 1, 278, 1, 278, 1, 278, 1, 278, 1, 278, 1, 278, 1, 279, 
	    1, 279, 1, 279, 1, 279, 1, 279, 1, 279, 1, 280, 1, 280, 1, 280, 1, 
	    280, 1, 280, 1, 280, 1, 281, 1, 281, 1, 281, 1, 281, 1, 281, 1, 282, 
	    1, 282, 1, 282, 1, 282, 1, 282, 1, 282, 1, 283, 1, 283, 1, 283, 1, 
	    283, 1, 283, 1, 283, 1, 283, 1, 283, 1, 283, 1, 284, 1, 284, 1, 284, 
	    1, 284, 1, 284, 1, 284, 1, 284, 1, 285, 1, 285, 1, 285, 1, 285, 1, 
	    286, 1, 286, 1, 286, 1, 286, 1, 286, 1, 287, 1, 287, 1, 287, 1, 287, 
	    1, 287, 1, 287, 1, 287, 1, 288, 1, 288, 1, 288, 1, 288, 1, 288, 1, 
	    288, 1, 288, 1, 288, 1, 289, 1, 289, 1, 289, 1, 289, 1, 289, 1, 289, 
	    1, 289, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 
	    290, 1, 291, 1, 291, 1, 291, 1, 291, 1, 291, 1, 291, 1, 291, 1, 291, 
	    1, 291, 1, 292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 
	    293, 1, 293, 1, 293, 1, 293, 1, 293, 1, 294, 1, 294, 1, 294, 1, 294, 
	    1, 294, 1, 294, 1, 294, 1, 294, 1, 294, 1, 294, 1, 295, 1, 295, 1, 
	    295, 1, 295, 1, 295, 1, 295, 1, 296, 1, 296, 1, 296, 1, 296, 1, 296, 
	    1, 296, 1, 296, 1, 296, 1, 296, 1, 296, 1, 296, 1, 296, 1, 296, 1, 
	    296, 1, 296, 1, 296, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 
	    1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 1, 298, 1, 
	    298, 1, 298, 1, 298, 1, 299, 1, 299, 1, 299, 1, 299, 1, 299, 1, 300, 
	    1, 300, 1, 300, 1, 300, 1, 300, 1, 300, 1, 301, 1, 301, 1, 301, 1, 
	    301, 1, 301, 1, 302, 1, 302, 1, 302, 1, 302, 1, 302, 1, 302, 1, 302, 
	    1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 304, 1, 
	    304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 305, 
	    1, 305, 1, 305, 1, 305, 1, 305, 1, 306, 1, 306, 1, 306, 1, 306, 1, 
	    306, 1, 307, 1, 307, 1, 307, 1, 307, 1, 307, 1, 307, 1, 307, 1, 308, 
	    1, 308, 1, 308, 1, 308, 1, 308, 1, 308, 1, 308, 1, 309, 1, 309, 1, 
	    309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 310, 1, 310, 
	    1, 310, 1, 310, 1, 311, 1, 311, 1, 311, 1, 311, 1, 311, 1, 311, 1, 
	    312, 1, 312, 1, 312, 1, 312, 1, 312, 1, 312, 1, 312, 1, 312, 1, 312, 
	    1, 312, 1, 312, 1, 313, 1, 313, 1, 313, 1, 313, 1, 313, 1, 313, 1, 
	    313, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 
	    1, 314, 1, 315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 
	    316, 1, 316, 1, 316, 1, 316, 1, 316, 1, 316, 1, 316, 1, 316, 1, 316, 
	    1, 316, 1, 317, 1, 317, 1, 317, 1, 317, 1, 317, 1, 317, 1, 317, 1, 
	    317, 1, 317, 1, 317, 1, 317, 1, 318, 1, 318, 1, 318, 1, 318, 1, 318, 
	    1, 318, 1, 318, 1, 319, 1, 319, 1, 319, 1, 319, 1, 319, 1, 319, 1, 
	    319, 1, 320, 1, 320, 1, 320, 1, 320, 1, 320, 1, 320, 1, 320, 1, 320, 
	    1, 320, 1, 320, 1, 321, 1, 321, 1, 321, 1, 321, 1, 321, 1, 322, 1, 
	    322, 1, 322, 1, 322, 1, 322, 1, 322, 1, 322, 1, 322, 1, 322, 1, 322, 
	    1, 322, 1, 322, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 
	    323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 
	    1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 1, 325, 1, 325, 1, 
	    325, 1, 325, 1, 325, 1, 325, 1, 325, 1, 326, 1, 326, 1, 326, 1, 326, 
	    1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 
	    327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 328, 1, 328, 
	    1, 328, 1, 328, 1, 328, 1, 328, 1, 328, 1, 328, 1, 328, 1, 328, 1, 
	    328, 1, 328, 1, 328, 1, 328, 1, 329, 1, 329, 1, 329, 1, 329, 1, 329, 
	    1, 330, 1, 330, 1, 330, 1, 330, 1, 330, 1, 330, 1, 330, 1, 330, 1, 
	    330, 1, 330, 1, 331, 1, 331, 1, 331, 1, 331, 1, 331, 1, 331, 1, 331, 
	    1, 331, 1, 331, 1, 331, 1, 331, 1, 332, 1, 332, 1, 332, 1, 332, 1, 
	    332, 1, 332, 1, 332, 1, 333, 1, 333, 1, 333, 1, 333, 1, 333, 1, 334, 
	    1, 334, 1, 334, 1, 334, 1, 334, 1, 335, 1, 335, 1, 335, 1, 335, 1, 
	    335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 336, 1, 336, 1, 336, 1, 336, 
	    1, 336, 1, 336, 1, 336, 1, 336, 1, 336, 1, 336, 1, 337, 1, 337, 1, 
	    337, 1, 337, 1, 337, 1, 337, 1, 337, 1, 337, 1, 337, 1, 337, 1, 337, 
	    1, 337, 1, 337, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 
	    338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 339, 
	    1, 339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 
	    339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 340, 1, 340, 1, 340, 1, 340, 
	    1, 340, 1, 340, 1, 340, 1, 340, 1, 340, 1, 340, 1, 340, 1, 340, 1, 
	    340, 1, 340, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 
	    1, 341, 1, 342, 1, 342, 1, 342, 1, 343, 1, 343, 1, 343, 1, 343, 1, 
	    343, 1, 343, 1, 344, 1, 344, 1, 344, 1, 344, 1, 344, 1, 344, 1, 344, 
	    1, 344, 1, 344, 1, 345, 1, 345, 1, 345, 1, 345, 1, 345, 1, 345, 1, 
	    345, 1, 345, 1, 345, 1, 345, 1, 345, 1, 345, 1, 346, 1, 346, 1, 346, 
	    1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 
	    346, 1, 346, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 
	    1, 347, 1, 347, 1, 347, 1, 348, 1, 348, 1, 348, 1, 348, 1, 348, 1, 
	    349, 1, 349, 1, 349, 1, 349, 1, 349, 1, 350, 1, 350, 1, 350, 1, 350, 
	    1, 350, 1, 350, 1, 350, 1, 350, 1, 350, 1, 351, 1, 351, 1, 351, 1, 
	    351, 1, 351, 1, 351, 1, 351, 1, 351, 1, 351, 1, 352, 1, 352, 1, 352, 
	    1, 352, 1, 352, 1, 353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 
	    353, 1, 353, 1, 353, 1, 353, 1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 
	    1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 1, 355, 1, 355, 1, 355, 1, 
	    355, 1, 355, 1, 355, 1, 355, 1, 355, 1, 356, 1, 356, 1, 356, 1, 356, 
	    1, 356, 1, 356, 1, 357, 1, 357, 1, 357, 1, 357, 1, 357, 1, 357, 1, 
	    357, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 
	    1, 359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 360, 1, 
	    360, 1, 360, 1, 360, 1, 360, 1, 360, 1, 360, 1, 360, 1, 361, 1, 361, 
	    1, 361, 1, 361, 1, 361, 1, 361, 1, 362, 1, 362, 1, 362, 1, 362, 1, 
	    362, 1, 362, 1, 362, 1, 363, 1, 363, 1, 363, 1, 363, 1, 364, 1, 364, 
	    1, 364, 1, 364, 1, 364, 1, 365, 1, 365, 1, 365, 1, 365, 1, 365, 1, 
	    365, 1, 366, 1, 366, 1, 366, 1, 366, 1, 366, 1, 366, 1, 366, 1, 367, 
	    1, 367, 1, 367, 1, 367, 1, 368, 1, 368, 1, 368, 1, 368, 1, 368, 1, 
	    368, 1, 368, 1, 368, 1, 369, 1, 369, 1, 369, 1, 369, 1, 369, 1, 369, 
	    1, 369, 1, 369, 1, 370, 1, 370, 1, 370, 1, 370, 1, 370, 1, 370, 1, 
	    370, 1, 370, 1, 371, 1, 371, 1, 371, 1, 371, 1, 371, 1, 372, 1, 372, 
	    1, 372, 1, 372, 1, 372, 1, 372, 1, 373, 1, 373, 1, 373, 1, 373, 1, 
	    373, 1, 374, 1, 374, 1, 374, 1, 374, 1, 374, 1, 375, 1, 375, 1, 375, 
	    1, 375, 1, 375, 1, 375, 1, 376, 1, 376, 1, 376, 1, 376, 1, 376, 1, 
	    377, 1, 377, 1, 377, 1, 377, 1, 377, 1, 377, 1, 378, 1, 378, 1, 378, 
	    1, 378, 1, 378, 1, 378, 1, 379, 1, 379, 1, 379, 1, 379, 1, 379, 1, 
	    379, 1, 379, 1, 380, 1, 380, 1, 380, 1, 380, 1, 380, 1, 381, 1, 381, 
	    1, 381, 1, 381, 1, 381, 1, 381, 1, 381, 1, 382, 1, 382, 1, 382, 1, 
	    382, 1, 382, 1, 383, 1, 383, 1, 383, 1, 383, 1, 383, 1, 383, 1, 384, 
	    1, 384, 1, 384, 1, 384, 1, 384, 1, 385, 1, 385, 1, 386, 1, 386, 1, 
	    387, 1, 387, 1, 388, 1, 388, 1, 389, 1, 389, 1, 390, 1, 390, 1, 391, 
	    1, 391, 1, 391, 1, 392, 1, 392, 1, 393, 1, 393, 1, 393, 1, 393, 1, 
	    394, 1, 394, 1, 394, 1, 394, 1, 395, 1, 395, 1, 395, 1, 396, 1, 396, 
	    1, 396, 1, 396, 3, 396, 3823, 8, 396, 1, 397, 1, 397, 1, 398, 1, 398, 
	    1, 398, 1, 399, 1, 399, 1, 400, 1, 400, 1, 400, 1, 401, 1, 401, 1, 
	    402, 1, 402, 1, 403, 1, 403, 1, 404, 1, 404, 1, 405, 1, 405, 1, 406, 
	    1, 406, 1, 406, 1, 407, 1, 407, 1, 408, 1, 408, 1, 409, 1, 409, 1, 
	    410, 1, 410, 1, 411, 1, 411, 1, 412, 1, 412, 1, 413, 1, 413, 1, 414, 
	    1, 414, 1, 414, 1, 415, 1, 415, 1, 416, 1, 416, 1, 416, 1, 417, 1, 
	    417, 1, 417, 5, 417, 3873, 8, 417, 10, 417, 12, 417, 3876, 9, 417, 
	    1, 417, 1, 417, 1, 417, 1, 417, 1, 417, 5, 417, 3883, 8, 417, 10, 417, 
	    12, 417, 3886, 9, 417, 1, 417, 1, 417, 1, 417, 1, 417, 1, 417, 5, 417, 
	    3893, 8, 417, 10, 417, 12, 417, 3896, 9, 417, 1, 417, 3, 417, 3899, 
	    8, 417, 1, 418, 1, 418, 1, 418, 5, 418, 3904, 8, 418, 10, 418, 12, 
	    418, 3907, 9, 418, 1, 418, 1, 418, 1, 419, 1, 419, 1, 419, 1, 419, 
	    1, 419, 1, 419, 1, 419, 5, 419, 3918, 8, 419, 10, 419, 12, 419, 3921, 
	    9, 419, 1, 419, 1, 419, 1, 420, 4, 420, 3926, 8, 420, 11, 420, 12, 
	    420, 3927, 1, 421, 4, 421, 3931, 8, 421, 11, 421, 12, 421, 3932, 1, 
	    421, 1, 421, 1, 422, 4, 422, 3938, 8, 422, 11, 422, 12, 422, 3939, 
	    1, 422, 1, 422, 1, 423, 4, 423, 3945, 8, 423, 11, 423, 12, 423, 3946, 
	    1, 423, 1, 423, 1, 424, 4, 424, 3952, 8, 424, 11, 424, 12, 424, 3953, 
	    1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 3, 424, 3962, 8, 424, 
	    1, 425, 1, 425, 1, 425, 1, 426, 4, 426, 3968, 8, 426, 11, 426, 12, 
	    426, 3969, 1, 426, 3, 426, 3973, 8, 426, 1, 426, 1, 426, 1, 426, 1, 
	    426, 3, 426, 3979, 8, 426, 1, 426, 1, 426, 1, 426, 3, 426, 3984, 8, 
	    426, 1, 427, 4, 427, 3987, 8, 427, 11, 427, 12, 427, 3988, 1, 427, 
	    3, 427, 3992, 8, 427, 1, 427, 1, 427, 1, 427, 1, 427, 3, 427, 3998, 
	    8, 427, 1, 427, 1, 427, 1, 427, 3, 427, 4003, 8, 427, 1, 428, 4, 428, 
	    4006, 8, 428, 11, 428, 12, 428, 4007, 1, 428, 3, 428, 4011, 8, 428, 
	    1, 428, 1, 428, 1, 428, 1, 428, 1, 428, 3, 428, 4018, 8, 428, 1, 428, 
	    1, 428, 1, 428, 1, 428, 1, 428, 3, 428, 4025, 8, 428, 1, 429, 1, 429, 
	    1, 429, 5, 429, 4030, 8, 429, 10, 429, 12, 429, 4033, 9, 429, 1, 429, 
	    1, 429, 3, 429, 4037, 8, 429, 1, 429, 1, 429, 1, 429, 5, 429, 4042, 
	    8, 429, 10, 429, 12, 429, 4045, 9, 429, 1, 430, 1, 430, 1, 430, 1, 
	    430, 5, 430, 4051, 8, 430, 10, 430, 12, 430, 4054, 9, 430, 1, 430, 
	    1, 430, 1, 431, 1, 431, 1, 431, 1, 432, 1, 432, 3, 432, 4063, 8, 432, 
	    1, 432, 4, 432, 4066, 8, 432, 11, 432, 12, 432, 4067, 1, 433, 1, 433, 
	    1, 434, 1, 434, 1, 435, 4, 435, 4075, 8, 435, 11, 435, 12, 435, 4076, 
	    1, 435, 1, 435, 5, 435, 4081, 8, 435, 10, 435, 12, 435, 4084, 9, 435, 
	    1, 435, 1, 435, 4, 435, 4088, 8, 435, 11, 435, 12, 435, 4089, 3, 435, 
	    4092, 8, 435, 1, 436, 1, 436, 1, 436, 1, 436, 5, 436, 4098, 8, 436, 
	    10, 436, 12, 436, 4101, 9, 436, 1, 436, 3, 436, 4104, 8, 436, 1, 436, 
	    3, 436, 4107, 8, 436, 1, 436, 1, 436, 1, 437, 1, 437, 1, 437, 1, 437, 
	    1, 437, 5, 437, 4116, 8, 437, 10, 437, 12, 437, 4119, 9, 437, 1, 437, 
	    1, 437, 1, 437, 1, 437, 1, 437, 1, 438, 4, 438, 4127, 8, 438, 11, 438, 
	    12, 438, 4128, 1, 438, 1, 438, 1, 439, 1, 439, 1, 439, 3, 439, 4136, 
	    8, 439, 1, 440, 1, 440, 1, 4117, 0, 441, 1, 1, 3, 2, 5, 3, 7, 4, 9, 
	    5, 11, 6, 13, 7, 15, 8, 17, 9, 19, 10, 21, 11, 23, 12, 25, 13, 27, 
	    14, 29, 15, 31, 16, 33, 17, 35, 18, 37, 19, 39, 20, 41, 21, 43, 22, 
	    45, 23, 47, 24, 49, 25, 51, 26, 53, 27, 55, 28, 57, 29, 59, 30, 61, 
	    31, 63, 32, 65, 33, 67, 34, 69, 35, 71, 36, 73, 37, 75, 38, 77, 39, 
	    79, 40, 81, 41, 83, 42, 85, 43, 87, 44, 89, 45, 91, 46, 93, 47, 95, 
	    48, 97, 49, 99, 50, 101, 51, 103, 52, 105, 53, 107, 54, 109, 55, 111, 
	    56, 113, 57, 115, 58, 117, 59, 119, 60, 121, 61, 123, 62, 125, 63, 
	    127, 64, 129, 65, 131, 66, 133, 67, 135, 68, 137, 69, 139, 70, 141, 
	    71, 143, 72, 145, 73, 147, 74, 149, 75, 151, 76, 153, 77, 155, 78, 
	    157, 79, 159, 80, 161, 81, 163, 82, 165, 83, 167, 84, 169, 85, 171, 
	    86, 173, 87, 175, 88, 177, 89, 179, 90, 181, 91, 183, 92, 185, 93, 
	    187, 94, 189, 95, 191, 96, 193, 97, 195, 98, 197, 99, 199, 100, 201, 
	    101, 203, 102, 205, 103, 207, 104, 209, 105, 211, 106, 213, 107, 215, 
	    108, 217, 109, 219, 110, 221, 111, 223, 112, 225, 113, 227, 114, 229, 
	    115, 231, 116, 233, 117, 235, 118, 237, 119, 239, 120, 241, 121, 243, 
	    122, 245, 123, 247, 124, 249, 125, 251, 126, 253, 127, 255, 128, 257, 
	    129, 259, 130, 261, 131, 263, 132, 265, 133, 267, 134, 269, 135, 271, 
	    136, 273, 137, 275, 138, 277, 139, 279, 140, 281, 141, 283, 142, 285, 
	    143, 287, 144, 289, 145, 291, 146, 293, 147, 295, 148, 297, 149, 299, 
	    150, 301, 151, 303, 152, 305, 153, 307, 154, 309, 155, 311, 156, 313, 
	    157, 315, 158, 317, 159, 319, 160, 321, 161, 323, 162, 325, 163, 327, 
	    164, 329, 165, 331, 166, 333, 167, 335, 168, 337, 169, 339, 170, 341, 
	    171, 343, 172, 345, 173, 347, 174, 349, 175, 351, 176, 353, 177, 355, 
	    178, 357, 179, 359, 180, 361, 181, 363, 182, 365, 183, 367, 184, 369, 
	    185, 371, 186, 373, 187, 375, 188, 377, 189, 379, 190, 381, 191, 383, 
	    192, 385, 193, 387, 194, 389, 195, 391, 196, 393, 197, 395, 198, 397, 
	    199, 399, 200, 401, 201, 403, 202, 405, 203, 407, 204, 409, 205, 411, 
	    206, 413, 207, 415, 208, 417, 209, 419, 210, 421, 211, 423, 212, 425, 
	    213, 427, 214, 429, 215, 431, 216, 433, 217, 435, 218, 437, 219, 439, 
	    220, 441, 221, 443, 222, 445, 223, 447, 224, 449, 225, 451, 226, 453, 
	    227, 455, 228, 457, 229, 459, 230, 461, 231, 463, 232, 465, 233, 467, 
	    234, 469, 235, 471, 236, 473, 237, 475, 238, 477, 239, 479, 240, 481, 
	    241, 483, 242, 485, 243, 487, 244, 489, 245, 491, 246, 493, 247, 495, 
	    248, 497, 249, 499, 250, 501, 251, 503, 252, 505, 253, 507, 254, 509, 
	    255, 511, 256, 513, 257, 515, 258, 517, 259, 519, 260, 521, 261, 523, 
	    262, 525, 263, 527, 264, 529, 265, 531, 266, 533, 267, 535, 268, 537, 
	    269, 539, 270, 541, 271, 543, 272, 545, 273, 547, 274, 549, 275, 551, 
	    276, 553, 277, 555, 278, 557, 279, 559, 280, 561, 281, 563, 282, 565, 
	    283, 567, 284, 569, 285, 571, 286, 573, 287, 575, 288, 577, 289, 579, 
	    290, 581, 291, 583, 292, 585, 293, 587, 294, 589, 295, 591, 296, 593, 
	    297, 595, 298, 597, 299, 599, 300, 601, 301, 603, 302, 605, 303, 607, 
	    304, 609, 305, 611, 306, 613, 307, 615, 308, 617, 309, 619, 310, 621, 
	    311, 623, 312, 625, 313, 627, 314, 629, 315, 631, 316, 633, 317, 635, 
	    318, 637, 319, 639, 320, 641, 321, 643, 322, 645, 323, 647, 324, 649, 
	    325, 651, 326, 653, 327, 655, 328, 657, 329, 659, 330, 661, 331, 663, 
	    332, 665, 333, 667, 334, 669, 335, 671, 336, 673, 337, 675, 338, 677, 
	    339, 679, 340, 681, 341, 683, 342, 685, 343, 687, 344, 689, 345, 691, 
	    346, 693, 347, 695, 348, 697, 349, 699, 350, 701, 351, 703, 352, 705, 
	    353, 707, 354, 709, 355, 711, 356, 713, 357, 715, 358, 717, 359, 719, 
	    360, 721, 361, 723, 362, 725, 363, 727, 364, 729, 365, 731, 366, 733, 
	    367, 735, 368, 737, 369, 739, 370, 741, 371, 743, 372, 745, 373, 747, 
	    374, 749, 375, 751, 376, 753, 377, 755, 378, 757, 379, 759, 380, 761, 
	    381, 763, 382, 765, 383, 767, 384, 769, 385, 771, 386, 773, 387, 775, 
	    388, 777, 389, 779, 390, 781, 391, 783, 392, 785, 393, 787, 394, 789, 
	    395, 791, 396, 793, 397, 795, 398, 797, 399, 799, 400, 801, 401, 803, 
	    402, 805, 403, 807, 404, 809, 405, 811, 406, 813, 407, 815, 408, 817, 
	    409, 819, 410, 821, 411, 823, 412, 825, 413, 827, 414, 829, 415, 831, 
	    416, 833, 417, 835, 418, 837, 419, 839, 420, 841, 421, 843, 422, 845, 
	    423, 847, 424, 849, 425, 851, 426, 853, 427, 855, 428, 857, 429, 859, 
	    430, 861, 431, 863, 432, 865, 0, 867, 0, 869, 0, 871, 0, 873, 433, 
	    875, 434, 877, 435, 879, 436, 881, 437, 1, 0, 11, 2, 0, 39, 39, 92, 
	    92, 1, 0, 39, 39, 1, 0, 34, 34, 2, 0, 34, 34, 92, 92, 1, 0, 96, 96, 
	    2, 0, 43, 43, 45, 45, 1, 0, 48, 57, 1, 0, 65, 90, 2, 0, 10, 10, 13, 
	    13, 3, 0, 9, 10, 13, 13, 32, 32, 2, 0, 34, 34, 39, 39, 4185, 0, 1, 
	    1, 0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 
	    9, 1, 0, 0, 0, 0, 11, 1, 0, 0, 0, 0, 13, 1, 0, 0, 0, 0, 15, 1, 0, 0, 
	    0, 0, 17, 1, 0, 0, 0, 0, 19, 1, 0, 0, 0, 0, 21, 1, 0, 0, 0, 0, 23, 
	    1, 0, 0, 0, 0, 25, 1, 0, 0, 0, 0, 27, 1, 0, 0, 0, 0, 29, 1, 0, 0, 0, 
	    0, 31, 1, 0, 0, 0, 0, 33, 1, 0, 0, 0, 0, 35, 1, 0, 0, 0, 0, 37, 1, 
	    0, 0, 0, 0, 39, 1, 0, 0, 0, 0, 41, 1, 0, 0, 0, 0, 43, 1, 0, 0, 0, 0, 
	    45, 1, 0, 0, 0, 0, 47, 1, 0, 0, 0, 0, 49, 1, 0, 0, 0, 0, 51, 1, 0, 
	    0, 0, 0, 53, 1, 0, 0, 0, 0, 55, 1, 0, 0, 0, 0, 57, 1, 0, 0, 0, 0, 59, 
	    1, 0, 0, 0, 0, 61, 1, 0, 0, 0, 0, 63, 1, 0, 0, 0, 0, 65, 1, 0, 0, 0, 
	    0, 67, 1, 0, 0, 0, 0, 69, 1, 0, 0, 0, 0, 71, 1, 0, 0, 0, 0, 73, 1, 
	    0, 0, 0, 0, 75, 1, 0, 0, 0, 0, 77, 1, 0, 0, 0, 0, 79, 1, 0, 0, 0, 0, 
	    81, 1, 0, 0, 0, 0, 83, 1, 0, 0, 0, 0, 85, 1, 0, 0, 0, 0, 87, 1, 0, 
	    0, 0, 0, 89, 1, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0, 93, 1, 0, 0, 0, 0, 95, 
	    1, 0, 0, 0, 0, 97, 1, 0, 0, 0, 0, 99, 1, 0, 0, 0, 0, 101, 1, 0, 0, 
	    0, 0, 103, 1, 0, 0, 0, 0, 105, 1, 0, 0, 0, 0, 107, 1, 0, 0, 0, 0, 109, 
	    1, 0, 0, 0, 0, 111, 1, 0, 0, 0, 0, 113, 1, 0, 0, 0, 0, 115, 1, 0, 0, 
	    0, 0, 117, 1, 0, 0, 0, 0, 119, 1, 0, 0, 0, 0, 121, 1, 0, 0, 0, 0, 123, 
	    1, 0, 0, 0, 0, 125, 1, 0, 0, 0, 0, 127, 1, 0, 0, 0, 0, 129, 1, 0, 0, 
	    0, 0, 131, 1, 0, 0, 0, 0, 133, 1, 0, 0, 0, 0, 135, 1, 0, 0, 0, 0, 137, 
	    1, 0, 0, 0, 0, 139, 1, 0, 0, 0, 0, 141, 1, 0, 0, 0, 0, 143, 1, 0, 0, 
	    0, 0, 145, 1, 0, 0, 0, 0, 147, 1, 0, 0, 0, 0, 149, 1, 0, 0, 0, 0, 151, 
	    1, 0, 0, 0, 0, 153, 1, 0, 0, 0, 0, 155, 1, 0, 0, 0, 0, 157, 1, 0, 0, 
	    0, 0, 159, 1, 0, 0, 0, 0, 161, 1, 0, 0, 0, 0, 163, 1, 0, 0, 0, 0, 165, 
	    1, 0, 0, 0, 0, 167, 1, 0, 0, 0, 0, 169, 1, 0, 0, 0, 0, 171, 1, 0, 0, 
	    0, 0, 173, 1, 0, 0, 0, 0, 175, 1, 0, 0, 0, 0, 177, 1, 0, 0, 0, 0, 179, 
	    1, 0, 0, 0, 0, 181, 1, 0, 0, 0, 0, 183, 1, 0, 0, 0, 0, 185, 1, 0, 0, 
	    0, 0, 187, 1, 0, 0, 0, 0, 189, 1, 0, 0, 0, 0, 191, 1, 0, 0, 0, 0, 193, 
	    1, 0, 0, 0, 0, 195, 1, 0, 0, 0, 0, 197, 1, 0, 0, 0, 0, 199, 1, 0, 0, 
	    0, 0, 201, 1, 0, 0, 0, 0, 203, 1, 0, 0, 0, 0, 205, 1, 0, 0, 0, 0, 207, 
	    1, 0, 0, 0, 0, 209, 1, 0, 0, 0, 0, 211, 1, 0, 0, 0, 0, 213, 1, 0, 0, 
	    0, 0, 215, 1, 0, 0, 0, 0, 217, 1, 0, 0, 0, 0, 219, 1, 0, 0, 0, 0, 221, 
	    1, 0, 0, 0, 0, 223, 1, 0, 0, 0, 0, 225, 1, 0, 0, 0, 0, 227, 1, 0, 0, 
	    0, 0, 229, 1, 0, 0, 0, 0, 231, 1, 0, 0, 0, 0, 233, 1, 0, 0, 0, 0, 235, 
	    1, 0, 0, 0, 0, 237, 1, 0, 0, 0, 0, 239, 1, 0, 0, 0, 0, 241, 1, 0, 0, 
	    0, 0, 243, 1, 0, 0, 0, 0, 245, 1, 0, 0, 0, 0, 247, 1, 0, 0, 0, 0, 249, 
	    1, 0, 0, 0, 0, 251, 1, 0, 0, 0, 0, 253, 1, 0, 0, 0, 0, 255, 1, 0, 0, 
	    0, 0, 257, 1, 0, 0, 0, 0, 259, 1, 0, 0, 0, 0, 261, 1, 0, 0, 0, 0, 263, 
	    1, 0, 0, 0, 0, 265, 1, 0, 0, 0, 0, 267, 1, 0, 0, 0, 0, 269, 1, 0, 0, 
	    0, 0, 271, 1, 0, 0, 0, 0, 273, 1, 0, 0, 0, 0, 275, 1, 0, 0, 0, 0, 277, 
	    1, 0, 0, 0, 0, 279, 1, 0, 0, 0, 0, 281, 1, 0, 0, 0, 0, 283, 1, 0, 0, 
	    0, 0, 285, 1, 0, 0, 0, 0, 287, 1, 0, 0, 0, 0, 289, 1, 0, 0, 0, 0, 291, 
	    1, 0, 0, 0, 0, 293, 1, 0, 0, 0, 0, 295, 1, 0, 0, 0, 0, 297, 1, 0, 0, 
	    0, 0, 299, 1, 0, 0, 0, 0, 301, 1, 0, 0, 0, 0, 303, 1, 0, 0, 0, 0, 305, 
	    1, 0, 0, 0, 0, 307, 1, 0, 0, 0, 0, 309, 1, 0, 0, 0, 0, 311, 1, 0, 0, 
	    0, 0, 313, 1, 0, 0, 0, 0, 315, 1, 0, 0, 0, 0, 317, 1, 0, 0, 0, 0, 319, 
	    1, 0, 0, 0, 0, 321, 1, 0, 0, 0, 0, 323, 1, 0, 0, 0, 0, 325, 1, 0, 0, 
	    0, 0, 327, 1, 0, 0, 0, 0, 329, 1, 0, 0, 0, 0, 331, 1, 0, 0, 0, 0, 333, 
	    1, 0, 0, 0, 0, 335, 1, 0, 0, 0, 0, 337, 1, 0, 0, 0, 0, 339, 1, 0, 0, 
	    0, 0, 341, 1, 0, 0, 0, 0, 343, 1, 0, 0, 0, 0, 345, 1, 0, 0, 0, 0, 347, 
	    1, 0, 0, 0, 0, 349, 1, 0, 0, 0, 0, 351, 1, 0, 0, 0, 0, 353, 1, 0, 0, 
	    0, 0, 355, 1, 0, 0, 0, 0, 357, 1, 0, 0, 0, 0, 359, 1, 0, 0, 0, 0, 361, 
	    1, 0, 0, 0, 0, 363, 1, 0, 0, 0, 0, 365, 1, 0, 0, 0, 0, 367, 1, 0, 0, 
	    0, 0, 369, 1, 0, 0, 0, 0, 371, 1, 0, 0, 0, 0, 373, 1, 0, 0, 0, 0, 375, 
	    1, 0, 0, 0, 0, 377, 1, 0, 0, 0, 0, 379, 1, 0, 0, 0, 0, 381, 1, 0, 0, 
	    0, 0, 383, 1, 0, 0, 0, 0, 385, 1, 0, 0, 0, 0, 387, 1, 0, 0, 0, 0, 389, 
	    1, 0, 0, 0, 0, 391, 1, 0, 0, 0, 0, 393, 1, 0, 0, 0, 0, 395, 1, 0, 0, 
	    0, 0, 397, 1, 0, 0, 0, 0, 399, 1, 0, 0, 0, 0, 401, 1, 0, 0, 0, 0, 403, 
	    1, 0, 0, 0, 0, 405, 1, 0, 0, 0, 0, 407, 1, 0, 0, 0, 0, 409, 1, 0, 0, 
	    0, 0, 411, 1, 0, 0, 0, 0, 413, 1, 0, 0, 0, 0, 415, 1, 0, 0, 0, 0, 417, 
	    1, 0, 0, 0, 0, 419, 1, 0, 0, 0, 0, 421, 1, 0, 0, 0, 0, 423, 1, 0, 0, 
	    0, 0, 425, 1, 0, 0, 0, 0, 427, 1, 0, 0, 0, 0, 429, 1, 0, 0, 0, 0, 431, 
	    1, 0, 0, 0, 0, 433, 1, 0, 0, 0, 0, 435, 1, 0, 0, 0, 0, 437, 1, 0, 0, 
	    0, 0, 439, 1, 0, 0, 0, 0, 441, 1, 0, 0, 0, 0, 443, 1, 0, 0, 0, 0, 445, 
	    1, 0, 0, 0, 0, 447, 1, 0, 0, 0, 0, 449, 1, 0, 0, 0, 0, 451, 1, 0, 0, 
	    0, 0, 453, 1, 0, 0, 0, 0, 455, 1, 0, 0, 0, 0, 457, 1, 0, 0, 0, 0, 459, 
	    1, 0, 0, 0, 0, 461, 1, 0, 0, 0, 0, 463, 1, 0, 0, 0, 0, 465, 1, 0, 0, 
	    0, 0, 467, 1, 0, 0, 0, 0, 469, 1, 0, 0, 0, 0, 471, 1, 0, 0, 0, 0, 473, 
	    1, 0, 0, 0, 0, 475, 1, 0, 0, 0, 0, 477, 1, 0, 0, 0, 0, 479, 1, 0, 0, 
	    0, 0, 481, 1, 0, 0, 0, 0, 483, 1, 0, 0, 0, 0, 485, 1, 0, 0, 0, 0, 487, 
	    1, 0, 0, 0, 0, 489, 1, 0, 0, 0, 0, 491, 1, 0, 0, 0, 0, 493, 1, 0, 0, 
	    0, 0, 495, 1, 0, 0, 0, 0, 497, 1, 0, 0, 0, 0, 499, 1, 0, 0, 0, 0, 501, 
	    1, 0, 0, 0, 0, 503, 1, 0, 0, 0, 0, 505, 1, 0, 0, 0, 0, 507, 1, 0, 0, 
	    0, 0, 509, 1, 0, 0, 0, 0, 511, 1, 0, 0, 0, 0, 513, 1, 0, 0, 0, 0, 515, 
	    1, 0, 0, 0, 0, 517, 1, 0, 0, 0, 0, 519, 1, 0, 0, 0, 0, 521, 1, 0, 0, 
	    0, 0, 523, 1, 0, 0, 0, 0, 525, 1, 0, 0, 0, 0, 527, 1, 0, 0, 0, 0, 529, 
	    1, 0, 0, 0, 0, 531, 1, 0, 0, 0, 0, 533, 1, 0, 0, 0, 0, 535, 1, 0, 0, 
	    0, 0, 537, 1, 0, 0, 0, 0, 539, 1, 0, 0, 0, 0, 541, 1, 0, 0, 0, 0, 543, 
	    1, 0, 0, 0, 0, 545, 1, 0, 0, 0, 0, 547, 1, 0, 0, 0, 0, 549, 1, 0, 0, 
	    0, 0, 551, 1, 0, 0, 0, 0, 553, 1, 0, 0, 0, 0, 555, 1, 0, 0, 0, 0, 557, 
	    1, 0, 0, 0, 0, 559, 1, 0, 0, 0, 0, 561, 1, 0, 0, 0, 0, 563, 1, 0, 0, 
	    0, 0, 565, 1, 0, 0, 0, 0, 567, 1, 0, 0, 0, 0, 569, 1, 0, 0, 0, 0, 571, 
	    1, 0, 0, 0, 0, 573, 1, 0, 0, 0, 0, 575, 1, 0, 0, 0, 0, 577, 1, 0, 0, 
	    0, 0, 579, 1, 0, 0, 0, 0, 581, 1, 0, 0, 0, 0, 583, 1, 0, 0, 0, 0, 585, 
	    1, 0, 0, 0, 0, 587, 1, 0, 0, 0, 0, 589, 1, 0, 0, 0, 0, 591, 1, 0, 0, 
	    0, 0, 593, 1, 0, 0, 0, 0, 595, 1, 0, 0, 0, 0, 597, 1, 0, 0, 0, 0, 599, 
	    1, 0, 0, 0, 0, 601, 1, 0, 0, 0, 0, 603, 1, 0, 0, 0, 0, 605, 1, 0, 0, 
	    0, 0, 607, 1, 0, 0, 0, 0, 609, 1, 0, 0, 0, 0, 611, 1, 0, 0, 0, 0, 613, 
	    1, 0, 0, 0, 0, 615, 1, 0, 0, 0, 0, 617, 1, 0, 0, 0, 0, 619, 1, 0, 0, 
	    0, 0, 621, 1, 0, 0, 0, 0, 623, 1, 0, 0, 0, 0, 625, 1, 0, 0, 0, 0, 627, 
	    1, 0, 0, 0, 0, 629, 1, 0, 0, 0, 0, 631, 1, 0, 0, 0, 0, 633, 1, 0, 0, 
	    0, 0, 635, 1, 0, 0, 0, 0, 637, 1, 0, 0, 0, 0, 639, 1, 0, 0, 0, 0, 641, 
	    1, 0, 0, 0, 0, 643, 1, 0, 0, 0, 0, 645, 1, 0, 0, 0, 0, 647, 1, 0, 0, 
	    0, 0, 649, 1, 0, 0, 0, 0, 651, 1, 0, 0, 0, 0, 653, 1, 0, 0, 0, 0, 655, 
	    1, 0, 0, 0, 0, 657, 1, 0, 0, 0, 0, 659, 1, 0, 0, 0, 0, 661, 1, 0, 0, 
	    0, 0, 663, 1, 0, 0, 0, 0, 665, 1, 0, 0, 0, 0, 667, 1, 0, 0, 0, 0, 669, 
	    1, 0, 0, 0, 0, 671, 1, 0, 0, 0, 0, 673, 1, 0, 0, 0, 0, 675, 1, 0, 0, 
	    0, 0, 677, 1, 0, 0, 0, 0, 679, 1, 0, 0, 0, 0, 681, 1, 0, 0, 0, 0, 683, 
	    1, 0, 0, 0, 0, 685, 1, 0, 0, 0, 0, 687, 1, 0, 0, 0, 0, 689, 1, 0, 0, 
	    0, 0, 691, 1, 0, 0, 0, 0, 693, 1, 0, 0, 0, 0, 695, 1, 0, 0, 0, 0, 697, 
	    1, 0, 0, 0, 0, 699, 1, 0, 0, 0, 0, 701, 1, 0, 0, 0, 0, 703, 1, 0, 0, 
	    0, 0, 705, 1, 0, 0, 0, 0, 707, 1, 0, 0, 0, 0, 709, 1, 0, 0, 0, 0, 711, 
	    1, 0, 0, 0, 0, 713, 1, 0, 0, 0, 0, 715, 1, 0, 0, 0, 0, 717, 1, 0, 0, 
	    0, 0, 719, 1, 0, 0, 0, 0, 721, 1, 0, 0, 0, 0, 723, 1, 0, 0, 0, 0, 725, 
	    1, 0, 0, 0, 0, 727, 1, 0, 0, 0, 0, 729, 1, 0, 0, 0, 0, 731, 1, 0, 0, 
	    0, 0, 733, 1, 0, 0, 0, 0, 735, 1, 0, 0, 0, 0, 737, 1, 0, 0, 0, 0, 739, 
	    1, 0, 0, 0, 0, 741, 1, 0, 0, 0, 0, 743, 1, 0, 0, 0, 0, 745, 1, 0, 0, 
	    0, 0, 747, 1, 0, 0, 0, 0, 749, 1, 0, 0, 0, 0, 751, 1, 0, 0, 0, 0, 753, 
	    1, 0, 0, 0, 0, 755, 1, 0, 0, 0, 0, 757, 1, 0, 0, 0, 0, 759, 1, 0, 0, 
	    0, 0, 761, 1, 0, 0, 0, 0, 763, 1, 0, 0, 0, 0, 765, 1, 0, 0, 0, 0, 767, 
	    1, 0, 0, 0, 0, 769, 1, 0, 0, 0, 0, 771, 1, 0, 0, 0, 0, 773, 1, 0, 0, 
	    0, 0, 775, 1, 0, 0, 0, 0, 777, 1, 0, 0, 0, 0, 779, 1, 0, 0, 0, 0, 781, 
	    1, 0, 0, 0, 0, 783, 1, 0, 0, 0, 0, 785, 1, 0, 0, 0, 0, 787, 1, 0, 0, 
	    0, 0, 789, 1, 0, 0, 0, 0, 791, 1, 0, 0, 0, 0, 793, 1, 0, 0, 0, 0, 795, 
	    1, 0, 0, 0, 0, 797, 1, 0, 0, 0, 0, 799, 1, 0, 0, 0, 0, 801, 1, 0, 0, 
	    0, 0, 803, 1, 0, 0, 0, 0, 805, 1, 0, 0, 0, 0, 807, 1, 0, 0, 0, 0, 809, 
	    1, 0, 0, 0, 0, 811, 1, 0, 0, 0, 0, 813, 1, 0, 0, 0, 0, 815, 1, 0, 0, 
	    0, 0, 817, 1, 0, 0, 0, 0, 819, 1, 0, 0, 0, 0, 821, 1, 0, 0, 0, 0, 823, 
	    1, 0, 0, 0, 0, 825, 1, 0, 0, 0, 0, 827, 1, 0, 0, 0, 0, 829, 1, 0, 0, 
	    0, 0, 831, 1, 0, 0, 0, 0, 833, 1, 0, 0, 0, 0, 835, 1, 0, 0, 0, 0, 837, 
	    1, 0, 0, 0, 0, 839, 1, 0, 0, 0, 0, 841, 1, 0, 0, 0, 0, 843, 1, 0, 0, 
	    0, 0, 845, 1, 0, 0, 0, 0, 847, 1, 0, 0, 0, 0, 849, 1, 0, 0, 0, 0, 851, 
	    1, 0, 0, 0, 0, 853, 1, 0, 0, 0, 0, 855, 1, 0, 0, 0, 0, 857, 1, 0, 0, 
	    0, 0, 859, 1, 0, 0, 0, 0, 861, 1, 0, 0, 0, 0, 863, 1, 0, 0, 0, 0, 873, 
	    1, 0, 0, 0, 0, 875, 1, 0, 0, 0, 0, 877, 1, 0, 0, 0, 0, 879, 1, 0, 0, 
	    0, 0, 881, 1, 0, 0, 0, 1, 883, 1, 0, 0, 0, 3, 886, 1, 0, 0, 0, 5, 889, 
	    1, 0, 0, 0, 7, 893, 1, 0, 0, 0, 9, 896, 1, 0, 0, 0, 11, 900, 1, 0, 
	    0, 0, 13, 906, 1, 0, 0, 0, 15, 910, 1, 0, 0, 0, 17, 916, 1, 0, 0, 0, 
	    19, 923, 1, 0, 0, 0, 21, 931, 1, 0, 0, 0, 23, 935, 1, 0, 0, 0, 25, 
	    940, 1, 0, 0, 0, 27, 944, 1, 0, 0, 0, 29, 954, 1, 0, 0, 0, 31, 962, 
	    1, 0, 0, 0, 33, 968, 1, 0, 0, 0, 35, 979, 1, 0, 0, 0, 37, 982, 1, 0, 
	    0, 0, 39, 986, 1, 0, 0, 0, 41, 989, 1, 0, 0, 0, 43, 1003, 1, 0, 0, 
	    0, 45, 1009, 1, 0, 0, 0, 47, 1017, 1, 0, 0, 0, 49, 1024, 1, 0, 0, 0, 
	    51, 1031, 1, 0, 0, 0, 53, 1033, 1, 0, 0, 0, 55, 1041, 1, 0, 0, 0, 57, 
	    1049, 1, 0, 0, 0, 59, 1054, 1, 0, 0, 0, 61, 1061, 1, 0, 0, 0, 63, 1069, 
	    1, 0, 0, 0, 65, 1072, 1, 0, 0, 0, 67, 1077, 1, 0, 0, 0, 69, 1083, 1, 
	    0, 0, 0, 71, 1090, 1, 0, 0, 0, 73, 1098, 1, 0, 0, 0, 75, 1103, 1, 0, 
	    0, 0, 77, 1108, 1, 0, 0, 0, 79, 1116, 1, 0, 0, 0, 81, 1125, 1, 0, 0, 
	    0, 83, 1132, 1, 0, 0, 0, 85, 1137, 1, 0, 0, 0, 87, 1147, 1, 0, 0, 0, 
	    89, 1153, 1, 0, 0, 0, 91, 1159, 1, 0, 0, 0, 93, 1167, 1, 0, 0, 0, 95, 
	    1177, 1, 0, 0, 0, 97, 1185, 1, 0, 0, 0, 99, 1193, 1, 0, 0, 0, 101, 
	    1203, 1, 0, 0, 0, 103, 1214, 1, 0, 0, 0, 105, 1221, 1, 0, 0, 0, 107, 
	    1229, 1, 0, 0, 0, 109, 1231, 1, 0, 0, 0, 111, 1239, 1, 0, 0, 0, 113, 
	    1246, 1, 0, 0, 0, 115, 1254, 1, 0, 0, 0, 117, 1266, 1, 0, 0, 0, 119, 
	    1279, 1, 0, 0, 0, 121, 1287, 1, 0, 0, 0, 123, 1299, 1, 0, 0, 0, 125, 
	    1310, 1, 0, 0, 0, 127, 1319, 1, 0, 0, 0, 129, 1324, 1, 0, 0, 0, 131, 
	    1330, 1, 0, 0, 0, 133, 1337, 1, 0, 0, 0, 135, 1343, 1, 0, 0, 0, 137, 
	    1348, 1, 0, 0, 0, 139, 1356, 1, 0, 0, 0, 141, 1369, 1, 0, 0, 0, 143, 
	    1382, 1, 0, 0, 0, 145, 1400, 1, 0, 0, 0, 147, 1413, 1, 0, 0, 0, 149, 
	    1417, 1, 0, 0, 0, 151, 1422, 1, 0, 0, 0, 153, 1432, 1, 0, 0, 0, 155, 
	    1437, 1, 0, 0, 0, 157, 1442, 1, 0, 0, 0, 159, 1451, 1, 0, 0, 0, 161, 
	    1461, 1, 0, 0, 0, 163, 1469, 1, 0, 0, 0, 165, 1478, 1, 0, 0, 0, 167, 
	    1487, 1, 0, 0, 0, 169, 1497, 1, 0, 0, 0, 171, 1510, 1, 0, 0, 0, 173, 
	    1514, 1, 0, 0, 0, 175, 1522, 1, 0, 0, 0, 177, 1530, 1, 0, 0, 0, 179, 
	    1537, 1, 0, 0, 0, 181, 1545, 1, 0, 0, 0, 183, 1553, 1, 0, 0, 0, 185, 
	    1561, 1, 0, 0, 0, 187, 1568, 1, 0, 0, 0, 189, 1578, 1, 0, 0, 0, 191, 
	    1583, 1, 0, 0, 0, 193, 1592, 1, 0, 0, 0, 195, 1606, 1, 0, 0, 0, 197, 
	    1610, 1, 0, 0, 0, 199, 1622, 1, 0, 0, 0, 201, 1632, 1, 0, 0, 0, 203, 
	    1641, 1, 0, 0, 0, 205, 1652, 1, 0, 0, 0, 207, 1656, 1, 0, 0, 0, 209, 
	    1659, 1, 0, 0, 0, 211, 1666, 1, 0, 0, 0, 213, 1671, 1, 0, 0, 0, 215, 
	    1676, 1, 0, 0, 0, 217, 1680, 1, 0, 0, 0, 219, 1687, 1, 0, 0, 0, 221, 
	    1695, 1, 0, 0, 0, 223, 1705, 1, 0, 0, 0, 225, 1712, 1, 0, 0, 0, 227, 
	    1721, 1, 0, 0, 0, 229, 1729, 1, 0, 0, 0, 231, 1737, 1, 0, 0, 0, 233, 
	    1744, 1, 0, 0, 0, 235, 1752, 1, 0, 0, 0, 237, 1759, 1, 0, 0, 0, 239, 
	    1768, 1, 0, 0, 0, 241, 1777, 1, 0, 0, 0, 243, 1785, 1, 0, 0, 0, 245, 
	    1791, 1, 0, 0, 0, 247, 1797, 1, 0, 0, 0, 249, 1804, 1, 0, 0, 0, 251, 
	    1811, 1, 0, 0, 0, 253, 1822, 1, 0, 0, 0, 255, 1828, 1, 0, 0, 0, 257, 
	    1834, 1, 0, 0, 0, 259, 1844, 1, 0, 0, 0, 261, 1848, 1, 0, 0, 0, 263, 
	    1856, 1, 0, 0, 0, 265, 1863, 1, 0, 0, 0, 267, 1873, 1, 0, 0, 0, 269, 
	    1878, 1, 0, 0, 0, 271, 1888, 1, 0, 0, 0, 273, 1893, 1, 0, 0, 0, 275, 
	    1902, 1, 0, 0, 0, 277, 1912, 1, 0, 0, 0, 279, 1922, 1, 0, 0, 0, 281, 
	    1929, 1, 0, 0, 0, 283, 1935, 1, 0, 0, 0, 285, 1941, 1, 0, 0, 0, 287, 
	    1950, 1, 0, 0, 0, 289, 1957, 1, 0, 0, 0, 291, 1962, 1, 0, 0, 0, 293, 
	    1968, 1, 0, 0, 0, 295, 1979, 1, 0, 0, 0, 297, 1988, 1, 0, 0, 0, 299, 
	    1991, 1, 0, 0, 0, 301, 1998, 1, 0, 0, 0, 303, 2008, 1, 0, 0, 0, 305, 
	    2015, 1, 0, 0, 0, 307, 2018, 1, 0, 0, 0, 309, 2026, 1, 0, 0, 0, 311, 
	    2032, 1, 0, 0, 0, 313, 2040, 1, 0, 0, 0, 315, 2046, 1, 0, 0, 0, 317, 
	    2053, 1, 0, 0, 0, 319, 2059, 1, 0, 0, 0, 321, 2071, 1, 0, 0, 0, 323, 
	    2078, 1, 0, 0, 0, 325, 2088, 1, 0, 0, 0, 327, 2097, 1, 0, 0, 0, 329, 
	    2101, 1, 0, 0, 0, 331, 2109, 1, 0, 0, 0, 333, 2114, 1, 0, 0, 0, 335, 
	    2122, 1, 0, 0, 0, 337, 2125, 1, 0, 0, 0, 339, 2131, 1, 0, 0, 0, 341, 
	    2137, 1, 0, 0, 0, 343, 2142, 1, 0, 0, 0, 345, 2146, 1, 0, 0, 0, 347, 
	    2151, 1, 0, 0, 0, 349, 2160, 1, 0, 0, 0, 351, 2165, 1, 0, 0, 0, 353, 
	    2173, 1, 0, 0, 0, 355, 2178, 1, 0, 0, 0, 357, 2186, 1, 0, 0, 0, 359, 
	    2191, 1, 0, 0, 0, 361, 2196, 1, 0, 0, 0, 363, 2202, 1, 0, 0, 0, 365, 
	    2208, 1, 0, 0, 0, 367, 2213, 1, 0, 0, 0, 369, 2221, 1, 0, 0, 0, 371, 
	    2226, 1, 0, 0, 0, 373, 2231, 1, 0, 0, 0, 375, 2237, 1, 0, 0, 0, 377, 
	    2246, 1, 0, 0, 0, 379, 2251, 1, 0, 0, 0, 381, 2257, 1, 0, 0, 0, 383, 
	    2265, 1, 0, 0, 0, 385, 2270, 1, 0, 0, 0, 387, 2276, 1, 0, 0, 0, 389, 
	    2280, 1, 0, 0, 0, 391, 2297, 1, 0, 0, 0, 393, 2305, 1, 0, 0, 0, 395, 
	    2318, 1, 0, 0, 0, 397, 2324, 1, 0, 0, 0, 399, 2336, 1, 0, 0, 0, 401, 
	    2349, 1, 0, 0, 0, 403, 2361, 1, 0, 0, 0, 405, 2374, 1, 0, 0, 0, 407, 
	    2380, 1, 0, 0, 0, 409, 2387, 1, 0, 0, 0, 411, 2395, 1, 0, 0, 0, 413, 
	    2400, 1, 0, 0, 0, 415, 2409, 1, 0, 0, 0, 417, 2415, 1, 0, 0, 0, 419, 
	    2422, 1, 0, 0, 0, 421, 2427, 1, 0, 0, 0, 423, 2432, 1, 0, 0, 0, 425, 
	    2442, 1, 0, 0, 0, 427, 2453, 1, 0, 0, 0, 429, 2466, 1, 0, 0, 0, 431, 
	    2477, 1, 0, 0, 0, 433, 2489, 1, 0, 0, 0, 435, 2497, 1, 0, 0, 0, 437, 
	    2500, 1, 0, 0, 0, 439, 2505, 1, 0, 0, 0, 441, 2509, 1, 0, 0, 0, 443, 
	    2514, 1, 0, 0, 0, 445, 2520, 1, 0, 0, 0, 447, 2528, 1, 0, 0, 0, 449, 
	    2531, 1, 0, 0, 0, 451, 2538, 1, 0, 0, 0, 453, 2541, 1, 0, 0, 0, 455, 
	    2546, 1, 0, 0, 0, 457, 2555, 1, 0, 0, 0, 459, 2562, 1, 0, 0, 0, 461, 
	    2570, 1, 0, 0, 0, 463, 2573, 1, 0, 0, 0, 465, 2579, 1, 0, 0, 0, 467, 
	    2583, 1, 0, 0, 0, 469, 2589, 1, 0, 0, 0, 471, 2602, 1, 0, 0, 0, 473, 
	    2607, 1, 0, 0, 0, 475, 2616, 1, 0, 0, 0, 477, 2624, 1, 0, 0, 0, 479, 
	    2634, 1, 0, 0, 0, 481, 2644, 1, 0, 0, 0, 483, 2656, 1, 0, 0, 0, 485, 
	    2667, 1, 0, 0, 0, 487, 2675, 1, 0, 0, 0, 489, 2691, 1, 0, 0, 0, 491, 
	    2707, 1, 0, 0, 0, 493, 2713, 1, 0, 0, 0, 495, 2721, 1, 0, 0, 0, 497, 
	    2730, 1, 0, 0, 0, 499, 2740, 1, 0, 0, 0, 501, 2748, 1, 0, 0, 0, 503, 
	    2759, 1, 0, 0, 0, 505, 2770, 1, 0, 0, 0, 507, 2776, 1, 0, 0, 0, 509, 
	    2782, 1, 0, 0, 0, 511, 2790, 1, 0, 0, 0, 513, 2798, 1, 0, 0, 0, 515, 
	    2804, 1, 0, 0, 0, 517, 2810, 1, 0, 0, 0, 519, 2816, 1, 0, 0, 0, 521, 
	    2821, 1, 0, 0, 0, 523, 2834, 1, 0, 0, 0, 525, 2847, 1, 0, 0, 0, 527, 
	    2855, 1, 0, 0, 0, 529, 2865, 1, 0, 0, 0, 531, 2872, 1, 0, 0, 0, 533, 
	    2879, 1, 0, 0, 0, 535, 2889, 1, 0, 0, 0, 537, 2900, 1, 0, 0, 0, 539, 
	    2908, 1, 0, 0, 0, 541, 2915, 1, 0, 0, 0, 543, 2922, 1, 0, 0, 0, 545, 
	    2933, 1, 0, 0, 0, 547, 2941, 1, 0, 0, 0, 549, 2947, 1, 0, 0, 0, 551, 
	    2955, 1, 0, 0, 0, 553, 2964, 1, 0, 0, 0, 555, 2971, 1, 0, 0, 0, 557, 
	    2979, 1, 0, 0, 0, 559, 2986, 1, 0, 0, 0, 561, 2992, 1, 0, 0, 0, 563, 
	    2998, 1, 0, 0, 0, 565, 3003, 1, 0, 0, 0, 567, 3009, 1, 0, 0, 0, 569, 
	    3018, 1, 0, 0, 0, 571, 3025, 1, 0, 0, 0, 573, 3029, 1, 0, 0, 0, 575, 
	    3034, 1, 0, 0, 0, 577, 3041, 1, 0, 0, 0, 579, 3049, 1, 0, 0, 0, 581, 
	    3056, 1, 0, 0, 0, 583, 3064, 1, 0, 0, 0, 585, 3073, 1, 0, 0, 0, 587, 
	    3080, 1, 0, 0, 0, 589, 3085, 1, 0, 0, 0, 591, 3095, 1, 0, 0, 0, 593, 
	    3101, 1, 0, 0, 0, 595, 3117, 1, 0, 0, 0, 597, 3130, 1, 0, 0, 0, 599, 
	    3134, 1, 0, 0, 0, 601, 3139, 1, 0, 0, 0, 603, 3145, 1, 0, 0, 0, 605, 
	    3150, 1, 0, 0, 0, 607, 3157, 1, 0, 0, 0, 609, 3164, 1, 0, 0, 0, 611, 
	    3173, 1, 0, 0, 0, 613, 3178, 1, 0, 0, 0, 615, 3183, 1, 0, 0, 0, 617, 
	    3190, 1, 0, 0, 0, 619, 3197, 1, 0, 0, 0, 621, 3206, 1, 0, 0, 0, 623, 
	    3210, 1, 0, 0, 0, 625, 3216, 1, 0, 0, 0, 627, 3227, 1, 0, 0, 0, 629, 
	    3234, 1, 0, 0, 0, 631, 3243, 1, 0, 0, 0, 633, 3250, 1, 0, 0, 0, 635, 
	    3260, 1, 0, 0, 0, 637, 3271, 1, 0, 0, 0, 639, 3278, 1, 0, 0, 0, 641, 
	    3285, 1, 0, 0, 0, 643, 3295, 1, 0, 0, 0, 645, 3300, 1, 0, 0, 0, 647, 
	    3312, 1, 0, 0, 0, 649, 3327, 1, 0, 0, 0, 651, 3333, 1, 0, 0, 0, 653, 
	    3340, 1, 0, 0, 0, 655, 3352, 1, 0, 0, 0, 657, 3359, 1, 0, 0, 0, 659, 
	    3373, 1, 0, 0, 0, 661, 3378, 1, 0, 0, 0, 663, 3388, 1, 0, 0, 0, 665, 
	    3399, 1, 0, 0, 0, 667, 3406, 1, 0, 0, 0, 669, 3411, 1, 0, 0, 0, 671, 
	    3416, 1, 0, 0, 0, 673, 3425, 1, 0, 0, 0, 675, 3435, 1, 0, 0, 0, 677, 
	    3448, 1, 0, 0, 0, 679, 3462, 1, 0, 0, 0, 681, 3476, 1, 0, 0, 0, 683, 
	    3490, 1, 0, 0, 0, 685, 3498, 1, 0, 0, 0, 687, 3501, 1, 0, 0, 0, 689, 
	    3507, 1, 0, 0, 0, 691, 3516, 1, 0, 0, 0, 693, 3528, 1, 0, 0, 0, 695, 
	    3541, 1, 0, 0, 0, 697, 3551, 1, 0, 0, 0, 699, 3556, 1, 0, 0, 0, 701, 
	    3561, 1, 0, 0, 0, 703, 3570, 1, 0, 0, 0, 705, 3579, 1, 0, 0, 0, 707, 
	    3584, 1, 0, 0, 0, 709, 3594, 1, 0, 0, 0, 711, 3604, 1, 0, 0, 0, 713, 
	    3612, 1, 0, 0, 0, 715, 3618, 1, 0, 0, 0, 717, 3625, 1, 0, 0, 0, 719, 
	    3633, 1, 0, 0, 0, 721, 3640, 1, 0, 0, 0, 723, 3648, 1, 0, 0, 0, 725, 
	    3654, 1, 0, 0, 0, 727, 3661, 1, 0, 0, 0, 729, 3665, 1, 0, 0, 0, 731, 
	    3670, 1, 0, 0, 0, 733, 3676, 1, 0, 0, 0, 735, 3683, 1, 0, 0, 0, 737, 
	    3687, 1, 0, 0, 0, 739, 3695, 1, 0, 0, 0, 741, 3703, 1, 0, 0, 0, 743, 
	    3711, 1, 0, 0, 0, 745, 3716, 1, 0, 0, 0, 747, 3722, 1, 0, 0, 0, 749, 
	    3727, 1, 0, 0, 0, 751, 3732, 1, 0, 0, 0, 753, 3738, 1, 0, 0, 0, 755, 
	    3743, 1, 0, 0, 0, 757, 3749, 1, 0, 0, 0, 759, 3755, 1, 0, 0, 0, 761, 
	    3762, 1, 0, 0, 0, 763, 3767, 1, 0, 0, 0, 765, 3774, 1, 0, 0, 0, 767, 
	    3779, 1, 0, 0, 0, 769, 3785, 1, 0, 0, 0, 771, 3790, 1, 0, 0, 0, 773, 
	    3792, 1, 0, 0, 0, 775, 3794, 1, 0, 0, 0, 777, 3796, 1, 0, 0, 0, 779, 
	    3798, 1, 0, 0, 0, 781, 3800, 1, 0, 0, 0, 783, 3802, 1, 0, 0, 0, 785, 
	    3805, 1, 0, 0, 0, 787, 3807, 1, 0, 0, 0, 789, 3811, 1, 0, 0, 0, 791, 
	    3815, 1, 0, 0, 0, 793, 3822, 1, 0, 0, 0, 795, 3824, 1, 0, 0, 0, 797, 
	    3826, 1, 0, 0, 0, 799, 3829, 1, 0, 0, 0, 801, 3831, 1, 0, 0, 0, 803, 
	    3834, 1, 0, 0, 0, 805, 3836, 1, 0, 0, 0, 807, 3838, 1, 0, 0, 0, 809, 
	    3840, 1, 0, 0, 0, 811, 3842, 1, 0, 0, 0, 813, 3844, 1, 0, 0, 0, 815, 
	    3847, 1, 0, 0, 0, 817, 3849, 1, 0, 0, 0, 819, 3851, 1, 0, 0, 0, 821, 
	    3853, 1, 0, 0, 0, 823, 3855, 1, 0, 0, 0, 825, 3857, 1, 0, 0, 0, 827, 
	    3859, 1, 0, 0, 0, 829, 3861, 1, 0, 0, 0, 831, 3864, 1, 0, 0, 0, 833, 
	    3866, 1, 0, 0, 0, 835, 3898, 1, 0, 0, 0, 837, 3900, 1, 0, 0, 0, 839, 
	    3910, 1, 0, 0, 0, 841, 3925, 1, 0, 0, 0, 843, 3930, 1, 0, 0, 0, 845, 
	    3937, 1, 0, 0, 0, 847, 3944, 1, 0, 0, 0, 849, 3961, 1, 0, 0, 0, 851, 
	    3963, 1, 0, 0, 0, 853, 3983, 1, 0, 0, 0, 855, 4002, 1, 0, 0, 0, 857, 
	    4024, 1, 0, 0, 0, 859, 4031, 1, 0, 0, 0, 861, 4046, 1, 0, 0, 0, 863, 
	    4057, 1, 0, 0, 0, 865, 4060, 1, 0, 0, 0, 867, 4069, 1, 0, 0, 0, 869, 
	    4071, 1, 0, 0, 0, 871, 4091, 1, 0, 0, 0, 873, 4093, 1, 0, 0, 0, 875, 
	    4110, 1, 0, 0, 0, 877, 4126, 1, 0, 0, 0, 879, 4135, 1, 0, 0, 0, 881, 
	    4137, 1, 0, 0, 0, 883, 884, 5, 61, 0, 0, 884, 885, 5, 62, 0, 0, 885, 
	    2, 1, 0, 0, 0, 886, 887, 5, 45, 0, 0, 887, 888, 5, 62, 0, 0, 888, 4, 
	    1, 0, 0, 0, 889, 890, 5, 63, 0, 0, 890, 891, 5, 58, 0, 0, 891, 892, 
	    5, 58, 0, 0, 892, 6, 1, 0, 0, 0, 893, 894, 5, 58, 0, 0, 894, 895, 5, 
	    58, 0, 0, 895, 8, 1, 0, 0, 0, 896, 897, 5, 65, 0, 0, 897, 898, 5, 68, 
	    0, 0, 898, 899, 5, 68, 0, 0, 899, 10, 1, 0, 0, 0, 900, 901, 5, 65, 
	    0, 0, 901, 902, 5, 70, 0, 0, 902, 903, 5, 84, 0, 0, 903, 904, 5, 69, 
	    0, 0, 904, 905, 5, 82, 0, 0, 905, 12, 1, 0, 0, 0, 906, 907, 5, 65, 
	    0, 0, 907, 908, 5, 76, 0, 0, 908, 909, 5, 76, 0, 0, 909, 14, 1, 0, 
	    0, 0, 910, 911, 5, 65, 0, 0, 911, 912, 5, 76, 0, 0, 912, 913, 5, 84, 
	    0, 0, 913, 914, 5, 69, 0, 0, 914, 915, 5, 82, 0, 0, 915, 16, 1, 0, 
	    0, 0, 916, 917, 5, 65, 0, 0, 917, 918, 5, 76, 0, 0, 918, 919, 5, 87, 
	    0, 0, 919, 920, 5, 65, 0, 0, 920, 921, 5, 89, 0, 0, 921, 922, 5, 83, 
	    0, 0, 922, 18, 1, 0, 0, 0, 923, 924, 5, 65, 0, 0, 924, 925, 5, 78, 
	    0, 0, 925, 926, 5, 65, 0, 0, 926, 927, 5, 76, 0, 0, 927, 928, 5, 89, 
	    0, 0, 928, 929, 5, 90, 0, 0, 929, 930, 5, 69, 0, 0, 930, 20, 1, 0, 
	    0, 0, 931, 932, 5, 65, 0, 0, 932, 933, 5, 78, 0, 0, 933, 934, 5, 68, 
	    0, 0, 934, 22, 1, 0, 0, 0, 935, 936, 5, 65, 0, 0, 936, 937, 5, 78, 
	    0, 0, 937, 938, 5, 84, 0, 0, 938, 939, 5, 73, 0, 0, 939, 24, 1, 0, 
	    0, 0, 940, 941, 5, 65, 0, 0, 941, 942, 5, 78, 0, 0, 942, 943, 5, 89, 
	    0, 0, 943, 26, 1, 0, 0, 0, 944, 945, 5, 65, 0, 0, 945, 946, 5, 78, 
	    0, 0, 946, 947, 5, 89, 0, 0, 947, 948, 5, 95, 0, 0, 948, 949, 5, 86, 
	    0, 0, 949, 950, 5, 65, 0, 0, 950, 951, 5, 76, 0, 0, 951, 952, 5, 85, 
	    0, 0, 952, 953, 5, 69, 0, 0, 953, 28, 1, 0, 0, 0, 954, 955, 5, 65, 
	    0, 0, 955, 956, 5, 82, 0, 0, 956, 957, 5, 67, 0, 0, 957, 958, 5, 72, 
	    0, 0, 958, 959, 5, 73, 0, 0, 959, 960, 5, 86, 0, 0, 960, 961, 5, 69, 
	    0, 0, 961, 30, 1, 0, 0, 0, 962, 963, 5, 65, 0, 0, 963, 964, 5, 82, 
	    0, 0, 964, 965, 5, 82, 0, 0, 965, 966, 5, 65, 0, 0, 966, 967, 5, 89, 
	    0, 0, 967, 32, 1, 0, 0, 0, 968, 969, 5, 65, 0, 0, 969, 970, 5, 82, 
	    0, 0, 970, 971, 5, 82, 0, 0, 971, 972, 5, 65, 0, 0, 972, 973, 5, 89, 
	    0, 0, 973, 974, 5, 83, 0, 0, 974, 975, 5, 95, 0, 0, 975, 976, 5, 90, 
	    0, 0, 976, 977, 5, 73, 0, 0, 977, 978, 5, 80, 0, 0, 978, 34, 1, 0, 
	    0, 0, 979, 980, 5, 65, 0, 0, 980, 981, 5, 83, 0, 0, 981, 36, 1, 0, 
	    0, 0, 982, 983, 5, 65, 0, 0, 983, 984, 5, 83, 0, 0, 984, 985, 5, 67, 
	    0, 0, 985, 38, 1, 0, 0, 0, 986, 987, 5, 65, 0, 0, 987, 988, 5, 84, 
	    0, 0, 988, 40, 1, 0, 0, 0, 989, 990, 5, 65, 0, 0, 990, 991, 5, 85, 
	    0, 0, 991, 992, 5, 84, 0, 0, 992, 993, 5, 72, 0, 0, 993, 994, 5, 79, 
	    0, 0, 994, 995, 5, 82, 0, 0, 995, 996, 5, 73, 0, 0, 996, 997, 5, 90, 
	    0, 0, 997, 998, 5, 65, 0, 0, 998, 999, 5, 84, 0, 0, 999, 1000, 5, 73, 
	    0, 0, 1000, 1001, 5, 79, 0, 0, 1001, 1002, 5, 78, 0, 0, 1002, 42, 1, 
	    0, 0, 0, 1003, 1004, 5, 66, 0, 0, 1004, 1005, 5, 69, 0, 0, 1005, 1006, 
	    5, 71, 0, 0, 1006, 1007, 5, 73, 0, 0, 1007, 1008, 5, 78, 0, 0, 1008, 
	    44, 1, 0, 0, 0, 1009, 1010, 5, 66, 0, 0, 1010, 1011, 5, 69, 0, 0, 1011, 
	    1012, 5, 84, 0, 0, 1012, 1013, 5, 87, 0, 0, 1013, 1014, 5, 69, 0, 0, 
	    1014, 1015, 5, 69, 0, 0, 1015, 1016, 5, 78, 0, 0, 1016, 46, 1, 0, 0, 
	    0, 1017, 1018, 5, 66, 0, 0, 1018, 1019, 5, 73, 0, 0, 1019, 1020, 5, 
	    71, 0, 0, 1020, 1021, 5, 73, 0, 0, 1021, 1022, 5, 78, 0, 0, 1022, 1023, 
	    5, 84, 0, 0, 1023, 48, 1, 0, 0, 0, 1024, 1025, 5, 66, 0, 0, 1025, 1026, 
	    5, 73, 0, 0, 1026, 1027, 5, 78, 0, 0, 1027, 1028, 5, 65, 0, 0, 1028, 
	    1029, 5, 82, 0, 0, 1029, 1030, 5, 89, 0, 0, 1030, 50, 1, 0, 0, 0, 1031, 
	    1032, 5, 88, 0, 0, 1032, 52, 1, 0, 0, 0, 1033, 1034, 5, 66, 0, 0, 1034, 
	    1035, 5, 73, 0, 0, 1035, 1036, 5, 78, 0, 0, 1036, 1037, 5, 68, 0, 0, 
	    1037, 1038, 5, 73, 0, 0, 1038, 1039, 5, 78, 0, 0, 1039, 1040, 5, 71, 
	    0, 0, 1040, 54, 1, 0, 0, 0, 1041, 1042, 5, 66, 0, 0, 1042, 1043, 5, 
	    79, 0, 0, 1043, 1044, 5, 79, 0, 0, 1044, 1045, 5, 76, 0, 0, 1045, 1046, 
	    5, 69, 0, 0, 1046, 1047, 5, 65, 0, 0, 1047, 1048, 5, 78, 0, 0, 1048, 
	    56, 1, 0, 0, 0, 1049, 1050, 5, 66, 0, 0, 1050, 1051, 5, 79, 0, 0, 1051, 
	    1052, 5, 84, 0, 0, 1052, 1053, 5, 72, 0, 0, 1053, 58, 1, 0, 0, 0, 1054, 
	    1055, 5, 66, 0, 0, 1055, 1056, 5, 85, 0, 0, 1056, 1057, 5, 67, 0, 0, 
	    1057, 1058, 5, 75, 0, 0, 1058, 1059, 5, 69, 0, 0, 1059, 1060, 5, 84, 
	    0, 0, 1060, 60, 1, 0, 0, 0, 1061, 1062, 5, 66, 0, 0, 1062, 1063, 5, 
	    85, 0, 0, 1063, 1064, 5, 67, 0, 0, 1064, 1065, 5, 75, 0, 0, 1065, 1066, 
	    5, 69, 0, 0, 1066, 1067, 5, 84, 0, 0, 1067, 1068, 5, 83, 0, 0, 1068, 
	    62, 1, 0, 0, 0, 1069, 1070, 5, 66, 0, 0, 1070, 1071, 5, 89, 0, 0, 1071, 
	    64, 1, 0, 0, 0, 1072, 1073, 5, 66, 0, 0, 1073, 1074, 5, 89, 0, 0, 1074, 
	    1075, 5, 84, 0, 0, 1075, 1076, 5, 69, 0, 0, 1076, 66, 1, 0, 0, 0, 1077, 
	    1078, 5, 67, 0, 0, 1078, 1079, 5, 65, 0, 0, 1079, 1080, 5, 67, 0, 0, 
	    1080, 1081, 5, 72, 0, 0, 1081, 1082, 5, 69, 0, 0, 1082, 68, 1, 0, 0, 
	    0, 1083, 1084, 5, 67, 0, 0, 1084, 1085, 5, 65, 0, 0, 1085, 1086, 5, 
	    76, 0, 0, 1086, 1087, 5, 76, 0, 0, 1087, 1088, 5, 69, 0, 0, 1088, 1089, 
	    5, 68, 0, 0, 1089, 70, 1, 0, 0, 0, 1090, 1091, 5, 67, 0, 0, 1091, 1092, 
	    5, 65, 0, 0, 1092, 1093, 5, 83, 0, 0, 1093, 1094, 5, 67, 0, 0, 1094, 
	    1095, 5, 65, 0, 0, 1095, 1096, 5, 68, 0, 0, 1096, 1097, 5, 69, 0, 0, 
	    1097, 72, 1, 0, 0, 0, 1098, 1099, 5, 67, 0, 0, 1099, 1100, 5, 65, 0, 
	    0, 1100, 1101, 5, 83, 0, 0, 1101, 1102, 5, 69, 0, 0, 1102, 74, 1, 0, 
	    0, 0, 1103, 1104, 5, 67, 0, 0, 1104, 1105, 5, 65, 0, 0, 1105, 1106, 
	    5, 83, 0, 0, 1106, 1107, 5, 84, 0, 0, 1107, 76, 1, 0, 0, 0, 1108, 1109, 
	    5, 67, 0, 0, 1109, 1110, 5, 65, 0, 0, 1110, 1111, 5, 84, 0, 0, 1111, 
	    1112, 5, 65, 0, 0, 1112, 1113, 5, 76, 0, 0, 1113, 1114, 5, 79, 0, 0, 
	    1114, 1115, 5, 71, 0, 0, 1115, 78, 1, 0, 0, 0, 1116, 1117, 5, 67, 0, 
	    0, 1117, 1118, 5, 65, 0, 0, 1118, 1119, 5, 84, 0, 0, 1119, 1120, 5, 
	    65, 0, 0, 1120, 1121, 5, 76, 0, 0, 1121, 1122, 5, 79, 0, 0, 1122, 1123, 
	    5, 71, 0, 0, 1123, 1124, 5, 83, 0, 0, 1124, 80, 1, 0, 0, 0, 1125, 1126, 
	    5, 67, 0, 0, 1126, 1127, 5, 72, 0, 0, 1127, 1128, 5, 65, 0, 0, 1128, 
	    1129, 5, 78, 0, 0, 1129, 1130, 5, 71, 0, 0, 1130, 1131, 5, 69, 0, 0, 
	    1131, 82, 1, 0, 0, 0, 1132, 1133, 5, 67, 0, 0, 1133, 1134, 5, 72, 0, 
	    0, 1134, 1135, 5, 65, 0, 0, 1135, 1136, 5, 82, 0, 0, 1136, 84, 1, 0, 
	    0, 0, 1137, 1138, 5, 67, 0, 0, 1138, 1139, 5, 72, 0, 0, 1139, 1140, 
	    5, 65, 0, 0, 1140, 1141, 5, 82, 0, 0, 1141, 1142, 5, 65, 0, 0, 1142, 
	    1143, 5, 67, 0, 0, 1143, 1144, 5, 84, 0, 0, 1144, 1145, 5, 69, 0, 0, 
	    1145, 1146, 5, 82, 0, 0, 1146, 86, 1, 0, 0, 0, 1147, 1148, 5, 67, 0, 
	    0, 1148, 1149, 5, 72, 0, 0, 1149, 1150, 5, 69, 0, 0, 1150, 1151, 5, 
	    67, 0, 0, 1151, 1152, 5, 75, 0, 0, 1152, 88, 1, 0, 0, 0, 1153, 1154, 
	    5, 67, 0, 0, 1154, 1155, 5, 76, 0, 0, 1155, 1156, 5, 69, 0, 0, 1156, 
	    1157, 5, 65, 0, 0, 1157, 1158, 5, 82, 0, 0, 1158, 90, 1, 0, 0, 0, 1159, 
	    1160, 5, 67, 0, 0, 1160, 1161, 5, 76, 0, 0, 1161, 1162, 5, 85, 0, 0, 
	    1162, 1163, 5, 83, 0, 0, 1163, 1164, 5, 84, 0, 0, 1164, 1165, 5, 69, 
	    0, 0, 1165, 1166, 5, 82, 0, 0, 1166, 92, 1, 0, 0, 0, 1167, 1168, 5, 
	    67, 0, 0, 1168, 1169, 5, 76, 0, 0, 1169, 1170, 5, 85, 0, 0, 1170, 1171, 
	    5, 83, 0, 0, 1171, 1172, 5, 84, 0, 0, 1172, 1173, 5, 69, 0, 0, 1173, 
	    1174, 5, 82, 0, 0, 1174, 1175, 5, 69, 0, 0, 1175, 1176, 5, 68, 0, 0, 
	    1176, 94, 1, 0, 0, 0, 1177, 1178, 5, 67, 0, 0, 1178, 1179, 5, 79, 0, 
	    0, 1179, 1180, 5, 68, 0, 0, 1180, 1181, 5, 69, 0, 0, 1181, 1182, 5, 
	    71, 0, 0, 1182, 1183, 5, 69, 0, 0, 1183, 1184, 5, 78, 0, 0, 1184, 96, 
	    1, 0, 0, 0, 1185, 1186, 5, 67, 0, 0, 1186, 1187, 5, 79, 0, 0, 1187, 
	    1188, 5, 76, 0, 0, 1188, 1189, 5, 76, 0, 0, 1189, 1190, 5, 65, 0, 0, 
	    1190, 1191, 5, 84, 0, 0, 1191, 1192, 5, 69, 0, 0, 1192, 98, 1, 0, 0, 
	    0, 1193, 1194, 5, 67, 0, 0, 1194, 1195, 5, 79, 0, 0, 1195, 1196, 5, 
	    76, 0, 0, 1196, 1197, 5, 76, 0, 0, 1197, 1198, 5, 65, 0, 0, 1198, 1199, 
	    5, 84, 0, 0, 1199, 1200, 5, 73, 0, 0, 1200, 1201, 5, 79, 0, 0, 1201, 
	    1202, 5, 78, 0, 0, 1202, 100, 1, 0, 0, 0, 1203, 1204, 5, 67, 0, 0, 
	    1204, 1205, 5, 79, 0, 0, 1205, 1206, 5, 76, 0, 0, 1206, 1207, 5, 76, 
	    0, 0, 1207, 1208, 5, 69, 0, 0, 1208, 1209, 5, 67, 0, 0, 1209, 1210, 
	    5, 84, 0, 0, 1210, 1211, 5, 73, 0, 0, 1211, 1212, 5, 79, 0, 0, 1212, 
	    1213, 5, 78, 0, 0, 1213, 102, 1, 0, 0, 0, 1214, 1215, 5, 67, 0, 0, 
	    1215, 1216, 5, 79, 0, 0, 1216, 1217, 5, 76, 0, 0, 1217, 1218, 5, 85, 
	    0, 0, 1218, 1219, 5, 77, 0, 0, 1219, 1220, 5, 78, 0, 0, 1220, 104, 
	    1, 0, 0, 0, 1221, 1222, 5, 67, 0, 0, 1222, 1223, 5, 79, 0, 0, 1223, 
	    1224, 5, 76, 0, 0, 1224, 1225, 5, 85, 0, 0, 1225, 1226, 5, 77, 0, 0, 
	    1226, 1227, 5, 78, 0, 0, 1227, 1228, 5, 83, 0, 0, 1228, 106, 1, 0, 
	    0, 0, 1229, 1230, 5, 44, 0, 0, 1230, 108, 1, 0, 0, 0, 1231, 1232, 5, 
	    67, 0, 0, 1232, 1233, 5, 79, 0, 0, 1233, 1234, 5, 77, 0, 0, 1234, 1235, 
	    5, 77, 0, 0, 1235, 1236, 5, 69, 0, 0, 1236, 1237, 5, 78, 0, 0, 1237, 
	    1238, 5, 84, 0, 0, 1238, 110, 1, 0, 0, 0, 1239, 1240, 5, 67, 0, 0, 
	    1240, 1241, 5, 79, 0, 0, 1241, 1242, 5, 77, 0, 0, 1242, 1243, 5, 77, 
	    0, 0, 1243, 1244, 5, 73, 0, 0, 1244, 1245, 5, 84, 0, 0, 1245, 112, 
	    1, 0, 0, 0, 1246, 1247, 5, 67, 0, 0, 1247, 1248, 5, 79, 0, 0, 1248, 
	    1249, 5, 77, 0, 0, 1249, 1250, 5, 80, 0, 0, 1250, 1251, 5, 65, 0, 0, 
	    1251, 1252, 5, 67, 0, 0, 1252, 1253, 5, 84, 0, 0, 1253, 114, 1, 0, 
	    0, 0, 1254, 1255, 5, 67, 0, 0, 1255, 1256, 5, 79, 0, 0, 1256, 1257, 
	    5, 77, 0, 0, 1257, 1258, 5, 80, 0, 0, 1258, 1259, 5, 65, 0, 0, 1259, 
	    1260, 5, 67, 0, 0, 1260, 1261, 5, 84, 0, 0, 1261, 1262, 5, 73, 0, 0, 
	    1262, 1263, 5, 79, 0, 0, 1263, 1264, 5, 78, 0, 0, 1264, 1265, 5, 83, 
	    0, 0, 1265, 116, 1, 0, 0, 0, 1266, 1267, 5, 67, 0, 0, 1267, 1268, 5, 
	    79, 0, 0, 1268, 1269, 5, 77, 0, 0, 1269, 1270, 5, 80, 0, 0, 1270, 1271, 
	    5, 69, 0, 0, 1271, 1272, 5, 78, 0, 0, 1272, 1273, 5, 83, 0, 0, 1273, 
	    1274, 5, 65, 0, 0, 1274, 1275, 5, 84, 0, 0, 1275, 1276, 5, 73, 0, 0, 
	    1276, 1277, 5, 79, 0, 0, 1277, 1278, 5, 78, 0, 0, 1278, 118, 1, 0, 
	    0, 0, 1279, 1280, 5, 67, 0, 0, 1280, 1281, 5, 79, 0, 0, 1281, 1282, 
	    5, 77, 0, 0, 1282, 1283, 5, 80, 0, 0, 1283, 1284, 5, 85, 0, 0, 1284, 
	    1285, 5, 84, 0, 0, 1285, 1286, 5, 69, 0, 0, 1286, 120, 1, 0, 0, 0, 
	    1287, 1288, 5, 67, 0, 0, 1288, 1289, 5, 79, 0, 0, 1289, 1290, 5, 78, 
	    0, 0, 1290, 1291, 5, 67, 0, 0, 1291, 1292, 5, 65, 0, 0, 1292, 1293, 
	    5, 84, 0, 0, 1293, 1294, 5, 69, 0, 0, 1294, 1295, 5, 78, 0, 0, 1295, 
	    1296, 5, 65, 0, 0, 1296, 1297, 5, 84, 0, 0, 1297, 1298, 5, 69, 0, 0, 
	    1298, 122, 1, 0, 0, 0, 1299, 1300, 5, 67, 0, 0, 1300, 1301, 5, 79, 
	    0, 0, 1301, 1302, 5, 78, 0, 0, 1302, 1303, 5, 83, 0, 0, 1303, 1304, 
	    5, 84, 0, 0, 1304, 1305, 5, 82, 0, 0, 1305, 1306, 5, 65, 0, 0, 1306, 
	    1307, 5, 73, 0, 0, 1307, 1308, 5, 78, 0, 0, 1308, 1309, 5, 84, 0, 0, 
	    1309, 124, 1, 0, 0, 0, 1310, 1311, 5, 67, 0, 0, 1311, 1312, 5, 79, 
	    0, 0, 1312, 1313, 5, 78, 0, 0, 1313, 1314, 5, 84, 0, 0, 1314, 1315, 
	    5, 65, 0, 0, 1315, 1316, 5, 73, 0, 0, 1316, 1317, 5, 78, 0, 0, 1317, 
	    1318, 5, 83, 0, 0, 1318, 126, 1, 0, 0, 0, 1319, 1320, 5, 67, 0, 0, 
	    1320, 1321, 5, 79, 0, 0, 1321, 1322, 5, 83, 0, 0, 1322, 1323, 5, 84, 
	    0, 0, 1323, 128, 1, 0, 0, 0, 1324, 1325, 5, 67, 0, 0, 1325, 1326, 5, 
	    79, 0, 0, 1326, 1327, 5, 85, 0, 0, 1327, 1328, 5, 78, 0, 0, 1328, 1329, 
	    5, 84, 0, 0, 1329, 130, 1, 0, 0, 0, 1330, 1331, 5, 67, 0, 0, 1331, 
	    1332, 5, 82, 0, 0, 1332, 1333, 5, 69, 0, 0, 1333, 1334, 5, 65, 0, 0, 
	    1334, 1335, 5, 84, 0, 0, 1335, 1336, 5, 69, 0, 0, 1336, 132, 1, 0, 
	    0, 0, 1337, 1338, 5, 67, 0, 0, 1338, 1339, 5, 82, 0, 0, 1339, 1340, 
	    5, 79, 0, 0, 1340, 1341, 5, 83, 0, 0, 1341, 1342, 5, 83, 0, 0, 1342, 
	    134, 1, 0, 0, 0, 1343, 1344, 5, 67, 0, 0, 1344, 1345, 5, 85, 0, 0, 
	    1345, 1346, 5, 66, 0, 0, 1346, 1347, 5, 69, 0, 0, 1347, 136, 1, 0, 
	    0, 0, 1348, 1349, 5, 67, 0, 0, 1349, 1350, 5, 85, 0, 0, 1350, 1351, 
	    5, 82, 0, 0, 1351, 1352, 5, 82, 0, 0, 1352, 1353, 5, 69, 0, 0, 1353, 
	    1354, 5, 78, 0, 0, 1354, 1355, 5, 84, 0, 0, 1355, 138, 1, 0, 0, 0, 
	    1356, 1357, 5, 67, 0, 0, 1357, 1358, 5, 85, 0, 0, 1358, 1359, 5, 82, 
	    0, 0, 1359, 1360, 5, 82, 0, 0, 1360, 1361, 5, 69, 0, 0, 1361, 1362, 
	    5, 78, 0, 0, 1362, 1363, 5, 84, 0, 0, 1363, 1364, 5, 95, 0, 0, 1364, 
	    1365, 5, 68, 0, 0, 1365, 1366, 5, 65, 0, 0, 1366, 1367, 5, 84, 0, 0, 
	    1367, 1368, 5, 69, 0, 0, 1368, 140, 1, 0, 0, 0, 1369, 1370, 5, 67, 
	    0, 0, 1370, 1371, 5, 85, 0, 0, 1371, 1372, 5, 82, 0, 0, 1372, 1373, 
	    5, 82, 0, 0, 1373, 1374, 5, 69, 0, 0, 1374, 1375, 5, 78, 0, 0, 1375, 
	    1376, 5, 84, 0, 0, 1376, 1377, 5, 95, 0, 0, 1377, 1378, 5, 84, 0, 0, 
	    1378, 1379, 5, 73, 0, 0, 1379, 1380, 5, 77, 0, 0, 1380, 1381, 5, 69, 
	    0, 0, 1381, 142, 1, 0, 0, 0, 1382, 1383, 5, 67, 0, 0, 1383, 1384, 5, 
	    85, 0, 0, 1384, 1385, 5, 82, 0, 0, 1385, 1386, 5, 82, 0, 0, 1386, 1387, 
	    5, 69, 0, 0, 1387, 1388, 5, 78, 0, 0, 1388, 1389, 5, 84, 0, 0, 1389, 
	    1390, 5, 95, 0, 0, 1390, 1391, 5, 84, 0, 0, 1391, 1392, 5, 73, 0, 0, 
	    1392, 1393, 5, 77, 0, 0, 1393, 1394, 5, 69, 0, 0, 1394, 1395, 5, 83, 
	    0, 0, 1395, 1396, 5, 84, 0, 0, 1396, 1397, 5, 65, 0, 0, 1397, 1398, 
	    5, 77, 0, 0, 1398, 1399, 5, 80, 0, 0, 1399, 144, 1, 0, 0, 0, 1400, 
	    1401, 5, 67, 0, 0, 1401, 1402, 5, 85, 0, 0, 1402, 1403, 5, 82, 0, 0, 
	    1403, 1404, 5, 82, 0, 0, 1404, 1405, 5, 69, 0, 0, 1405, 1406, 5, 78, 
	    0, 0, 1406, 1407, 5, 84, 0, 0, 1407, 1408, 5, 95, 0, 0, 1408, 1409, 
	    5, 85, 0, 0, 1409, 1410, 5, 83, 0, 0, 1410, 1411, 5, 69, 0, 0, 1411, 
	    1412, 5, 82, 0, 0, 1412, 146, 1, 0, 0, 0, 1413, 1414, 5, 68, 0, 0, 
	    1414, 1415, 5, 65, 0, 0, 1415, 1416, 5, 89, 0, 0, 1416, 148, 1, 0, 
	    0, 0, 1417, 1418, 5, 68, 0, 0, 1418, 1419, 5, 65, 0, 0, 1419, 1420, 
	    5, 89, 0, 0, 1420, 1421, 5, 83, 0, 0, 1421, 150, 1, 0, 0, 0, 1422, 
	    1423, 5, 68, 0, 0, 1423, 1424, 5, 65, 0, 0, 1424, 1425, 5, 89, 0, 0, 
	    1425, 1426, 5, 79, 0, 0, 1426, 1427, 5, 70, 0, 0, 1427, 1428, 5, 89, 
	    0, 0, 1428, 1429, 5, 69, 0, 0, 1429, 1430, 5, 65, 0, 0, 1430, 1431, 
	    5, 82, 0, 0, 1431, 152, 1, 0, 0, 0, 1432, 1433, 5, 68, 0, 0, 1433, 
	    1434, 5, 65, 0, 0, 1434, 1435, 5, 84, 0, 0, 1435, 1436, 5, 65, 0, 0, 
	    1436, 154, 1, 0, 0, 0, 1437, 1438, 5, 68, 0, 0, 1438, 1439, 5, 65, 
	    0, 0, 1439, 1440, 5, 84, 0, 0, 1440, 1441, 5, 69, 0, 0, 1441, 156, 
	    1, 0, 0, 0, 1442, 1443, 5, 68, 0, 0, 1443, 1444, 5, 65, 0, 0, 1444, 
	    1445, 5, 84, 0, 0, 1445, 1446, 5, 65, 0, 0, 1446, 1447, 5, 66, 0, 0, 
	    1447, 1448, 5, 65, 0, 0, 1448, 1449, 5, 83, 0, 0, 1449, 1450, 5, 69, 
	    0, 0, 1450, 158, 1, 0, 0, 0, 1451, 1452, 5, 68, 0, 0, 1452, 1453, 5, 
	    65, 0, 0, 1453, 1454, 5, 84, 0, 0, 1454, 1455, 5, 65, 0, 0, 1455, 1456, 
	    5, 66, 0, 0, 1456, 1457, 5, 65, 0, 0, 1457, 1458, 5, 83, 0, 0, 1458, 
	    1459, 5, 69, 0, 0, 1459, 1460, 5, 83, 0, 0, 1460, 160, 1, 0, 0, 0, 
	    1461, 1462, 5, 68, 0, 0, 1462, 1463, 5, 65, 0, 0, 1463, 1464, 5, 84, 
	    0, 0, 1464, 1465, 5, 69, 0, 0, 1465, 1466, 5, 65, 0, 0, 1466, 1467, 
	    5, 68, 0, 0, 1467, 1468, 5, 68, 0, 0, 1468, 162, 1, 0, 0, 0, 1469, 
	    1470, 5, 68, 0, 0, 1470, 1471, 5, 65, 0, 0, 1471, 1472, 5, 84, 0, 0, 
	    1472, 1473, 5, 69, 0, 0, 1473, 1474, 5, 95, 0, 0, 1474, 1475, 5, 65, 
	    0, 0, 1475, 1476, 5, 68, 0, 0, 1476, 1477, 5, 68, 0, 0, 1477, 164, 
	    1, 0, 0, 0, 1478, 1479, 5, 68, 0, 0, 1479, 1480, 5, 65, 0, 0, 1480, 
	    1481, 5, 84, 0, 0, 1481, 1482, 5, 69, 0, 0, 1482, 1483, 5, 68, 0, 0, 
	    1483, 1484, 5, 73, 0, 0, 1484, 1485, 5, 70, 0, 0, 1485, 1486, 5, 70, 
	    0, 0, 1486, 166, 1, 0, 0, 0, 1487, 1488, 5, 68, 0, 0, 1488, 1489, 5, 
	    65, 0, 0, 1489, 1490, 5, 84, 0, 0, 1490, 1491, 5, 69, 0, 0, 1491, 1492, 
	    5, 95, 0, 0, 1492, 1493, 5, 68, 0, 0, 1493, 1494, 5, 73, 0, 0, 1494, 
	    1495, 5, 70, 0, 0, 1495, 1496, 5, 70, 0, 0, 1496, 168, 1, 0, 0, 0, 
	    1497, 1498, 5, 68, 0, 0, 1498, 1499, 5, 66, 0, 0, 1499, 1500, 5, 80, 
	    0, 0, 1500, 1501, 5, 82, 0, 0, 1501, 1502, 5, 79, 0, 0, 1502, 1503, 
	    5, 80, 0, 0, 1503, 1504, 5, 69, 0, 0, 1504, 1505, 5, 82, 0, 0, 1505, 
	    1506, 5, 84, 0, 0, 1506, 1507, 5, 73, 0, 0, 1507, 1508, 5, 69, 0, 0, 
	    1508, 1509, 5, 83, 0, 0, 1509, 170, 1, 0, 0, 0, 1510, 1511, 5, 68, 
	    0, 0, 1511, 1512, 5, 69, 0, 0, 1512, 1513, 5, 67, 0, 0, 1513, 172, 
	    1, 0, 0, 0, 1514, 1515, 5, 68, 0, 0, 1515, 1516, 5, 69, 0, 0, 1516, 
	    1517, 5, 67, 0, 0, 1517, 1518, 5, 73, 0, 0, 1518, 1519, 5, 77, 0, 0, 
	    1519, 1520, 5, 65, 0, 0, 1520, 1521, 5, 76, 0, 0, 1521, 174, 1, 0, 
	    0, 0, 1522, 1523, 5, 68, 0, 0, 1523, 1524, 5, 69, 0, 0, 1524, 1525, 
	    5, 67, 0, 0, 1525, 1526, 5, 76, 0, 0, 1526, 1527, 5, 65, 0, 0, 1527, 
	    1528, 5, 82, 0, 0, 1528, 1529, 5, 69, 0, 0, 1529, 176, 1, 0, 0, 0, 
	    1530, 1531, 5, 68, 0, 0, 1531, 1532, 5, 69, 0, 0, 1532, 1533, 5, 67, 
	    0, 0, 1533, 1534, 5, 79, 0, 0, 1534, 1535, 5, 68, 0, 0, 1535, 1536, 
	    5, 69, 0, 0, 1536, 178, 1, 0, 0, 0, 1537, 1538, 5, 68, 0, 0, 1538, 
	    1539, 5, 69, 0, 0, 1539, 1540, 5, 70, 0, 0, 1540, 1541, 5, 65, 0, 0, 
	    1541, 1542, 5, 85, 0, 0, 1542, 1543, 5, 76, 0, 0, 1543, 1544, 5, 84, 
	    0, 0, 1544, 180, 1, 0, 0, 0, 1545, 1546, 5, 68, 0, 0, 1546, 1547, 5, 
	    69, 0, 0, 1547, 1548, 5, 70, 0, 0, 1548, 1549, 5, 73, 0, 0, 1549, 1550, 
	    5, 78, 0, 0, 1550, 1551, 5, 69, 0, 0, 1551, 1552, 5, 68, 0, 0, 1552, 
	    182, 1, 0, 0, 0, 1553, 1554, 5, 68, 0, 0, 1554, 1555, 5, 69, 0, 0, 
	    1555, 1556, 5, 70, 0, 0, 1556, 1557, 5, 73, 0, 0, 1557, 1558, 5, 78, 
	    0, 0, 1558, 1559, 5, 69, 0, 0, 1559, 1560, 5, 82, 0, 0, 1560, 184, 
	    1, 0, 0, 0, 1561, 1562, 5, 68, 0, 0, 1562, 1563, 5, 69, 0, 0, 1563, 
	    1564, 5, 76, 0, 0, 1564, 1565, 5, 69, 0, 0, 1565, 1566, 5, 84, 0, 0, 
	    1566, 1567, 5, 69, 0, 0, 1567, 186, 1, 0, 0, 0, 1568, 1569, 5, 68, 
	    0, 0, 1569, 1570, 5, 69, 0, 0, 1570, 1571, 5, 76, 0, 0, 1571, 1572, 
	    5, 73, 0, 0, 1572, 1573, 5, 77, 0, 0, 1573, 1574, 5, 73, 0, 0, 1574, 
	    1575, 5, 84, 0, 0, 1575, 1576, 5, 69, 0, 0, 1576, 1577, 5, 68, 0, 0, 
	    1577, 188, 1, 0, 0, 0, 1578, 1579, 5, 68, 0, 0, 1579, 1580, 5, 69, 
	    0, 0, 1580, 1581, 5, 83, 0, 0, 1581, 1582, 5, 67, 0, 0, 1582, 190, 
	    1, 0, 0, 0, 1583, 1584, 5, 68, 0, 0, 1584, 1585, 5, 69, 0, 0, 1585, 
	    1586, 5, 83, 0, 0, 1586, 1587, 5, 67, 0, 0, 1587, 1588, 5, 82, 0, 0, 
	    1588, 1589, 5, 73, 0, 0, 1589, 1590, 5, 66, 0, 0, 1590, 1591, 5, 69, 
	    0, 0, 1591, 192, 1, 0, 0, 0, 1592, 1593, 5, 68, 0, 0, 1593, 1594, 5, 
	    69, 0, 0, 1594, 1595, 5, 84, 0, 0, 1595, 1596, 5, 69, 0, 0, 1596, 1597, 
	    5, 82, 0, 0, 1597, 1598, 5, 77, 0, 0, 1598, 1599, 5, 73, 0, 0, 1599, 
	    1600, 5, 78, 0, 0, 1600, 1601, 5, 73, 0, 0, 1601, 1602, 5, 83, 0, 0, 
	    1602, 1603, 5, 84, 0, 0, 1603, 1604, 5, 73, 0, 0, 1604, 1605, 5, 67, 
	    0, 0, 1605, 194, 1, 0, 0, 0, 1606, 1607, 5, 68, 0, 0, 1607, 1608, 5, 
	    70, 0, 0, 1608, 1609, 5, 83, 0, 0, 1609, 196, 1, 0, 0, 0, 1610, 1611, 
	    5, 68, 0, 0, 1611, 1612, 5, 73, 0, 0, 1612, 1613, 5, 82, 0, 0, 1613, 
	    1614, 5, 69, 0, 0, 1614, 1615, 5, 67, 0, 0, 1615, 1616, 5, 84, 0, 0, 
	    1616, 1617, 5, 79, 0, 0, 1617, 1618, 5, 82, 0, 0, 1618, 1619, 5, 73, 
	    0, 0, 1619, 1620, 5, 69, 0, 0, 1620, 1621, 5, 83, 0, 0, 1621, 198, 
	    1, 0, 0, 0, 1622, 1623, 5, 68, 0, 0, 1623, 1624, 5, 73, 0, 0, 1624, 
	    1625, 5, 82, 0, 0, 1625, 1626, 5, 69, 0, 0, 1626, 1627, 5, 67, 0, 0, 
	    1627, 1628, 5, 84, 0, 0, 1628, 1629, 5, 79, 0, 0, 1629, 1630, 5, 82, 
	    0, 0, 1630, 1631, 5, 89, 0, 0, 1631, 200, 1, 0, 0, 0, 1632, 1633, 5, 
	    68, 0, 0, 1633, 1634, 5, 73, 0, 0, 1634, 1635, 5, 83, 0, 0, 1635, 1636, 
	    5, 84, 0, 0, 1636, 1637, 5, 73, 0, 0, 1637, 1638, 5, 78, 0, 0, 1638, 
	    1639, 5, 67, 0, 0, 1639, 1640, 5, 84, 0, 0, 1640, 202, 1, 0, 0, 0, 
	    1641, 1642, 5, 68, 0, 0, 1642, 1643, 5, 73, 0, 0, 1643, 1644, 5, 83, 
	    0, 0, 1644, 1645, 5, 84, 0, 0, 1645, 1646, 5, 82, 0, 0, 1646, 1647, 
	    5, 73, 0, 0, 1647, 1648, 5, 66, 0, 0, 1648, 1649, 5, 85, 0, 0, 1649, 
	    1650, 5, 84, 0, 0, 1650, 1651, 5, 69, 0, 0, 1651, 204, 1, 0, 0, 0, 
	    1652, 1653, 5, 68, 0, 0, 1653, 1654, 5, 73, 0, 0, 1654, 1655, 5, 86, 
	    0, 0, 1655, 206, 1, 0, 0, 0, 1656, 1657, 5, 68, 0, 0, 1657, 1658, 5, 
	    79, 0, 0, 1658, 208, 1, 0, 0, 0, 1659, 1660, 5, 68, 0, 0, 1660, 1661, 
	    5, 79, 0, 0, 1661, 1662, 5, 85, 0, 0, 1662, 1663, 5, 66, 0, 0, 1663, 
	    1664, 5, 76, 0, 0, 1664, 1665, 5, 69, 0, 0, 1665, 210, 1, 0, 0, 0, 
	    1666, 1667, 5, 68, 0, 0, 1667, 1668, 5, 82, 0, 0, 1668, 1669, 5, 79, 
	    0, 0, 1669, 1670, 5, 80, 0, 0, 1670, 212, 1, 0, 0, 0, 1671, 1672, 5, 
	    69, 0, 0, 1672, 1673, 5, 76, 0, 0, 1673, 1674, 5, 83, 0, 0, 1674, 1675, 
	    5, 69, 0, 0, 1675, 214, 1, 0, 0, 0, 1676, 1677, 5, 69, 0, 0, 1677, 
	    1678, 5, 78, 0, 0, 1678, 1679, 5, 68, 0, 0, 1679, 216, 1, 0, 0, 0, 
	    1680, 1681, 5, 69, 0, 0, 1681, 1682, 5, 83, 0, 0, 1682, 1683, 5, 67, 
	    0, 0, 1683, 1684, 5, 65, 0, 0, 1684, 1685, 5, 80, 0, 0, 1685, 1686, 
	    5, 69, 0, 0, 1686, 218, 1, 0, 0, 0, 1687, 1688, 5, 69, 0, 0, 1688, 
	    1689, 5, 83, 0, 0, 1689, 1690, 5, 67, 0, 0, 1690, 1691, 5, 65, 0, 0, 
	    1691, 1692, 5, 80, 0, 0, 1692, 1693, 5, 69, 0, 0, 1693, 1694, 5, 68, 
	    0, 0, 1694, 220, 1, 0, 0, 0, 1695, 1696, 5, 69, 0, 0, 1696, 1697, 5, 
	    86, 0, 0, 1697, 1698, 5, 79, 0, 0, 1698, 1699, 5, 76, 0, 0, 1699, 1700, 
	    5, 85, 0, 0, 1700, 1701, 5, 84, 0, 0, 1701, 1702, 5, 73, 0, 0, 1702, 
	    1703, 5, 79, 0, 0, 1703, 1704, 5, 78, 0, 0, 1704, 222, 1, 0, 0, 0, 
	    1705, 1706, 5, 69, 0, 0, 1706, 1707, 5, 88, 0, 0, 1707, 1708, 5, 67, 
	    0, 0, 1708, 1709, 5, 69, 0, 0, 1709, 1710, 5, 80, 0, 0, 1710, 1711, 
	    5, 84, 0, 0, 1711, 224, 1, 0, 0, 0, 1712, 1713, 5, 69, 0, 0, 1713, 
	    1714, 5, 88, 0, 0, 1714, 1715, 5, 67, 0, 0, 1715, 1716, 5, 72, 0, 0, 
	    1716, 1717, 5, 65, 0, 0, 1717, 1718, 5, 78, 0, 0, 1718, 1719, 5, 71, 
	    0, 0, 1719, 1720, 5, 69, 0, 0, 1720, 226, 1, 0, 0, 0, 1721, 1722, 5, 
	    69, 0, 0, 1722, 1723, 5, 88, 0, 0, 1723, 1724, 5, 67, 0, 0, 1724, 1725, 
	    5, 76, 0, 0, 1725, 1726, 5, 85, 0, 0, 1726, 1727, 5, 68, 0, 0, 1727, 
	    1728, 5, 69, 0, 0, 1728, 228, 1, 0, 0, 0, 1729, 1730, 5, 69, 0, 0, 
	    1730, 1731, 5, 88, 0, 0, 1731, 1732, 5, 69, 0, 0, 1732, 1733, 5, 67, 
	    0, 0, 1733, 1734, 5, 85, 0, 0, 1734, 1735, 5, 84, 0, 0, 1735, 1736, 
	    5, 69, 0, 0, 1736, 230, 1, 0, 0, 0, 1737, 1738, 5, 69, 0, 0, 1738, 
	    1739, 5, 88, 0, 0, 1739, 1740, 5, 73, 0, 0, 1740, 1741, 5, 83, 0, 0, 
	    1741, 1742, 5, 84, 0, 0, 1742, 1743, 5, 83, 0, 0, 1743, 232, 1, 0, 
	    0, 0, 1744, 1745, 5, 69, 0, 0, 1745, 1746, 5, 88, 0, 0, 1746, 1747, 
	    5, 80, 0, 0, 1747, 1748, 5, 76, 0, 0, 1748, 1749, 5, 65, 0, 0, 1749, 
	    1750, 5, 73, 0, 0, 1750, 1751, 5, 78, 0, 0, 1751, 234, 1, 0, 0, 0, 
	    1752, 1753, 5, 69, 0, 0, 1753, 1754, 5, 88, 0, 0, 1754, 1755, 5, 80, 
	    0, 0, 1755, 1756, 5, 79, 0, 0, 1756, 1757, 5, 82, 0, 0, 1757, 1758, 
	    5, 84, 0, 0, 1758, 236, 1, 0, 0, 0, 1759, 1760, 5, 69, 0, 0, 1760, 
	    1761, 5, 88, 0, 0, 1761, 1762, 5, 84, 0, 0, 1762, 1763, 5, 69, 0, 0, 
	    1763, 1764, 5, 78, 0, 0, 1764, 1765, 5, 68, 0, 0, 1765, 1766, 5, 69, 
	    0, 0, 1766, 1767, 5, 68, 0, 0, 1767, 238, 1, 0, 0, 0, 1768, 1769, 5, 
	    69, 0, 0, 1769, 1770, 5, 88, 0, 0, 1770, 1771, 5, 84, 0, 0, 1771, 1772, 
	    5, 69, 0, 0, 1772, 1773, 5, 82, 0, 0, 1773, 1774, 5, 78, 0, 0, 1774, 
	    1775, 5, 65, 0, 0, 1775, 1776, 5, 76, 0, 0, 1776, 240, 1, 0, 0, 0, 
	    1777, 1778, 5, 69, 0, 0, 1778, 1779, 5, 88, 0, 0, 1779, 1780, 5, 84, 
	    0, 0, 1780, 1781, 5, 82, 0, 0, 1781, 1782, 5, 65, 0, 0, 1782, 1783, 
	    5, 67, 0, 0, 1783, 1784, 5, 84, 0, 0, 1784, 242, 1, 0, 0, 0, 1785, 
	    1786, 5, 70, 0, 0, 1786, 1787, 5, 65, 0, 0, 1787, 1788, 5, 76, 0, 0, 
	    1788, 1789, 5, 83, 0, 0, 1789, 1790, 5, 69, 0, 0, 1790, 244, 1, 0, 
	    0, 0, 1791, 1792, 5, 70, 0, 0, 1792, 1793, 5, 69, 0, 0, 1793, 1794, 
	    5, 84, 0, 0, 1794, 1795, 5, 67, 0, 0, 1795, 1796, 5, 72, 0, 0, 1796, 
	    246, 1, 0, 0, 0, 1797, 1798, 5, 70, 0, 0, 1798, 1799, 5, 73, 0, 0, 
	    1799, 1800, 5, 69, 0, 0, 1800, 1801, 5, 76, 0, 0, 1801, 1802, 5, 68, 
	    0, 0, 1802, 1803, 5, 83, 0, 0, 1803, 248, 1, 0, 0, 0, 1804, 1805, 5, 
	    70, 0, 0, 1805, 1806, 5, 73, 0, 0, 1806, 1807, 5, 76, 0, 0, 1807, 1808, 
	    5, 84, 0, 0, 1808, 1809, 5, 69, 0, 0, 1809, 1810, 5, 82, 0, 0, 1810, 
	    250, 1, 0, 0, 0, 1811, 1812, 5, 70, 0, 0, 1812, 1813, 5, 73, 0, 0, 
	    1813, 1814, 5, 76, 0, 0, 1814, 1815, 5, 69, 0, 0, 1815, 1816, 5, 70, 
	    0, 0, 1816, 1817, 5, 79, 0, 0, 1817, 1818, 5, 82, 0, 0, 1818, 1819, 
	    5, 77, 0, 0, 1819, 1820, 5, 65, 0, 0, 1820, 1821, 5, 84, 0, 0, 1821, 
	    252, 1, 0, 0, 0, 1822, 1823, 5, 70, 0, 0, 1823, 1824, 5, 73, 0, 0, 
	    1824, 1825, 5, 82, 0, 0, 1825, 1826, 5, 83, 0, 0, 1826, 1827, 5, 84, 
	    0, 0, 1827, 254, 1, 0, 0, 0, 1828, 1829, 5, 70, 0, 0, 1829, 1830, 5, 
	    76, 0, 0, 1830, 1831, 5, 79, 0, 0, 1831, 1832, 5, 65, 0, 0, 1832, 1833, 
	    5, 84, 0, 0, 1833, 256, 1, 0, 0, 0, 1834, 1835, 5, 70, 0, 0, 1835, 
	    1836, 5, 79, 0, 0, 1836, 1837, 5, 76, 0, 0, 1837, 1838, 5, 76, 0, 0, 
	    1838, 1839, 5, 79, 0, 0, 1839, 1840, 5, 87, 0, 0, 1840, 1841, 5, 73, 
	    0, 0, 1841, 1842, 5, 78, 0, 0, 1842, 1843, 5, 71, 0, 0, 1843, 258, 
	    1, 0, 0, 0, 1844, 1845, 5, 70, 0, 0, 1845, 1846, 5, 79, 0, 0, 1846, 
	    1847, 5, 82, 0, 0, 1847, 260, 1, 0, 0, 0, 1848, 1849, 5, 70, 0, 0, 
	    1849, 1850, 5, 79, 0, 0, 1850, 1851, 5, 82, 0, 0, 1851, 1852, 5, 69, 
	    0, 0, 1852, 1853, 5, 73, 0, 0, 1853, 1854, 5, 71, 0, 0, 1854, 1855, 
	    5, 78, 0, 0, 1855, 262, 1, 0, 0, 0, 1856, 1857, 5, 70, 0, 0, 1857, 
	    1858, 5, 79, 0, 0, 1858, 1859, 5, 82, 0, 0, 1859, 1860, 5, 77, 0, 0, 
	    1860, 1861, 5, 65, 0, 0, 1861, 1862, 5, 84, 0, 0, 1862, 264, 1, 0, 
	    0, 0, 1863, 1864, 5, 70, 0, 0, 1864, 1865, 5, 79, 0, 0, 1865, 1866, 
	    5, 82, 0, 0, 1866, 1867, 5, 77, 0, 0, 1867, 1868, 5, 65, 0, 0, 1868, 
	    1869, 5, 84, 0, 0, 1869, 1870, 5, 84, 0, 0, 1870, 1871, 5, 69, 0, 0, 
	    1871, 1872, 5, 68, 0, 0, 1872, 266, 1, 0, 0, 0, 1873, 1874, 5, 70, 
	    0, 0, 1874, 1875, 5, 82, 0, 0, 1875, 1876, 5, 79, 0, 0, 1876, 1877, 
	    5, 77, 0, 0, 1877, 268, 1, 0, 0, 0, 1878, 1879, 5, 70, 0, 0, 1879, 
	    1880, 5, 82, 0, 0, 1880, 1881, 5, 79, 0, 0, 1881, 1882, 5, 77, 0, 0, 
	    1882, 1883, 5, 95, 0, 0, 1883, 1884, 5, 74, 0, 0, 1884, 1885, 5, 83, 
	    0, 0, 1885, 1886, 5, 79, 0, 0, 1886, 1887, 5, 78, 0, 0, 1887, 270, 
	    1, 0, 0, 0, 1888, 1889, 5, 70, 0, 0, 1889, 1890, 5, 85, 0, 0, 1890, 
	    1891, 5, 76, 0, 0, 1891, 1892, 5, 76, 0, 0, 1892, 272, 1, 0, 0, 0, 
	    1893, 1894, 5, 70, 0, 0, 1894, 1895, 5, 85, 0, 0, 1895, 1896, 5, 78, 
	    0, 0, 1896, 1897, 5, 67, 0, 0, 1897, 1898, 5, 84, 0, 0, 1898, 1899, 
	    5, 73, 0, 0, 1899, 1900, 5, 79, 0, 0, 1900, 1901, 5, 78, 0, 0, 1901, 
	    274, 1, 0, 0, 0, 1902, 1903, 5, 70, 0, 0, 1903, 1904, 5, 85, 0, 0, 
	    1904, 1905, 5, 78, 0, 0, 1905, 1906, 5, 67, 0, 0, 1906, 1907, 5, 84, 
	    0, 0, 1907, 1908, 5, 73, 0, 0, 1908, 1909, 5, 79, 0, 0, 1909, 1910, 
	    5, 78, 0, 0, 1910, 1911, 5, 83, 0, 0, 1911, 276, 1, 0, 0, 0, 1912, 
	    1913, 5, 71, 0, 0, 1913, 1914, 5, 69, 0, 0, 1914, 1915, 5, 78, 0, 0, 
	    1915, 1916, 5, 69, 0, 0, 1916, 1917, 5, 82, 0, 0, 1917, 1918, 5, 65, 
	    0, 0, 1918, 1919, 5, 84, 0, 0, 1919, 1920, 5, 69, 0, 0, 1920, 1921, 
	    5, 68, 0, 0, 1921, 278, 1, 0, 0, 0, 1922, 1923, 5, 71, 0, 0, 1923, 
	    1924, 5, 76, 0, 0, 1924, 1925, 5, 79, 0, 0, 1925, 1926, 5, 66, 0, 0, 
	    1926, 1927, 5, 65, 0, 0, 1927, 1928, 5, 76, 0, 0, 1928, 280, 1, 0, 
	    0, 0, 1929, 1930, 5, 71, 0, 0, 1930, 1931, 5, 82, 0, 0, 1931, 1932, 
	    5, 65, 0, 0, 1932, 1933, 5, 78, 0, 0, 1933, 1934, 5, 84, 0, 0, 1934, 
	    282, 1, 0, 0, 0, 1935, 1936, 5, 71, 0, 0, 1936, 1937, 5, 82, 0, 0, 
	    1937, 1938, 5, 79, 0, 0, 1938, 1939, 5, 85, 0, 0, 1939, 1940, 5, 80, 
	    0, 0, 1940, 284, 1, 0, 0, 0, 1941, 1942, 5, 71, 0, 0, 1942, 1943, 5, 
	    82, 0, 0, 1943, 1944, 5, 79, 0, 0, 1944, 1945, 5, 85, 0, 0, 1945, 1946, 
	    5, 80, 0, 0, 1946, 1947, 5, 73, 0, 0, 1947, 1948, 5, 78, 0, 0, 1948, 
	    1949, 5, 71, 0, 0, 1949, 286, 1, 0, 0, 0, 1950, 1951, 5, 72, 0, 0, 
	    1951, 1952, 5, 65, 0, 0, 1952, 1953, 5, 86, 0, 0, 1953, 1954, 5, 73, 
	    0, 0, 1954, 1955, 5, 78, 0, 0, 1955, 1956, 5, 71, 0, 0, 1956, 288, 
	    1, 0, 0, 0, 1957, 1958, 5, 72, 0, 0, 1958, 1959, 5, 79, 0, 0, 1959, 
	    1960, 5, 85, 0, 0, 1960, 1961, 5, 82, 0, 0, 1961, 290, 1, 0, 0, 0, 
	    1962, 1963, 5, 72, 0, 0, 1963, 1964, 5, 79, 0, 0, 1964, 1965, 5, 85, 
	    0, 0, 1965, 1966, 5, 82, 0, 0, 1966, 1967, 5, 83, 0, 0, 1967, 292, 
	    1, 0, 0, 0, 1968, 1969, 5, 73, 0, 0, 1969, 1970, 5, 68, 0, 0, 1970, 
	    1971, 5, 69, 0, 0, 1971, 1972, 5, 78, 0, 0, 1972, 1973, 5, 84, 0, 0, 
	    1973, 1974, 5, 73, 0, 0, 1974, 1975, 5, 70, 0, 0, 1975, 1976, 5, 73, 
	    0, 0, 1976, 1977, 5, 69, 0, 0, 1977, 1978, 5, 82, 0, 0, 1978, 294, 
	    1, 0, 0, 0, 1979, 1980, 5, 73, 0, 0, 1980, 1981, 5, 68, 0, 0, 1981, 
	    1982, 5, 69, 0, 0, 1982, 1983, 5, 78, 0, 0, 1983, 1984, 5, 84, 0, 0, 
	    1984, 1985, 5, 73, 0, 0, 1985, 1986, 5, 84, 0, 0, 1986, 1987, 5, 89, 
	    0, 0, 1987, 296, 1, 0, 0, 0, 1988, 1989, 5, 73, 0, 0, 1989, 1990, 5, 
	    70, 0, 0, 1990, 298, 1, 0, 0, 0, 1991, 1992, 5, 73, 0, 0, 1992, 1993, 
	    5, 71, 0, 0, 1993, 1994, 5, 78, 0, 0, 1994, 1995, 5, 79, 0, 0, 1995, 
	    1996, 5, 82, 0, 0, 1996, 1997, 5, 69, 0, 0, 1997, 300, 1, 0, 0, 0, 
	    1998, 1999, 5, 73, 0, 0, 1999, 2000, 5, 77, 0, 0, 2000, 2001, 5, 77, 
	    0, 0, 2001, 2002, 5, 69, 0, 0, 2002, 2003, 5, 68, 0, 0, 2003, 2004, 
	    5, 73, 0, 0, 2004, 2005, 5, 65, 0, 0, 2005, 2006, 5, 84, 0, 0, 2006, 
	    2007, 5, 69, 0, 0, 2007, 302, 1, 0, 0, 0, 2008, 2009, 5, 73, 0, 0, 
	    2009, 2010, 5, 77, 0, 0, 2010, 2011, 5, 80, 0, 0, 2011, 2012, 5, 79, 
	    0, 0, 2012, 2013, 5, 82, 0, 0, 2013, 2014, 5, 84, 0, 0, 2014, 304, 
	    1, 0, 0, 0, 2015, 2016, 5, 73, 0, 0, 2016, 2017, 5, 78, 0, 0, 2017, 
	    306, 1, 0, 0, 0, 2018, 2019, 5, 73, 0, 0, 2019, 2020, 5, 78, 0, 0, 
	    2020, 2021, 5, 67, 0, 0, 2021, 2022, 5, 76, 0, 0, 2022, 2023, 5, 85, 
	    0, 0, 2023, 2024, 5, 68, 0, 0, 2024, 2025, 5, 69, 0, 0, 2025, 308, 
	    1, 0, 0, 0, 2026, 2027, 5, 73, 0, 0, 2027, 2028, 5, 78, 0, 0, 2028, 
	    2029, 5, 68, 0, 0, 2029, 2030, 5, 69, 0, 0, 2030, 2031, 5, 88, 0, 0, 
	    2031, 310, 1, 0, 0, 0, 2032, 2033, 5, 73, 0, 0, 2033, 2034, 5, 78, 
	    0, 0, 2034, 2035, 5, 68, 0, 0, 2035, 2036, 5, 69, 0, 0, 2036, 2037, 
	    5, 88, 0, 0, 2037, 2038, 5, 69, 0, 0, 2038, 2039, 5, 83, 0, 0, 2039, 
	    312, 1, 0, 0, 0, 2040, 2041, 5, 73, 0, 0, 2041, 2042, 5, 78, 0, 0, 
	    2042, 2043, 5, 78, 0, 0, 2043, 2044, 5, 69, 0, 0, 2044, 2045, 5, 82, 
	    0, 0, 2045, 314, 1, 0, 0, 0, 2046, 2047, 5, 73, 0, 0, 2047, 2048, 5, 
	    78, 0, 0, 2048, 2049, 5, 80, 0, 0, 2049, 2050, 5, 65, 0, 0, 2050, 2051, 
	    5, 84, 0, 0, 2051, 2052, 5, 72, 0, 0, 2052, 316, 1, 0, 0, 0, 2053, 
	    2054, 5, 73, 0, 0, 2054, 2055, 5, 78, 0, 0, 2055, 2056, 5, 80, 0, 0, 
	    2056, 2057, 5, 85, 0, 0, 2057, 2058, 5, 84, 0, 0, 2058, 318, 1, 0, 
	    0, 0, 2059, 2060, 5, 73, 0, 0, 2060, 2061, 5, 78, 0, 0, 2061, 2062, 
	    5, 80, 0, 0, 2062, 2063, 5, 85, 0, 0, 2063, 2064, 5, 84, 0, 0, 2064, 
	    2065, 5, 70, 0, 0, 2065, 2066, 5, 79, 0, 0, 2066, 2067, 5, 82, 0, 0, 
	    2067, 2068, 5, 77, 0, 0, 2068, 2069, 5, 65, 0, 0, 2069, 2070, 5, 84, 
	    0, 0, 2070, 320, 1, 0, 0, 0, 2071, 2072, 5, 73, 0, 0, 2072, 2073, 5, 
	    78, 0, 0, 2073, 2074, 5, 83, 0, 0, 2074, 2075, 5, 69, 0, 0, 2075, 2076, 
	    5, 82, 0, 0, 2076, 2077, 5, 84, 0, 0, 2077, 322, 1, 0, 0, 0, 2078, 
	    2079, 5, 73, 0, 0, 2079, 2080, 5, 78, 0, 0, 2080, 2081, 5, 84, 0, 0, 
	    2081, 2082, 5, 69, 0, 0, 2082, 2083, 5, 82, 0, 0, 2083, 2084, 5, 83, 
	    0, 0, 2084, 2085, 5, 69, 0, 0, 2085, 2086, 5, 67, 0, 0, 2086, 2087, 
	    5, 84, 0, 0, 2087, 324, 1, 0, 0, 0, 2088, 2089, 5, 73, 0, 0, 2089, 
	    2090, 5, 78, 0, 0, 2090, 2091, 5, 84, 0, 0, 2091, 2092, 5, 69, 0, 0, 
	    2092, 2093, 5, 82, 0, 0, 2093, 2094, 5, 86, 0, 0, 2094, 2095, 5, 65, 
	    0, 0, 2095, 2096, 5, 76, 0, 0, 2096, 326, 1, 0, 0, 0, 2097, 2098, 5, 
	    73, 0, 0, 2098, 2099, 5, 78, 0, 0, 2099, 2100, 5, 84, 0, 0, 2100, 328, 
	    1, 0, 0, 0, 2101, 2102, 5, 73, 0, 0, 2102, 2103, 5, 78, 0, 0, 2103, 
	    2104, 5, 84, 0, 0, 2104, 2105, 5, 69, 0, 0, 2105, 2106, 5, 71, 0, 0, 
	    2106, 2107, 5, 69, 0, 0, 2107, 2108, 5, 82, 0, 0, 2108, 330, 1, 0, 
	    0, 0, 2109, 2110, 5, 73, 0, 0, 2110, 2111, 5, 78, 0, 0, 2111, 2112, 
	    5, 84, 0, 0, 2112, 2113, 5, 79, 0, 0, 2113, 332, 1, 0, 0, 0, 2114, 
	    2115, 5, 73, 0, 0, 2115, 2116, 5, 78, 0, 0, 2116, 2117, 5, 86, 0, 0, 
	    2117, 2118, 5, 79, 0, 0, 2118, 2119, 5, 75, 0, 0, 2119, 2120, 5, 69, 
	    0, 0, 2120, 2121, 5, 82, 0, 0, 2121, 334, 1, 0, 0, 0, 2122, 2123, 5, 
	    73, 0, 0, 2123, 2124, 5, 83, 0, 0, 2124, 336, 1, 0, 0, 0, 2125, 2126, 
	    5, 73, 0, 0, 2126, 2127, 5, 84, 0, 0, 2127, 2128, 5, 69, 0, 0, 2128, 
	    2129, 5, 77, 0, 0, 2129, 2130, 5, 83, 0, 0, 2130, 338, 1, 0, 0, 0, 
	    2131, 2132, 5, 73, 0, 0, 2132, 2133, 5, 76, 0, 0, 2133, 2134, 5, 73, 
	    0, 0, 2134, 2135, 5, 75, 0, 0, 2135, 2136, 5, 69, 0, 0, 2136, 340, 
	    1, 0, 0, 0, 2137, 2138, 5, 74, 0, 0, 2138, 2139, 5, 79, 0, 0, 2139, 
	    2140, 5, 73, 0, 0, 2140, 2141, 5, 78, 0, 0, 2141, 342, 1, 0, 0, 0, 
	    2142, 2143, 5, 75, 0, 0, 2143, 2144, 5, 69, 0, 0, 2144, 2145, 5, 89, 
	    0, 0, 2145, 344, 1, 0, 0, 0, 2146, 2147, 5, 75, 0, 0, 2147, 2148, 5, 
	    69, 0, 0, 2148, 2149, 5, 89, 0, 0, 2149, 2150, 5, 83, 0, 0, 2150, 346, 
	    1, 0, 0, 0, 2151, 2152, 5, 76, 0, 0, 2152, 2153, 5, 65, 0, 0, 2153, 
	    2154, 5, 78, 0, 0, 2154, 2155, 5, 71, 0, 0, 2155, 2156, 5, 85, 0, 0, 
	    2156, 2157, 5, 65, 0, 0, 2157, 2158, 5, 71, 0, 0, 2158, 2159, 5, 69, 
	    0, 0, 2159, 348, 1, 0, 0, 0, 2160, 2161, 5, 76, 0, 0, 2161, 2162, 5, 
	    65, 0, 0, 2162, 2163, 5, 83, 0, 0, 2163, 2164, 5, 84, 0, 0, 2164, 350, 
	    1, 0, 0, 0, 2165, 2166, 5, 76, 0, 0, 2166, 2167, 5, 65, 0, 0, 2167, 
	    2168, 5, 84, 0, 0, 2168, 2169, 5, 69, 0, 0, 2169, 2170, 5, 82, 0, 0, 
	    2170, 2171, 5, 65, 0, 0, 2171, 2172, 5, 76, 0, 0, 2172, 352, 1, 0, 
	    0, 0, 2173, 2174, 5, 76, 0, 0, 2174, 2175, 5, 65, 0, 0, 2175, 2176, 
	    5, 90, 0, 0, 2176, 2177, 5, 89, 0, 0, 2177, 354, 1, 0, 0, 0, 2178, 
	    2179, 5, 76, 0, 0, 2179, 2180, 5, 69, 0, 0, 2180, 2181, 5, 65, 0, 0, 
	    2181, 2182, 5, 68, 0, 0, 2182, 2183, 5, 73, 0, 0, 2183, 2184, 5, 78, 
	    0, 0, 2184, 2185, 5, 71, 0, 0, 2185, 356, 1, 0, 0, 0, 2186, 2187, 5, 
	    76, 0, 0, 2187, 2188, 5, 69, 0, 0, 2188, 2189, 5, 70, 0, 0, 2189, 2190, 
	    5, 84, 0, 0, 2190, 358, 1, 0, 0, 0, 2191, 2192, 5, 76, 0, 0, 2192, 
	    2193, 5, 73, 0, 0, 2193, 2194, 5, 75, 0, 0, 2194, 2195, 5, 69, 0, 0, 
	    2195, 360, 1, 0, 0, 0, 2196, 2197, 5, 76, 0, 0, 2197, 2198, 5, 73, 
	    0, 0, 2198, 2199, 5, 77, 0, 0, 2199, 2200, 5, 73, 0, 0, 2200, 2201, 
	    5, 84, 0, 0, 2201, 362, 1, 0, 0, 0, 2202, 2203, 5, 76, 0, 0, 2203, 
	    2204, 5, 73, 0, 0, 2204, 2205, 5, 78, 0, 0, 2205, 2206, 5, 69, 0, 0, 
	    2206, 2207, 5, 83, 0, 0, 2207, 364, 1, 0, 0, 0, 2208, 2209, 5, 76, 
	    0, 0, 2209, 2210, 5, 73, 0, 0, 2210, 2211, 5, 83, 0, 0, 2211, 2212, 
	    5, 84, 0, 0, 2212, 366, 1, 0, 0, 0, 2213, 2214, 5, 76, 0, 0, 2214, 
	    2215, 5, 73, 0, 0, 2215, 2216, 5, 83, 0, 0, 2216, 2217, 5, 84, 0, 0, 
	    2217, 2218, 5, 65, 0, 0, 2218, 2219, 5, 71, 0, 0, 2219, 2220, 5, 71, 
	    0, 0, 2220, 368, 1, 0, 0, 0, 2221, 2222, 5, 76, 0, 0, 2222, 2223, 5, 
	    73, 0, 0, 2223, 2224, 5, 86, 0, 0, 2224, 2225, 5, 69, 0, 0, 2225, 370, 
	    1, 0, 0, 0, 2226, 2227, 5, 76, 0, 0, 2227, 2228, 5, 79, 0, 0, 2228, 
	    2229, 5, 65, 0, 0, 2229, 2230, 5, 68, 0, 0, 2230, 372, 1, 0, 0, 0, 
	    2231, 2232, 5, 76, 0, 0, 2232, 2233, 5, 79, 0, 0, 2233, 2234, 5, 67, 
	    0, 0, 2234, 2235, 5, 65, 0, 0, 2235, 2236, 5, 76, 0, 0, 2236, 374, 
	    1, 0, 0, 0, 2237, 2238, 5, 76, 0, 0, 2238, 2239, 5, 79, 0, 0, 2239, 
	    2240, 5, 67, 0, 0, 2240, 2241, 5, 65, 0, 0, 2241, 2242, 5, 84, 0, 0, 
	    2242, 2243, 5, 73, 0, 0, 2243, 2244, 5, 79, 0, 0, 2244, 2245, 5, 78, 
	    0, 0, 2245, 376, 1, 0, 0, 0, 2246, 2247, 5, 76, 0, 0, 2247, 2248, 5, 
	    79, 0, 0, 2248, 2249, 5, 67, 0, 0, 2249, 2250, 5, 75, 0, 0, 2250, 378, 
	    1, 0, 0, 0, 2251, 2252, 5, 76, 0, 0, 2252, 2253, 5, 79, 0, 0, 2253, 
	    2254, 5, 67, 0, 0, 2254, 2255, 5, 75, 0, 0, 2255, 2256, 5, 83, 0, 0, 
	    2256, 380, 1, 0, 0, 0, 2257, 2258, 5, 76, 0, 0, 2258, 2259, 5, 79, 
	    0, 0, 2259, 2260, 5, 71, 0, 0, 2260, 2261, 5, 73, 0, 0, 2261, 2262, 
	    5, 67, 0, 0, 2262, 2263, 5, 65, 0, 0, 2263, 2264, 5, 76, 0, 0, 2264, 
	    382, 1, 0, 0, 0, 2265, 2266, 5, 76, 0, 0, 2266, 2267, 5, 79, 0, 0, 
	    2267, 2268, 5, 78, 0, 0, 2268, 2269, 5, 71, 0, 0, 2269, 384, 1, 0, 
	    0, 0, 2270, 2271, 5, 77, 0, 0, 2271, 2272, 5, 65, 0, 0, 2272, 2273, 
	    5, 67, 0, 0, 2273, 2274, 5, 82, 0, 0, 2274, 2275, 5, 79, 0, 0, 2275, 
	    386, 1, 0, 0, 0, 2276, 2277, 5, 77, 0, 0, 2277, 2278, 5, 65, 0, 0, 
	    2278, 2279, 5, 80, 0, 0, 2279, 388, 1, 0, 0, 0, 2280, 2281, 5, 77, 
	    0, 0, 2281, 2282, 5, 65, 0, 0, 2282, 2283, 5, 80, 0, 0, 2283, 2284, 
	    5, 95, 0, 0, 2284, 2285, 5, 70, 0, 0, 2285, 2286, 5, 82, 0, 0, 2286, 
	    2287, 5, 79, 0, 0, 2287, 2288, 5, 77, 0, 0, 2288, 2289, 5, 95, 0, 0, 
	    2289, 2290, 5, 69, 0, 0, 2290, 2291, 5, 78, 0, 0, 2291, 2292, 5, 84, 
	    0, 0, 2292, 2293, 5, 82, 0, 0, 2293, 2294, 5, 73, 0, 0, 2294, 2295, 
	    5, 69, 0, 0, 2295, 2296, 5, 83, 0, 0, 2296, 390, 1, 0, 0, 0, 2297, 
	    2298, 5, 77, 0, 0, 2298, 2299, 5, 65, 0, 0, 2299, 2300, 5, 84, 0, 0, 
	    2300, 2301, 5, 67, 0, 0, 2301, 2302, 5, 72, 0, 0, 2302, 2303, 5, 69, 
	    0, 0, 2303, 2304, 5, 68, 0, 0, 2304, 392, 1, 0, 0, 0, 2305, 2306, 5, 
	    77, 0, 0, 2306, 2307, 5, 65, 0, 0, 2307, 2308, 5, 84, 0, 0, 2308, 2309, 
	    5, 69, 0, 0, 2309, 2310, 5, 82, 0, 0, 2310, 2311, 5, 73, 0, 0, 2311, 
	    2312, 5, 65, 0, 0, 2312, 2313, 5, 76, 0, 0, 2313, 2314, 5, 73, 0, 0, 
	    2314, 2315, 5, 90, 0, 0, 2315, 2316, 5, 69, 0, 0, 2316, 2317, 5, 68, 
	    0, 0, 2317, 394, 1, 0, 0, 0, 2318, 2319, 5, 77, 0, 0, 2319, 2320, 5, 
	    69, 0, 0, 2320, 2321, 5, 82, 0, 0, 2321, 2322, 5, 71, 0, 0, 2322, 2323, 
	    5, 69, 0, 0, 2323, 396, 1, 0, 0, 0, 2324, 2325, 5, 77, 0, 0, 2325, 
	    2326, 5, 73, 0, 0, 2326, 2327, 5, 67, 0, 0, 2327, 2328, 5, 82, 0, 0, 
	    2328, 2329, 5, 79, 0, 0, 2329, 2330, 5, 83, 0, 0, 2330, 2331, 5, 69, 
	    0, 0, 2331, 2332, 5, 67, 0, 0, 2332, 2333, 5, 79, 0, 0, 2333, 2334, 
	    5, 78, 0, 0, 2334, 2335, 5, 68, 0, 0, 2335, 398, 1, 0, 0, 0, 2336, 
	    2337, 5, 77, 0, 0, 2337, 2338, 5, 73, 0, 0, 2338, 2339, 5, 67, 0, 0, 
	    2339, 2340, 5, 82, 0, 0, 2340, 2341, 5, 79, 0, 0, 2341, 2342, 5, 83, 
	    0, 0, 2342, 2343, 5, 69, 0, 0, 2343, 2344, 5, 67, 0, 0, 2344, 2345, 
	    5, 79, 0, 0, 2345, 2346, 5, 78, 0, 0, 2346, 2347, 5, 68, 0, 0, 2347, 
	    2348, 5, 83, 0, 0, 2348, 400, 1, 0, 0, 0, 2349, 2350, 5, 77, 0, 0, 
	    2350, 2351, 5, 73, 0, 0, 2351, 2352, 5, 76, 0, 0, 2352, 2353, 5, 76, 
	    0, 0, 2353, 2354, 5, 73, 0, 0, 2354, 2355, 5, 83, 0, 0, 2355, 2356, 
	    5, 69, 0, 0, 2356, 2357, 5, 67, 0, 0, 2357, 2358, 5, 79, 0, 0, 2358, 
	    2359, 5, 78, 0, 0, 2359, 2360, 5, 68, 0, 0, 2360, 402, 1, 0, 0, 0, 
	    2361, 2362, 5, 77, 0, 0, 2362, 2363, 5, 73, 0, 0, 2363, 2364, 5, 76, 
	    0, 0, 2364, 2365, 5, 76, 0, 0, 2365, 2366, 5, 73, 0, 0, 2366, 2367, 
	    5, 83, 0, 0, 2367, 2368, 5, 69, 0, 0, 2368, 2369, 5, 67, 0, 0, 2369, 
	    2370, 5, 79, 0, 0, 2370, 2371, 5, 78, 0, 0, 2371, 2372, 5, 68, 0, 0, 
	    2372, 2373, 5, 83, 0, 0, 2373, 404, 1, 0, 0, 0, 2374, 2375, 5, 77, 
	    0, 0, 2375, 2376, 5, 73, 0, 0, 2376, 2377, 5, 78, 0, 0, 2377, 2378, 
	    5, 85, 0, 0, 2378, 2379, 5, 83, 0, 0, 2379, 406, 1, 0, 0, 0, 2380, 
	    2381, 5, 77, 0, 0, 2381, 2382, 5, 73, 0, 0, 2382, 2383, 5, 78, 0, 0, 
	    2383, 2384, 5, 85, 0, 0, 2384, 2385, 5, 84, 0, 0, 2385, 2386, 5, 69, 
	    0, 0, 2386, 408, 1, 0, 0, 0, 2387, 2388, 5, 77, 0, 0, 2388, 2389, 5, 
	    73, 0, 0, 2389, 2390, 5, 78, 0, 0, 2390, 2391, 5, 85, 0, 0, 2391, 2392, 
	    5, 84, 0, 0, 2392, 2393, 5, 69, 0, 0, 2393, 2394, 5, 83, 0, 0, 2394, 
	    410, 1, 0, 0, 0, 2395, 2396, 5, 77, 0, 0, 2396, 2397, 5, 79, 0, 0, 
	    2397, 2398, 5, 68, 0, 0, 2398, 2399, 5, 69, 0, 0, 2399, 412, 1, 0, 
	    0, 0, 2400, 2401, 5, 77, 0, 0, 2401, 2402, 5, 79, 0, 0, 2402, 2403, 
	    5, 68, 0, 0, 2403, 2404, 5, 73, 0, 0, 2404, 2405, 5, 70, 0, 0, 2405, 
	    2406, 5, 73, 0, 0, 2406, 2407, 5, 69, 0, 0, 2407, 2408, 5, 83, 0, 0, 
	    2408, 414, 1, 0, 0, 0, 2409, 2410, 5, 77, 0, 0, 2410, 2411, 5, 79, 
	    0, 0, 2411, 2412, 5, 78, 0, 0, 2412, 2413, 5, 84, 0, 0, 2413, 2414, 
	    5, 72, 0, 0, 2414, 416, 1, 0, 0, 0, 2415, 2416, 5, 77, 0, 0, 2416, 
	    2417, 5, 79, 0, 0, 2417, 2418, 5, 78, 0, 0, 2418, 2419, 5, 84, 0, 0, 
	    2419, 2420, 5, 72, 0, 0, 2420, 2421, 5, 83, 0, 0, 2421, 418, 1, 0, 
	    0, 0, 2422, 2423, 5, 77, 0, 0, 2423, 2424, 5, 83, 0, 0, 2424, 2425, 
	    5, 67, 0, 0, 2425, 2426, 5, 75, 0, 0, 2426, 420, 1, 0, 0, 0, 2427, 
	    2428, 5, 78, 0, 0, 2428, 2429, 5, 65, 0, 0, 2429, 2430, 5, 77, 0, 0, 
	    2430, 2431, 5, 69, 0, 0, 2431, 422, 1, 0, 0, 0, 2432, 2433, 5, 78, 
	    0, 0, 2433, 2434, 5, 65, 0, 0, 2434, 2435, 5, 77, 0, 0, 2435, 2436, 
	    5, 69, 0, 0, 2436, 2437, 5, 83, 0, 0, 2437, 2438, 5, 80, 0, 0, 2438, 
	    2439, 5, 65, 0, 0, 2439, 2440, 5, 67, 0, 0, 2440, 2441, 5, 69, 0, 0, 
	    2441, 424, 1, 0, 0, 0, 2442, 2443, 5, 78, 0, 0, 2443, 2444, 5, 65, 
	    0, 0, 2444, 2445, 5, 77, 0, 0, 2445, 2446, 5, 69, 0, 0, 2446, 2447, 
	    5, 83, 0, 0, 2447, 2448, 5, 80, 0, 0, 2448, 2449, 5, 65, 0, 0, 2449, 
	    2450, 5, 67, 0, 0, 2450, 2451, 5, 69, 0, 0, 2451, 2452, 5, 83, 0, 0, 
	    2452, 426, 1, 0, 0, 0, 2453, 2454, 5, 78, 0, 0, 2454, 2455, 5, 65, 
	    0, 0, 2455, 2456, 5, 77, 0, 0, 2456, 2457, 5, 69, 0, 0, 2457, 2458, 
	    5, 68, 0, 0, 2458, 2459, 5, 95, 0, 0, 2459, 2460, 5, 83, 0, 0, 2460, 
	    2461, 5, 84, 0, 0, 2461, 2462, 5, 82, 0, 0, 2462, 2463, 5, 85, 0, 0, 
	    2463, 2464, 5, 67, 0, 0, 2464, 2465, 5, 84, 0, 0, 2465, 428, 1, 0, 
	    0, 0, 2466, 2467, 5, 78, 0, 0, 2467, 2468, 5, 65, 0, 0, 2468, 2469, 
	    5, 78, 0, 0, 2469, 2470, 5, 79, 0, 0, 2470, 2471, 5, 83, 0, 0, 2471, 
	    2472, 5, 69, 0, 0, 2472, 2473, 5, 67, 0, 0, 2473, 2474, 5, 79, 0, 0, 
	    2474, 2475, 5, 78, 0, 0, 2475, 2476, 5, 68, 0, 0, 2476, 430, 1, 0, 
	    0, 0, 2477, 2478, 5, 78, 0, 0, 2478, 2479, 5, 65, 0, 0, 2479, 2480, 
	    5, 78, 0, 0, 2480, 2481, 5, 79, 0, 0, 2481, 2482, 5, 83, 0, 0, 2482, 
	    2483, 5, 69, 0, 0, 2483, 2484, 5, 67, 0, 0, 2484, 2485, 5, 79, 0, 0, 
	    2485, 2486, 5, 78, 0, 0, 2486, 2487, 5, 68, 0, 0, 2487, 2488, 5, 83, 
	    0, 0, 2488, 432, 1, 0, 0, 0, 2489, 2490, 5, 78, 0, 0, 2490, 2491, 5, 
	    65, 0, 0, 2491, 2492, 5, 84, 0, 0, 2492, 2493, 5, 85, 0, 0, 2493, 2494, 
	    5, 82, 0, 0, 2494, 2495, 5, 65, 0, 0, 2495, 2496, 5, 76, 0, 0, 2496, 
	    434, 1, 0, 0, 0, 2497, 2498, 5, 78, 0, 0, 2498, 2499, 5, 79, 0, 0, 
	    2499, 436, 1, 0, 0, 0, 2500, 2501, 5, 78, 0, 0, 2501, 2502, 5, 79, 
	    0, 0, 2502, 2503, 5, 78, 0, 0, 2503, 2504, 5, 69, 0, 0, 2504, 438, 
	    1, 0, 0, 0, 2505, 2506, 5, 78, 0, 0, 2506, 2507, 5, 79, 0, 0, 2507, 
	    2508, 5, 84, 0, 0, 2508, 440, 1, 0, 0, 0, 2509, 2510, 5, 78, 0, 0, 
	    2510, 2511, 5, 85, 0, 0, 2511, 2512, 5, 76, 0, 0, 2512, 2513, 5, 76, 
	    0, 0, 2513, 442, 1, 0, 0, 0, 2514, 2515, 5, 78, 0, 0, 2515, 2516, 5, 
	    85, 0, 0, 2516, 2517, 5, 76, 0, 0, 2517, 2518, 5, 76, 0, 0, 2518, 2519, 
	    5, 83, 0, 0, 2519, 444, 1, 0, 0, 0, 2520, 2521, 5, 78, 0, 0, 2521, 
	    2522, 5, 85, 0, 0, 2522, 2523, 5, 77, 0, 0, 2523, 2524, 5, 69, 0, 0, 
	    2524, 2525, 5, 82, 0, 0, 2525, 2526, 5, 73, 0, 0, 2526, 2527, 5, 67, 
	    0, 0, 2527, 446, 1, 0, 0, 0, 2528, 2529, 5, 79, 0, 0, 2529, 2530, 5, 
	    70, 0, 0, 2530, 448, 1, 0, 0, 0, 2531, 2532, 5, 79, 0, 0, 2532, 2533, 
	    5, 70, 0, 0, 2533, 2534, 5, 70, 0, 0, 2534, 2535, 5, 83, 0, 0, 2535, 
	    2536, 5, 69, 0, 0, 2536, 2537, 5, 84, 0, 0, 2537, 450, 1, 0, 0, 0, 
	    2538, 2539, 5, 79, 0, 0, 2539, 2540, 5, 78, 0, 0, 2540, 452, 1, 0, 
	    0, 0, 2541, 2542, 5, 79, 0, 0, 2542, 2543, 5, 78, 0, 0, 2543, 2544, 
	    5, 76, 0, 0, 2544, 2545, 5, 89, 0, 0, 2545, 454, 1, 0, 0, 0, 2546, 
	    2547, 5, 79, 0, 0, 2547, 2548, 5, 80, 0, 0, 2548, 2549, 5, 84, 0, 0, 
	    2549, 2550, 5, 73, 0, 0, 2550, 2551, 5, 77, 0, 0, 2551, 2552, 5, 73, 
	    0, 0, 2552, 2553, 5, 90, 0, 0, 2553, 2554, 5, 69, 0, 0, 2554, 456, 
	    1, 0, 0, 0, 2555, 2556, 5, 79, 0, 0, 2556, 2557, 5, 80, 0, 0, 2557, 
	    2558, 5, 84, 0, 0, 2558, 2559, 5, 73, 0, 0, 2559, 2560, 5, 79, 0, 0, 
	    2560, 2561, 5, 78, 0, 0, 2561, 458, 1, 0, 0, 0, 2562, 2563, 5, 79, 
	    0, 0, 2563, 2564, 5, 80, 0, 0, 2564, 2565, 5, 84, 0, 0, 2565, 2566, 
	    5, 73, 0, 0, 2566, 2567, 5, 79, 0, 0, 2567, 2568, 5, 78, 0, 0, 2568, 
	    2569, 5, 83, 0, 0, 2569, 460, 1, 0, 0, 0, 2570, 2571, 5, 79, 0, 0, 
	    2571, 2572, 5, 82, 0, 0, 2572, 462, 1, 0, 0, 0, 2573, 2574, 5, 79, 
	    0, 0, 2574, 2575, 5, 82, 0, 0, 2575, 2576, 5, 68, 0, 0, 2576, 2577, 
	    5, 69, 0, 0, 2577, 2578, 5, 82, 0, 0, 2578, 464, 1, 0, 0, 0, 2579, 
	    2580, 5, 79, 0, 0, 2580, 2581, 5, 85, 0, 0, 2581, 2582, 5, 84, 0, 0, 
	    2582, 466, 1, 0, 0, 0, 2583, 2584, 5, 79, 0, 0, 2584, 2585, 5, 85, 
	    0, 0, 2585, 2586, 5, 84, 0, 0, 2586, 2587, 5, 69, 0, 0, 2587, 2588, 
	    5, 82, 0, 0, 2588, 468, 1, 0, 0, 0, 2589, 2590, 5, 79, 0, 0, 2590, 
	    2591, 5, 85, 0, 0, 2591, 2592, 5, 84, 0, 0, 2592, 2593, 5, 80, 0, 0, 
	    2593, 2594, 5, 85, 0, 0, 2594, 2595, 5, 84, 0, 0, 2595, 2596, 5, 70, 
	    0, 0, 2596, 2597, 5, 79, 0, 0, 2597, 2598, 5, 82, 0, 0, 2598, 2599, 
	    5, 77, 0, 0, 2599, 2600, 5, 65, 0, 0, 2600, 2601, 5, 84, 0, 0, 2601, 
	    470, 1, 0, 0, 0, 2602, 2603, 5, 79, 0, 0, 2603, 2604, 5, 86, 0, 0, 
	    2604, 2605, 5, 69, 0, 0, 2605, 2606, 5, 82, 0, 0, 2606, 472, 1, 0, 
	    0, 0, 2607, 2608, 5, 79, 0, 0, 2608, 2609, 5, 86, 0, 0, 2609, 2610, 
	    5, 69, 0, 0, 2610, 2611, 5, 82, 0, 0, 2611, 2612, 5, 76, 0, 0, 2612, 
	    2613, 5, 65, 0, 0, 2613, 2614, 5, 80, 0, 0, 2614, 2615, 5, 83, 0, 0, 
	    2615, 474, 1, 0, 0, 0, 2616, 2617, 5, 79, 0, 0, 2617, 2618, 5, 86, 
	    0, 0, 2618, 2619, 5, 69, 0, 0, 2619, 2620, 5, 82, 0, 0, 2620, 2621, 
	    5, 76, 0, 0, 2621, 2622, 5, 65, 0, 0, 2622, 2623, 5, 89, 0, 0, 2623, 
	    476, 1, 0, 0, 0, 2624, 2625, 5, 79, 0, 0, 2625, 2626, 5, 86, 0, 0, 
	    2626, 2627, 5, 69, 0, 0, 2627, 2628, 5, 82, 0, 0, 2628, 2629, 5, 87, 
	    0, 0, 2629, 2630, 5, 82, 0, 0, 2630, 2631, 5, 73, 0, 0, 2631, 2632, 
	    5, 84, 0, 0, 2632, 2633, 5, 69, 0, 0, 2633, 478, 1, 0, 0, 0, 2634, 
	    2635, 5, 80, 0, 0, 2635, 2636, 5, 65, 0, 0, 2636, 2637, 5, 82, 0, 0, 
	    2637, 2638, 5, 84, 0, 0, 2638, 2639, 5, 73, 0, 0, 2639, 2640, 5, 84, 
	    0, 0, 2640, 2641, 5, 73, 0, 0, 2641, 2642, 5, 79, 0, 0, 2642, 2643, 
	    5, 78, 0, 0, 2643, 480, 1, 0, 0, 0, 2644, 2645, 5, 80, 0, 0, 2645, 
	    2646, 5, 65, 0, 0, 2646, 2647, 5, 82, 0, 0, 2647, 2648, 5, 84, 0, 0, 
	    2648, 2649, 5, 73, 0, 0, 2649, 2650, 5, 84, 0, 0, 2650, 2651, 5, 73, 
	    0, 0, 2651, 2652, 5, 79, 0, 0, 2652, 2653, 5, 78, 0, 0, 2653, 2654, 
	    5, 69, 0, 0, 2654, 2655, 5, 68, 0, 0, 2655, 482, 1, 0, 0, 0, 2656, 
	    2657, 5, 80, 0, 0, 2657, 2658, 5, 65, 0, 0, 2658, 2659, 5, 82, 0, 0, 
	    2659, 2660, 5, 84, 0, 0, 2660, 2661, 5, 73, 0, 0, 2661, 2662, 5, 84, 
	    0, 0, 2662, 2663, 5, 73, 0, 0, 2663, 2664, 5, 79, 0, 0, 2664, 2665, 
	    5, 78, 0, 0, 2665, 2666, 5, 83, 0, 0, 2666, 484, 1, 0, 0, 0, 2667, 
	    2668, 5, 80, 0, 0, 2668, 2669, 5, 69, 0, 0, 2669, 2670, 5, 82, 0, 0, 
	    2670, 2671, 5, 67, 0, 0, 2671, 2672, 5, 69, 0, 0, 2672, 2673, 5, 78, 
	    0, 0, 2673, 2674, 5, 84, 0, 0, 2674, 486, 1, 0, 0, 0, 2675, 2676, 5, 
	    80, 0, 0, 2676, 2677, 5, 69, 0, 0, 2677, 2678, 5, 82, 0, 0, 2678, 2679, 
	    5, 67, 0, 0, 2679, 2680, 5, 69, 0, 0, 2680, 2681, 5, 78, 0, 0, 2681, 
	    2682, 5, 84, 0, 0, 2682, 2683, 5, 73, 0, 0, 2683, 2684, 5, 76, 0, 0, 
	    2684, 2685, 5, 69, 0, 0, 2685, 2686, 5, 95, 0, 0, 2686, 2687, 5, 67, 
	    0, 0, 2687, 2688, 5, 79, 0, 0, 2688, 2689, 5, 78, 0, 0, 2689, 2690, 
	    5, 84, 0, 0, 2690, 488, 1, 0, 0, 0, 2691, 2692, 5, 80, 0, 0, 2692, 
	    2693, 5, 69, 0, 0, 2693, 2694, 5, 82, 0, 0, 2694, 2695, 5, 67, 0, 0, 
	    2695, 2696, 5, 69, 0, 0, 2696, 2697, 5, 78, 0, 0, 2697, 2698, 5, 84, 
	    0, 0, 2698, 2699, 5, 73, 0, 0, 2699, 2700, 5, 76, 0, 0, 2700, 2701, 
	    5, 69, 0, 0, 2701, 2702, 5, 95, 0, 0, 2702, 2703, 5, 68, 0, 0, 2703, 
	    2704, 5, 73, 0, 0, 2704, 2705, 5, 83, 0, 0, 2705, 2706, 5, 67, 0, 0, 
	    2706, 490, 1, 0, 0, 0, 2707, 2708, 5, 80, 0, 0, 2708, 2709, 5, 73, 
	    0, 0, 2709, 2710, 5, 86, 0, 0, 2710, 2711, 5, 79, 0, 0, 2711, 2712, 
	    5, 84, 0, 0, 2712, 492, 1, 0, 0, 0, 2713, 2714, 5, 80, 0, 0, 2714, 
	    2715, 5, 76, 0, 0, 2715, 2716, 5, 65, 0, 0, 2716, 2717, 5, 67, 0, 0, 
	    2717, 2718, 5, 73, 0, 0, 2718, 2719, 5, 78, 0, 0, 2719, 2720, 5, 71, 
	    0, 0, 2720, 494, 1, 0, 0, 0, 2721, 2722, 5, 80, 0, 0, 2722, 2723, 5, 
	    79, 0, 0, 2723, 2724, 5, 83, 0, 0, 2724, 2725, 5, 73, 0, 0, 2725, 2726, 
	    5, 84, 0, 0, 2726, 2727, 5, 73, 0, 0, 2727, 2728, 5, 79, 0, 0, 2728, 
	    2729, 5, 78, 0, 0, 2729, 496, 1, 0, 0, 0, 2730, 2731, 5, 80, 0, 0, 
	    2731, 2732, 5, 82, 0, 0, 2732, 2733, 5, 69, 0, 0, 2733, 2734, 5, 67, 
	    0, 0, 2734, 2735, 5, 69, 0, 0, 2735, 2736, 5, 68, 0, 0, 2736, 2737, 
	    5, 73, 0, 0, 2737, 2738, 5, 78, 0, 0, 2738, 2739, 5, 71, 0, 0, 2739, 
	    498, 1, 0, 0, 0, 2740, 2741, 5, 80, 0, 0, 2741, 2742, 5, 82, 0, 0, 
	    2742, 2743, 5, 73, 0, 0, 2743, 2744, 5, 77, 0, 0, 2744, 2745, 5, 65, 
	    0, 0, 2745, 2746, 5, 82, 0, 0, 2746, 2747, 5, 89, 0, 0, 2747, 500, 
	    1, 0, 0, 0, 2748, 2749, 5, 80, 0, 0, 2749, 2750, 5, 82, 0, 0, 2750, 
	    2751, 5, 73, 0, 0, 2751, 2752, 5, 78, 0, 0, 2752, 2753, 5, 67, 0, 0, 
	    2753, 2754, 5, 73, 0, 0, 2754, 2755, 5, 80, 0, 0, 2755, 2756, 5, 65, 
	    0, 0, 2756, 2757, 5, 76, 0, 0, 2757, 2758, 5, 83, 0, 0, 2758, 502, 
	    1, 0, 0, 0, 2759, 2760, 5, 80, 0, 0, 2760, 2761, 5, 82, 0, 0, 2761, 
	    2762, 5, 79, 0, 0, 2762, 2763, 5, 80, 0, 0, 2763, 2764, 5, 69, 0, 0, 
	    2764, 2765, 5, 82, 0, 0, 2765, 2766, 5, 84, 0, 0, 2766, 2767, 5, 73, 
	    0, 0, 2767, 2768, 5, 69, 0, 0, 2768, 2769, 5, 83, 0, 0, 2769, 504, 
	    1, 0, 0, 0, 2770, 2771, 5, 80, 0, 0, 2771, 2772, 5, 82, 0, 0, 2772, 
	    2773, 5, 85, 0, 0, 2773, 2774, 5, 78, 0, 0, 2774, 2775, 5, 69, 0, 0, 
	    2775, 506, 1, 0, 0, 0, 2776, 2777, 5, 80, 0, 0, 2777, 2778, 5, 85, 
	    0, 0, 2778, 2779, 5, 82, 0, 0, 2779, 2780, 5, 71, 0, 0, 2780, 2781, 
	    5, 69, 0, 0, 2781, 508, 1, 0, 0, 0, 2782, 2783, 5, 81, 0, 0, 2783, 
	    2784, 5, 85, 0, 0, 2784, 2785, 5, 65, 0, 0, 2785, 2786, 5, 76, 0, 0, 
	    2786, 2787, 5, 73, 0, 0, 2787, 2788, 5, 70, 0, 0, 2788, 2789, 5, 89, 
	    0, 0, 2789, 510, 1, 0, 0, 0, 2790, 2791, 5, 81, 0, 0, 2791, 2792, 5, 
	    85, 0, 0, 2792, 2793, 5, 65, 0, 0, 2793, 2794, 5, 82, 0, 0, 2794, 2795, 
	    5, 84, 0, 0, 2795, 2796, 5, 69, 0, 0, 2796, 2797, 5, 82, 0, 0, 2797, 
	    512, 1, 0, 0, 0, 2798, 2799, 5, 81, 0, 0, 2799, 2800, 5, 85, 0, 0, 
	    2800, 2801, 5, 69, 0, 0, 2801, 2802, 5, 82, 0, 0, 2802, 2803, 5, 89, 
	    0, 0, 2803, 514, 1, 0, 0, 0, 2804, 2805, 5, 82, 0, 0, 2805, 2806, 5, 
	    65, 0, 0, 2806, 2807, 5, 78, 0, 0, 2807, 2808, 5, 71, 0, 0, 2808, 2809, 
	    5, 69, 0, 0, 2809, 516, 1, 0, 0, 0, 2810, 2811, 5, 82, 0, 0, 2811, 
	    2812, 5, 69, 0, 0, 2812, 2813, 5, 65, 0, 0, 2813, 2814, 5, 68, 0, 0, 
	    2814, 2815, 5, 83, 0, 0, 2815, 518, 1, 0, 0, 0, 2816, 2817, 5, 82, 
	    0, 0, 2817, 2818, 5, 69, 0, 0, 2818, 2819, 5, 65, 0, 0, 2819, 2820, 
	    5, 76, 0, 0, 2820, 520, 1, 0, 0, 0, 2821, 2822, 5, 82, 0, 0, 2822, 
	    2823, 5, 69, 0, 0, 2823, 2824, 5, 67, 0, 0, 2824, 2825, 5, 79, 0, 0, 
	    2825, 2826, 5, 82, 0, 0, 2826, 2827, 5, 68, 0, 0, 2827, 2828, 5, 82, 
	    0, 0, 2828, 2829, 5, 69, 0, 0, 2829, 2830, 5, 65, 0, 0, 2830, 2831, 
	    5, 68, 0, 0, 2831, 2832, 5, 69, 0, 0, 2832, 2833, 5, 82, 0, 0, 2833, 
	    522, 1, 0, 0, 0, 2834, 2835, 5, 82, 0, 0, 2835, 2836, 5, 69, 0, 0, 
	    2836, 2837, 5, 67, 0, 0, 2837, 2838, 5, 79, 0, 0, 2838, 2839, 5, 82, 
	    0, 0, 2839, 2840, 5, 68, 0, 0, 2840, 2841, 5, 87, 0, 0, 2841, 2842, 
	    5, 82, 0, 0, 2842, 2843, 5, 73, 0, 0, 2843, 2844, 5, 84, 0, 0, 2844, 
	    2845, 5, 69, 0, 0, 2845, 2846, 5, 82, 0, 0, 2846, 524, 1, 0, 0, 0, 
	    2847, 2848, 5, 82, 0, 0, 2848, 2849, 5, 69, 0, 0, 2849, 2850, 5, 67, 
	    0, 0, 2850, 2851, 5, 79, 0, 0, 2851, 2852, 5, 86, 0, 0, 2852, 2853, 
	    5, 69, 0, 0, 2853, 2854, 5, 82, 0, 0, 2854, 526, 1, 0, 0, 0, 2855, 
	    2856, 5, 82, 0, 0, 2856, 2857, 5, 69, 0, 0, 2857, 2858, 5, 67, 0, 0, 
	    2858, 2859, 5, 85, 0, 0, 2859, 2860, 5, 82, 0, 0, 2860, 2861, 5, 83, 
	    0, 0, 2861, 2862, 5, 73, 0, 0, 2862, 2863, 5, 86, 0, 0, 2863, 2864, 
	    5, 69, 0, 0, 2864, 528, 1, 0, 0, 0, 2865, 2866, 5, 82, 0, 0, 2866, 
	    2867, 5, 69, 0, 0, 2867, 2868, 5, 68, 0, 0, 2868, 2869, 5, 85, 0, 0, 
	    2869, 2870, 5, 67, 0, 0, 2870, 2871, 5, 69, 0, 0, 2871, 530, 1, 0, 
	    0, 0, 2872, 2873, 5, 82, 0, 0, 2873, 2874, 5, 69, 0, 0, 2874, 2875, 
	    5, 71, 0, 0, 2875, 2876, 5, 69, 0, 0, 2876, 2877, 5, 88, 0, 0, 2877, 
	    2878, 5, 80, 0, 0, 2878, 532, 1, 0, 0, 0, 2879, 2880, 5, 82, 0, 0, 
	    2880, 2881, 5, 69, 0, 0, 2881, 2882, 5, 70, 0, 0, 2882, 2883, 5, 69, 
	    0, 0, 2883, 2884, 5, 82, 0, 0, 2884, 2885, 5, 69, 0, 0, 2885, 2886, 
	    5, 78, 0, 0, 2886, 2887, 5, 67, 0, 0, 2887, 2888, 5, 69, 0, 0, 2888, 
	    534, 1, 0, 0, 0, 2889, 2890, 5, 82, 0, 0, 2890, 2891, 5, 69, 0, 0, 
	    2891, 2892, 5, 70, 0, 0, 2892, 2893, 5, 69, 0, 0, 2893, 2894, 5, 82, 
	    0, 0, 2894, 2895, 5, 69, 0, 0, 2895, 2896, 5, 78, 0, 0, 2896, 2897, 
	    5, 67, 0, 0, 2897, 2898, 5, 69, 0, 0, 2898, 2899, 5, 83, 0, 0, 2899, 
	    536, 1, 0, 0, 0, 2900, 2901, 5, 82, 0, 0, 2901, 2902, 5, 69, 0, 0, 
	    2902, 2903, 5, 70, 0, 0, 2903, 2904, 5, 82, 0, 0, 2904, 2905, 5, 69, 
	    0, 0, 2905, 2906, 5, 83, 0, 0, 2906, 2907, 5, 72, 0, 0, 2907, 538, 
	    1, 0, 0, 0, 2908, 2909, 5, 82, 0, 0, 2909, 2910, 5, 69, 0, 0, 2910, 
	    2911, 5, 78, 0, 0, 2911, 2912, 5, 65, 0, 0, 2912, 2913, 5, 77, 0, 0, 
	    2913, 2914, 5, 69, 0, 0, 2914, 540, 1, 0, 0, 0, 2915, 2916, 5, 82, 
	    0, 0, 2916, 2917, 5, 69, 0, 0, 2917, 2918, 5, 80, 0, 0, 2918, 2919, 
	    5, 65, 0, 0, 2919, 2920, 5, 73, 0, 0, 2920, 2921, 5, 82, 0, 0, 2921, 
	    542, 1, 0, 0, 0, 2922, 2923, 5, 82, 0, 0, 2923, 2924, 5, 69, 0, 0, 
	    2924, 2925, 5, 80, 0, 0, 2925, 2926, 5, 69, 0, 0, 2926, 2927, 5, 65, 
	    0, 0, 2927, 2928, 5, 84, 0, 0, 2928, 2929, 5, 65, 0, 0, 2929, 2930, 
	    5, 66, 0, 0, 2930, 2931, 5, 76, 0, 0, 2931, 2932, 5, 69, 0, 0, 2932, 
	    544, 1, 0, 0, 0, 2933, 2934, 5, 82, 0, 0, 2934, 2935, 5, 69, 0, 0, 
	    2935, 2936, 5, 80, 0, 0, 2936, 2937, 5, 76, 0, 0, 2937, 2938, 5, 65, 
	    0, 0, 2938, 2939, 5, 67, 0, 0, 2939, 2940, 5, 69, 0, 0, 2940, 546, 
	    1, 0, 0, 0, 2941, 2942, 5, 82, 0, 0, 2942, 2943, 5, 69, 0, 0, 2943, 
	    2944, 5, 83, 0, 0, 2944, 2945, 5, 69, 0, 0, 2945, 2946, 5, 84, 0, 0, 
	    2946, 548, 1, 0, 0, 0, 2947, 2948, 5, 82, 0, 0, 2948, 2949, 5, 69, 
	    0, 0, 2949, 2950, 5, 83, 0, 0, 2950, 2951, 5, 80, 0, 0, 2951, 2952, 
	    5, 69, 0, 0, 2952, 2953, 5, 67, 0, 0, 2953, 2954, 5, 84, 0, 0, 2954, 
	    550, 1, 0, 0, 0, 2955, 2956, 5, 82, 0, 0, 2956, 2957, 5, 69, 0, 0, 
	    2957, 2958, 5, 83, 0, 0, 2958, 2959, 5, 84, 0, 0, 2959, 2960, 5, 82, 
	    0, 0, 2960, 2961, 5, 73, 0, 0, 2961, 2962, 5, 67, 0, 0, 2962, 2963, 
	    5, 84, 0, 0, 2963, 552, 1, 0, 0, 0, 2964, 2965, 5, 82, 0, 0, 2965, 
	    2966, 5, 69, 0, 0, 2966, 2967, 5, 84, 0, 0, 2967, 2968, 5, 85, 0, 0, 
	    2968, 2969, 5, 82, 0, 0, 2969, 2970, 5, 78, 0, 0, 2970, 554, 1, 0, 
	    0, 0, 2971, 2972, 5, 82, 0, 0, 2972, 2973, 5, 69, 0, 0, 2973, 2974, 
	    5, 84, 0, 0, 2974, 2975, 5, 85, 0, 0, 2975, 2976, 5, 82, 0, 0, 2976, 
	    2977, 5, 78, 0, 0, 2977, 2978, 5, 83, 0, 0, 2978, 556, 1, 0, 0, 0, 
	    2979, 2980, 5, 82, 0, 0, 2980, 2981, 5, 69, 0, 0, 2981, 2982, 5, 86, 
	    0, 0, 2982, 2983, 5, 79, 0, 0, 2983, 2984, 5, 75, 0, 0, 2984, 2985, 
	    5, 69, 0, 0, 2985, 558, 1, 0, 0, 0, 2986, 2987, 5, 82, 0, 0, 2987, 
	    2988, 5, 73, 0, 0, 2988, 2989, 5, 71, 0, 0, 2989, 2990, 5, 72, 0, 0, 
	    2990, 2991, 5, 84, 0, 0, 2991, 560, 1, 0, 0, 0, 2992, 2993, 5, 82, 
	    0, 0, 2993, 2994, 5, 76, 0, 0, 2994, 2995, 5, 73, 0, 0, 2995, 2996, 
	    5, 75, 0, 0, 2996, 2997, 5, 69, 0, 0, 2997, 562, 1, 0, 0, 0, 2998, 
	    2999, 5, 82, 0, 0, 2999, 3000, 5, 79, 0, 0, 3000, 3001, 5, 76, 0, 0, 
	    3001, 3002, 5, 69, 0, 0, 3002, 564, 1, 0, 0, 0, 3003, 3004, 5, 82, 
	    0, 0, 3004, 3005, 5, 79, 0, 0, 3005, 3006, 5, 76, 0, 0, 3006, 3007, 
	    5, 69, 0, 0, 3007, 3008, 5, 83, 0, 0, 3008, 566, 1, 0, 0, 0, 3009, 
	    3010, 5, 82, 0, 0, 3010, 3011, 5, 79, 0, 0, 3011, 3012, 5, 76, 0, 0, 
	    3012, 3013, 5, 76, 0, 0, 3013, 3014, 5, 66, 0, 0, 3014, 3015, 5, 65, 
	    0, 0, 3015, 3016, 5, 67, 0, 0, 3016, 3017, 5, 75, 0, 0, 3017, 568, 
	    1, 0, 0, 0, 3018, 3019, 5, 82, 0, 0, 3019, 3020, 5, 79, 0, 0, 3020, 
	    3021, 5, 76, 0, 0, 3021, 3022, 5, 76, 0, 0, 3022, 3023, 5, 85, 0, 0, 
	    3023, 3024, 5, 80, 0, 0, 3024, 570, 1, 0, 0, 0, 3025, 3026, 5, 82, 
	    0, 0, 3026, 3027, 5, 79, 0, 0, 3027, 3028, 5, 87, 0, 0, 3028, 572, 
	    1, 0, 0, 0, 3029, 3030, 5, 82, 0, 0, 3030, 3031, 5, 79, 0, 0, 3031, 
	    3032, 5, 87, 0, 0, 3032, 3033, 5, 83, 0, 0, 3033, 574, 1, 0, 0, 0, 
	    3034, 3035, 5, 83, 0, 0, 3035, 3036, 5, 69, 0, 0, 3036, 3037, 5, 67, 
	    0, 0, 3037, 3038, 5, 79, 0, 0, 3038, 3039, 5, 78, 0, 0, 3039, 3040, 
	    5, 68, 0, 0, 3040, 576, 1, 0, 0, 0, 3041, 3042, 5, 83, 0, 0, 3042, 
	    3043, 5, 69, 0, 0, 3043, 3044, 5, 67, 0, 0, 3044, 3045, 5, 79, 0, 0, 
	    3045, 3046, 5, 78, 0, 0, 3046, 3047, 5, 68, 0, 0, 3047, 3048, 5, 83, 
	    0, 0, 3048, 578, 1, 0, 0, 0, 3049, 3050, 5, 83, 0, 0, 3050, 3051, 5, 
	    67, 0, 0, 3051, 3052, 5, 72, 0, 0, 3052, 3053, 5, 69, 0, 0, 3053, 3054, 
	    5, 77, 0, 0, 3054, 3055, 5, 65, 0, 0, 3055, 580, 1, 0, 0, 0, 3056, 
	    3057, 5, 83, 0, 0, 3057, 3058, 5, 67, 0, 0, 3058, 3059, 5, 72, 0, 0, 
	    3059, 3060, 5, 69, 0, 0, 3060, 3061, 5, 77, 0, 0, 3061, 3062, 5, 65, 
	    0, 0, 3062, 3063, 5, 83, 0, 0, 3063, 582, 1, 0, 0, 0, 3064, 3065, 5, 
	    83, 0, 0, 3065, 3066, 5, 69, 0, 0, 3066, 3067, 5, 67, 0, 0, 3067, 3068, 
	    5, 85, 0, 0, 3068, 3069, 5, 82, 0, 0, 3069, 3070, 5, 73, 0, 0, 3070, 
	    3071, 5, 84, 0, 0, 3071, 3072, 5, 89, 0, 0, 3072, 584, 1, 0, 0, 0, 
	    3073, 3074, 5, 83, 0, 0, 3074, 3075, 5, 69, 0, 0, 3075, 3076, 5, 76, 
	    0, 0, 3076, 3077, 5, 69, 0, 0, 3077, 3078, 5, 67, 0, 0, 3078, 3079, 
	    5, 84, 0, 0, 3079, 586, 1, 0, 0, 0, 3080, 3081, 5, 83, 0, 0, 3081, 
	    3082, 5, 69, 0, 0, 3082, 3083, 5, 77, 0, 0, 3083, 3084, 5, 73, 0, 0, 
	    3084, 588, 1, 0, 0, 0, 3085, 3086, 5, 83, 0, 0, 3086, 3087, 5, 69, 
	    0, 0, 3087, 3088, 5, 80, 0, 0, 3088, 3089, 5, 65, 0, 0, 3089, 3090, 
	    5, 82, 0, 0, 3090, 3091, 5, 65, 0, 0, 3091, 3092, 5, 84, 0, 0, 3092, 
	    3093, 5, 69, 0, 0, 3093, 3094, 5, 68, 0, 0, 3094, 590, 1, 0, 0, 0, 
	    3095, 3096, 5, 83, 0, 0, 3096, 3097, 5, 69, 0, 0, 3097, 3098, 5, 82, 
	    0, 0, 3098, 3099, 5, 68, 0, 0, 3099, 3100, 5, 69, 0, 0, 3100, 592, 
	    1, 0, 0, 0, 3101, 3102, 5, 83, 0, 0, 3102, 3103, 5, 69, 0, 0, 3103, 
	    3104, 5, 82, 0, 0, 3104, 3105, 5, 68, 0, 0, 3105, 3106, 5, 69, 0, 0, 
	    3106, 3107, 5, 80, 0, 0, 3107, 3108, 5, 82, 0, 0, 3108, 3109, 5, 79, 
	    0, 0, 3109, 3110, 5, 80, 0, 0, 3110, 3111, 5, 69, 0, 0, 3111, 3112, 
	    5, 82, 0, 0, 3112, 3113, 5, 84, 0, 0, 3113, 3114, 5, 73, 0, 0, 3114, 
	    3115, 5, 69, 0, 0, 3115, 3116, 5, 83, 0, 0, 3116, 594, 1, 0, 0, 0, 
	    3117, 3118, 5, 83, 0, 0, 3118, 3119, 5, 69, 0, 0, 3119, 3120, 5, 83, 
	    0, 0, 3120, 3121, 5, 83, 0, 0, 3121, 3122, 5, 73, 0, 0, 3122, 3123, 
	    5, 79, 0, 0, 3123, 3124, 5, 78, 0, 0, 3124, 3125, 5, 95, 0, 0, 3125, 
	    3126, 5, 85, 0, 0, 3126, 3127, 5, 83, 0, 0, 3127, 3128, 5, 69, 0, 0, 
	    3128, 3129, 5, 82, 0, 0, 3129, 596, 1, 0, 0, 0, 3130, 3131, 5, 83, 
	    0, 0, 3131, 3132, 5, 69, 0, 0, 3132, 3133, 5, 84, 0, 0, 3133, 598, 
	    1, 0, 0, 0, 3134, 3135, 5, 83, 0, 0, 3135, 3136, 5, 69, 0, 0, 3136, 
	    3137, 5, 84, 0, 0, 3137, 3138, 5, 83, 0, 0, 3138, 600, 1, 0, 0, 0, 
	    3139, 3140, 5, 83, 0, 0, 3140, 3141, 5, 72, 0, 0, 3141, 3142, 5, 79, 
	    0, 0, 3142, 3143, 5, 82, 0, 0, 3143, 3144, 5, 84, 0, 0, 3144, 602, 
	    1, 0, 0, 0, 3145, 3146, 5, 83, 0, 0, 3146, 3147, 5, 72, 0, 0, 3147, 
	    3148, 5, 79, 0, 0, 3148, 3149, 5, 87, 0, 0, 3149, 604, 1, 0, 0, 0, 
	    3150, 3151, 5, 83, 0, 0, 3151, 3152, 5, 73, 0, 0, 3152, 3153, 5, 78, 
	    0, 0, 3153, 3154, 5, 71, 0, 0, 3154, 3155, 5, 76, 0, 0, 3155, 3156, 
	    5, 69, 0, 0, 3156, 606, 1, 0, 0, 0, 3157, 3158, 5, 83, 0, 0, 3158, 
	    3159, 5, 75, 0, 0, 3159, 3160, 5, 69, 0, 0, 3160, 3161, 5, 87, 0, 0, 
	    3161, 3162, 5, 69, 0, 0, 3162, 3163, 5, 68, 0, 0, 3163, 608, 1, 0, 
	    0, 0, 3164, 3165, 5, 83, 0, 0, 3165, 3166, 5, 77, 0, 0, 3166, 3167, 
	    5, 65, 0, 0, 3167, 3168, 5, 76, 0, 0, 3168, 3169, 5, 76, 0, 0, 3169, 
	    3170, 5, 73, 0, 0, 3170, 3171, 5, 78, 0, 0, 3171, 3172, 5, 84, 0, 0, 
	    3172, 610, 1, 0, 0, 0, 3173, 3174, 5, 83, 0, 0, 3174, 3175, 5, 79, 
	    0, 0, 3175, 3176, 5, 77, 0, 0, 3176, 3177, 5, 69, 0, 0, 3177, 612, 
	    1, 0, 0, 0, 3178, 3179, 5, 83, 0, 0, 3179, 3180, 5, 79, 0, 0, 3180, 
	    3181, 5, 82, 0, 0, 3181, 3182, 5, 84, 0, 0, 3182, 614, 1, 0, 0, 0, 
	    3183, 3184, 5, 83, 0, 0, 3184, 3185, 5, 79, 0, 0, 3185, 3186, 5, 82, 
	    0, 0, 3186, 3187, 5, 84, 0, 0, 3187, 3188, 5, 69, 0, 0, 3188, 3189, 
	    5, 68, 0, 0, 3189, 616, 1, 0, 0, 0, 3190, 3191, 5, 83, 0, 0, 3191, 
	    3192, 5, 79, 0, 0, 3192, 3193, 5, 85, 0, 0, 3193, 3194, 5, 82, 0, 0, 
	    3194, 3195, 5, 67, 0, 0, 3195, 3196, 5, 69, 0, 0, 3196, 618, 1, 0, 
	    0, 0, 3197, 3198, 5, 83, 0, 0, 3198, 3199, 5, 80, 0, 0, 3199, 3200, 
	    5, 69, 0, 0, 3200, 3201, 5, 67, 0, 0, 3201, 3202, 5, 73, 0, 0, 3202, 
	    3203, 5, 70, 0, 0, 3203, 3204, 5, 73, 0, 0, 3204, 3205, 5, 67, 0, 0, 
	    3205, 620, 1, 0, 0, 0, 3206, 3207, 5, 83, 0, 0, 3207, 3208, 5, 81, 
	    0, 0, 3208, 3209, 5, 76, 0, 0, 3209, 622, 1, 0, 0, 0, 3210, 3211, 5, 
	    83, 0, 0, 3211, 3212, 5, 84, 0, 0, 3212, 3213, 5, 65, 0, 0, 3213, 3214, 
	    5, 82, 0, 0, 3214, 3215, 5, 84, 0, 0, 3215, 624, 1, 0, 0, 0, 3216, 
	    3217, 5, 83, 0, 0, 3217, 3218, 5, 84, 0, 0, 3218, 3219, 5, 65, 0, 0, 
	    3219, 3220, 5, 84, 0, 0, 3220, 3221, 5, 73, 0, 0, 3221, 3222, 5, 83, 
	    0, 0, 3222, 3223, 5, 84, 0, 0, 3223, 3224, 5, 73, 0, 0, 3224, 3225, 
	    5, 67, 0, 0, 3225, 3226, 5, 83, 0, 0, 3226, 626, 1, 0, 0, 0, 3227, 
	    3228, 5, 83, 0, 0, 3228, 3229, 5, 84, 0, 0, 3229, 3230, 5, 79, 0, 0, 
	    3230, 3231, 5, 82, 0, 0, 3231, 3232, 5, 69, 0, 0, 3232, 3233, 5, 68, 
	    0, 0, 3233, 628, 1, 0, 0, 0, 3234, 3235, 5, 83, 0, 0, 3235, 3236, 5, 
	    84, 0, 0, 3236, 3237, 5, 82, 0, 0, 3237, 3238, 5, 65, 0, 0, 3238, 3239, 
	    5, 84, 0, 0, 3239, 3240, 5, 73, 0, 0, 3240, 3241, 5, 70, 0, 0, 3241, 
	    3242, 5, 89, 0, 0, 3242, 630, 1, 0, 0, 0, 3243, 3244, 5, 83, 0, 0, 
	    3244, 3245, 5, 84, 0, 0, 3245, 3246, 5, 82, 0, 0, 3246, 3247, 5, 69, 
	    0, 0, 3247, 3248, 5, 65, 0, 0, 3248, 3249, 5, 77, 0, 0, 3249, 632, 
	    1, 0, 0, 0, 3250, 3251, 5, 83, 0, 0, 3251, 3252, 5, 84, 0, 0, 3252, 
	    3253, 5, 82, 0, 0, 3253, 3254, 5, 69, 0, 0, 3254, 3255, 5, 65, 0, 0, 
	    3255, 3256, 5, 77, 0, 0, 3256, 3257, 5, 73, 0, 0, 3257, 3258, 5, 78, 
	    0, 0, 3258, 3259, 5, 71, 0, 0, 3259, 634, 1, 0, 0, 0, 3260, 3261, 5, 
	    83, 0, 0, 3261, 3262, 5, 84, 0, 0, 3262, 3263, 5, 82, 0, 0, 3263, 3264, 
	    5, 73, 0, 0, 3264, 3265, 5, 78, 0, 0, 3265, 3266, 5, 71, 0, 0, 3266, 
	    3267, 5, 95, 0, 0, 3267, 3268, 5, 65, 0, 0, 3268, 3269, 5, 71, 0, 0, 
	    3269, 3270, 5, 71, 0, 0, 3270, 636, 1, 0, 0, 0, 3271, 3272, 5, 83, 
	    0, 0, 3272, 3273, 5, 84, 0, 0, 3273, 3274, 5, 82, 0, 0, 3274, 3275, 
	    5, 85, 0, 0, 3275, 3276, 5, 67, 0, 0, 3276, 3277, 5, 84, 0, 0, 3277, 
	    638, 1, 0, 0, 0, 3278, 3279, 5, 83, 0, 0, 3279, 3280, 5, 85, 0, 0, 
	    3280, 3281, 5, 66, 0, 0, 3281, 3282, 5, 83, 0, 0, 3282, 3283, 5, 84, 
	    0, 0, 3283, 3284, 5, 82, 0, 0, 3284, 640, 1, 0, 0, 0, 3285, 3286, 5, 
	    83, 0, 0, 3286, 3287, 5, 85, 0, 0, 3287, 3288, 5, 66, 0, 0, 3288, 3289, 
	    5, 83, 0, 0, 3289, 3290, 5, 84, 0, 0, 3290, 3291, 5, 82, 0, 0, 3291, 
	    3292, 5, 73, 0, 0, 3292, 3293, 5, 78, 0, 0, 3293, 3294, 5, 71, 0, 0, 
	    3294, 642, 1, 0, 0, 0, 3295, 3296, 5, 83, 0, 0, 3296, 3297, 5, 89, 
	    0, 0, 3297, 3298, 5, 78, 0, 0, 3298, 3299, 5, 67, 0, 0, 3299, 644, 
	    1, 0, 0, 0, 3300, 3301, 5, 83, 0, 0, 3301, 3302, 5, 89, 0, 0, 3302, 
	    3303, 5, 83, 0, 0, 3303, 3304, 5, 84, 0, 0, 3304, 3305, 5, 69, 0, 0, 
	    3305, 3306, 5, 77, 0, 0, 3306, 3307, 5, 95, 0, 0, 3307, 3308, 5, 84, 
	    0, 0, 3308, 3309, 5, 73, 0, 0, 3309, 3310, 5, 77, 0, 0, 3310, 3311, 
	    5, 69, 0, 0, 3311, 646, 1, 0, 0, 0, 3312, 3313, 5, 83, 0, 0, 3313, 
	    3314, 5, 89, 0, 0, 3314, 3315, 5, 83, 0, 0, 3315, 3316, 5, 84, 0, 0, 
	    3316, 3317, 5, 69, 0, 0, 3317, 3318, 5, 77, 0, 0, 3318, 3319, 5, 95, 
	    0, 0, 3319, 3320, 5, 86, 0, 0, 3320, 3321, 5, 69, 0, 0, 3321, 3322, 
	    5, 82, 0, 0, 3322, 3323, 5, 83, 0, 0, 3323, 3324, 5, 73, 0, 0, 3324, 
	    3325, 5, 79, 0, 0, 3325, 3326, 5, 78, 0, 0, 3326, 648, 1, 0, 0, 0, 
	    3327, 3328, 5, 84, 0, 0, 3328, 3329, 5, 65, 0, 0, 3329, 3330, 5, 66, 
	    0, 0, 3330, 3331, 5, 76, 0, 0, 3331, 3332, 5, 69, 0, 0, 3332, 650, 
	    1, 0, 0, 0, 3333, 3334, 5, 84, 0, 0, 3334, 3335, 5, 65, 0, 0, 3335, 
	    3336, 5, 66, 0, 0, 3336, 3337, 5, 76, 0, 0, 3337, 3338, 5, 69, 0, 0, 
	    3338, 3339, 5, 83, 0, 0, 3339, 652, 1, 0, 0, 0, 3340, 3341, 5, 84, 
	    0, 0, 3341, 3342, 5, 65, 0, 0, 3342, 3343, 5, 66, 0, 0, 3343, 3344, 
	    5, 76, 0, 0, 3344, 3345, 5, 69, 0, 0, 3345, 3346, 5, 83, 0, 0, 3346, 
	    3347, 5, 65, 0, 0, 3347, 3348, 5, 77, 0, 0, 3348, 3349, 5, 80, 0, 0, 
	    3349, 3350, 5, 76, 0, 0, 3350, 3351, 5, 69, 0, 0, 3351, 654, 1, 0, 
	    0, 0, 3352, 3353, 5, 84, 0, 0, 3353, 3354, 5, 65, 0, 0, 3354, 3355, 
	    5, 82, 0, 0, 3355, 3356, 5, 71, 0, 0, 3356, 3357, 5, 69, 0, 0, 3357, 
	    3358, 5, 84, 0, 0, 3358, 656, 1, 0, 0, 0, 3359, 3360, 5, 84, 0, 0, 
	    3360, 3361, 5, 66, 0, 0, 3361, 3362, 5, 76, 0, 0, 3362, 3363, 5, 80, 
	    0, 0, 3363, 3364, 5, 82, 0, 0, 3364, 3365, 5, 79, 0, 0, 3365, 3366, 
	    5, 80, 0, 0, 3366, 3367, 5, 69, 0, 0, 3367, 3368, 5, 82, 0, 0, 3368, 
	    3369, 5, 84, 0, 0, 3369, 3370, 5, 73, 0, 0, 3370, 3371, 5, 69, 0, 0, 
	    3371, 3372, 5, 83, 0, 0, 3372, 658, 1, 0, 0, 0, 3373, 3374, 5, 84, 
	    0, 0, 3374, 3375, 5, 69, 0, 0, 3375, 3376, 5, 77, 0, 0, 3376, 3377, 
	    5, 80, 0, 0, 3377, 660, 1, 0, 0, 0, 3378, 3379, 5, 84, 0, 0, 3379, 
	    3380, 5, 69, 0, 0, 3380, 3381, 5, 77, 0, 0, 3381, 3382, 5, 80, 0, 0, 
	    3382, 3383, 5, 79, 0, 0, 3383, 3384, 5, 82, 0, 0, 3384, 3385, 5, 65, 
	    0, 0, 3385, 3386, 5, 82, 0, 0, 3386, 3387, 5, 89, 0, 0, 3387, 662, 
	    1, 0, 0, 0, 3388, 3389, 5, 84, 0, 0, 3389, 3390, 5, 69, 0, 0, 3390, 
	    3391, 5, 82, 0, 0, 3391, 3392, 5, 77, 0, 0, 3392, 3393, 5, 73, 0, 0, 
	    3393, 3394, 5, 78, 0, 0, 3394, 3395, 5, 65, 0, 0, 3395, 3396, 5, 84, 
	    0, 0, 3396, 3397, 5, 69, 0, 0, 3397, 3398, 5, 68, 0, 0, 3398, 664, 
	    1, 0, 0, 0, 3399, 3400, 5, 83, 0, 0, 3400, 3401, 5, 84, 0, 0, 3401, 
	    3402, 5, 82, 0, 0, 3402, 3403, 5, 73, 0, 0, 3403, 3404, 5, 78, 0, 0, 
	    3404, 3405, 5, 71, 0, 0, 3405, 666, 1, 0, 0, 0, 3406, 3407, 5, 84, 
	    0, 0, 3407, 3408, 5, 72, 0, 0, 3408, 3409, 5, 69, 0, 0, 3409, 3410, 
	    5, 78, 0, 0, 3410, 668, 1, 0, 0, 0, 3411, 3412, 5, 84, 0, 0, 3412, 
	    3413, 5, 73, 0, 0, 3413, 3414, 5, 77, 0, 0, 3414, 3415, 5, 69, 0, 0, 
	    3415, 670, 1, 0, 0, 0, 3416, 3417, 5, 84, 0, 0, 3417, 3418, 5, 73, 
	    0, 0, 3418, 3419, 5, 77, 0, 0, 3419, 3420, 5, 69, 0, 0, 3420, 3421, 
	    5, 68, 0, 0, 3421, 3422, 5, 73, 0, 0, 3422, 3423, 5, 70, 0, 0, 3423, 
	    3424, 5, 70, 0, 0, 3424, 672, 1, 0, 0, 0, 3425, 3426, 5, 84, 0, 0, 
	    3426, 3427, 5, 73, 0, 0, 3427, 3428, 5, 77, 0, 0, 3428, 3429, 5, 69, 
	    0, 0, 3429, 3430, 5, 83, 0, 0, 3430, 3431, 5, 84, 0, 0, 3431, 3432, 
	    5, 65, 0, 0, 3432, 3433, 5, 77, 0, 0, 3433, 3434, 5, 80, 0, 0, 3434, 
	    674, 1, 0, 0, 0, 3435, 3436, 5, 84, 0, 0, 3436, 3437, 5, 73, 0, 0, 
	    3437, 3438, 5, 77, 0, 0, 3438, 3439, 5, 69, 0, 0, 3439, 3440, 5, 83, 
	    0, 0, 3440, 3441, 5, 84, 0, 0, 3441, 3442, 5, 65, 0, 0, 3442, 3443, 
	    5, 77, 0, 0, 3443, 3444, 5, 80, 0, 0, 3444, 3445, 5, 65, 0, 0, 3445, 
	    3446, 5, 68, 0, 0, 3446, 3447, 5, 68, 0, 0, 3447, 676, 1, 0, 0, 0, 
	    3448, 3449, 5, 84, 0, 0, 3449, 3450, 5, 73, 0, 0, 3450, 3451, 5, 77, 
	    0, 0, 3451, 3452, 5, 69, 0, 0, 3452, 3453, 5, 83, 0, 0, 3453, 3454, 
	    5, 84, 0, 0, 3454, 3455, 5, 65, 0, 0, 3455, 3456, 5, 77, 0, 0, 3456, 
	    3457, 5, 80, 0, 0, 3457, 3458, 5, 68, 0, 0, 3458, 3459, 5, 73, 0, 0, 
	    3459, 3460, 5, 70, 0, 0, 3460, 3461, 5, 70, 0, 0, 3461, 678, 1, 0, 
	    0, 0, 3462, 3463, 5, 84, 0, 0, 3463, 3464, 5, 73, 0, 0, 3464, 3465, 
	    5, 77, 0, 0, 3465, 3466, 5, 69, 0, 0, 3466, 3467, 5, 83, 0, 0, 3467, 
	    3468, 5, 84, 0, 0, 3468, 3469, 5, 65, 0, 0, 3469, 3470, 5, 77, 0, 0, 
	    3470, 3471, 5, 80, 0, 0, 3471, 3472, 5, 95, 0, 0, 3472, 3473, 5, 76, 
	    0, 0, 3473, 3474, 5, 84, 0, 0, 3474, 3475, 5, 90, 0, 0, 3475, 680, 
	    1, 0, 0, 0, 3476, 3477, 5, 84, 0, 0, 3477, 3478, 5, 73, 0, 0, 3478, 
	    3479, 5, 77, 0, 0, 3479, 3480, 5, 69, 0, 0, 3480, 3481, 5, 83, 0, 0, 
	    3481, 3482, 5, 84, 0, 0, 3482, 3483, 5, 65, 0, 0, 3483, 3484, 5, 77, 
	    0, 0, 3484, 3485, 5, 80, 0, 0, 3485, 3486, 5, 95, 0, 0, 3486, 3487, 
	    5, 78, 0, 0, 3487, 3488, 5, 84, 0, 0, 3488, 3489, 5, 90, 0, 0, 3489, 
	    682, 1, 0, 0, 0, 3490, 3491, 5, 84, 0, 0, 3491, 3492, 5, 73, 0, 0, 
	    3492, 3493, 5, 78, 0, 0, 3493, 3494, 5, 89, 0, 0, 3494, 3495, 5, 73, 
	    0, 0, 3495, 3496, 5, 78, 0, 0, 3496, 3497, 5, 84, 0, 0, 3497, 684, 
	    1, 0, 0, 0, 3498, 3499, 5, 84, 0, 0, 3499, 3500, 5, 79, 0, 0, 3500, 
	    686, 1, 0, 0, 0, 3501, 3502, 5, 84, 0, 0, 3502, 3503, 5, 79, 0, 0, 
	    3503, 3504, 5, 85, 0, 0, 3504, 3505, 5, 67, 0, 0, 3505, 3506, 5, 72, 
	    0, 0, 3506, 688, 1, 0, 0, 0, 3507, 3508, 5, 84, 0, 0, 3508, 3509, 5, 
	    82, 0, 0, 3509, 3510, 5, 65, 0, 0, 3510, 3511, 5, 73, 0, 0, 3511, 3512, 
	    5, 76, 0, 0, 3512, 3513, 5, 73, 0, 0, 3513, 3514, 5, 78, 0, 0, 3514, 
	    3515, 5, 71, 0, 0, 3515, 690, 1, 0, 0, 0, 3516, 3517, 5, 84, 0, 0, 
	    3517, 3518, 5, 82, 0, 0, 3518, 3519, 5, 65, 0, 0, 3519, 3520, 5, 78, 
	    0, 0, 3520, 3521, 5, 83, 0, 0, 3521, 3522, 5, 65, 0, 0, 3522, 3523, 
	    5, 67, 0, 0, 3523, 3524, 5, 84, 0, 0, 3524, 3525, 5, 73, 0, 0, 3525, 
	    3526, 5, 79, 0, 0, 3526, 3527, 5, 78, 0, 0, 3527, 692, 1, 0, 0, 0, 
	    3528, 3529, 5, 84, 0, 0, 3529, 3530, 5, 82, 0, 0, 3530, 3531, 5, 65, 
	    0, 0, 3531, 3532, 5, 78, 0, 0, 3532, 3533, 5, 83, 0, 0, 3533, 3534, 
	    5, 65, 0, 0, 3534, 3535, 5, 67, 0, 0, 3535, 3536, 5, 84, 0, 0, 3536, 
	    3537, 5, 73, 0, 0, 3537, 3538, 5, 79, 0, 0, 3538, 3539, 5, 78, 0, 0, 
	    3539, 3540, 5, 83, 0, 0, 3540, 694, 1, 0, 0, 0, 3541, 3542, 5, 84, 
	    0, 0, 3542, 3543, 5, 82, 0, 0, 3543, 3544, 5, 65, 0, 0, 3544, 3545, 
	    5, 78, 0, 0, 3545, 3546, 5, 83, 0, 0, 3546, 3547, 5, 70, 0, 0, 3547, 
	    3548, 5, 79, 0, 0, 3548, 3549, 5, 82, 0, 0, 3549, 3550, 5, 77, 0, 0, 
	    3550, 696, 1, 0, 0, 0, 3551, 3552, 5, 84, 0, 0, 3552, 3553, 5, 82, 
	    0, 0, 3553, 3554, 5, 73, 0, 0, 3554, 3555, 5, 77, 0, 0, 3555, 698, 
	    1, 0, 0, 0, 3556, 3557, 5, 84, 0, 0, 3557, 3558, 5, 82, 0, 0, 3558, 
	    3559, 5, 85, 0, 0, 3559, 3560, 5, 69, 0, 0, 3560, 700, 1, 0, 0, 0, 
	    3561, 3562, 5, 84, 0, 0, 3562, 3563, 5, 82, 0, 0, 3563, 3564, 5, 85, 
	    0, 0, 3564, 3565, 5, 78, 0, 0, 3565, 3566, 5, 67, 0, 0, 3566, 3567, 
	    5, 65, 0, 0, 3567, 3568, 5, 84, 0, 0, 3568, 3569, 5, 69, 0, 0, 3569, 
	    702, 1, 0, 0, 0, 3570, 3571, 5, 84, 0, 0, 3571, 3572, 5, 82, 0, 0, 
	    3572, 3573, 5, 89, 0, 0, 3573, 3574, 5, 95, 0, 0, 3574, 3575, 5, 67, 
	    0, 0, 3575, 3576, 5, 65, 0, 0, 3576, 3577, 5, 83, 0, 0, 3577, 3578, 
	    5, 84, 0, 0, 3578, 704, 1, 0, 0, 0, 3579, 3580, 5, 84, 0, 0, 3580, 
	    3581, 5, 89, 0, 0, 3581, 3582, 5, 80, 0, 0, 3582, 3583, 5, 69, 0, 0, 
	    3583, 706, 1, 0, 0, 0, 3584, 3585, 5, 85, 0, 0, 3585, 3586, 5, 78, 
	    0, 0, 3586, 3587, 5, 65, 0, 0, 3587, 3588, 5, 82, 0, 0, 3588, 3589, 
	    5, 67, 0, 0, 3589, 3590, 5, 72, 0, 0, 3590, 3591, 5, 73, 0, 0, 3591, 
	    3592, 5, 86, 0, 0, 3592, 3593, 5, 69, 0, 0, 3593, 708, 1, 0, 0, 0, 
	    3594, 3595, 5, 85, 0, 0, 3595, 3596, 5, 78, 0, 0, 3596, 3597, 5, 66, 
	    0, 0, 3597, 3598, 5, 79, 0, 0, 3598, 3599, 5, 85, 0, 0, 3599, 3600, 
	    5, 78, 0, 0, 3600, 3601, 5, 68, 0, 0, 3601, 3602, 5, 69, 0, 0, 3602, 
	    3603, 5, 68, 0, 0, 3603, 710, 1, 0, 0, 0, 3604, 3605, 5, 85, 0, 0, 
	    3605, 3606, 5, 78, 0, 0, 3606, 3607, 5, 67, 0, 0, 3607, 3608, 5, 65, 
	    0, 0, 3608, 3609, 5, 67, 0, 0, 3609, 3610, 5, 72, 0, 0, 3610, 3611, 
	    5, 69, 0, 0, 3611, 712, 1, 0, 0, 0, 3612, 3613, 5, 85, 0, 0, 3613, 
	    3614, 5, 78, 0, 0, 3614, 3615, 5, 73, 0, 0, 3615, 3616, 5, 79, 0, 0, 
	    3616, 3617, 5, 78, 0, 0, 3617, 714, 1, 0, 0, 0, 3618, 3619, 5, 85, 
	    0, 0, 3619, 3620, 5, 78, 0, 0, 3620, 3621, 5, 73, 0, 0, 3621, 3622, 
	    5, 81, 0, 0, 3622, 3623, 5, 85, 0, 0, 3623, 3624, 5, 69, 0, 0, 3624, 
	    716, 1, 0, 0, 0, 3625, 3626, 5, 85, 0, 0, 3626, 3627, 5, 78, 0, 0, 
	    3627, 3628, 5, 75, 0, 0, 3628, 3629, 5, 78, 0, 0, 3629, 3630, 5, 79, 
	    0, 0, 3630, 3631, 5, 87, 0, 0, 3631, 3632, 5, 78, 0, 0, 3632, 718, 
	    1, 0, 0, 0, 3633, 3634, 5, 85, 0, 0, 3634, 3635, 5, 78, 0, 0, 3635, 
	    3636, 5, 76, 0, 0, 3636, 3637, 5, 79, 0, 0, 3637, 3638, 5, 67, 0, 0, 
	    3638, 3639, 5, 75, 0, 0, 3639, 720, 1, 0, 0, 0, 3640, 3641, 5, 85, 
	    0, 0, 3641, 3642, 5, 78, 0, 0, 3642, 3643, 5, 80, 0, 0, 3643, 3644, 
	    5, 73, 0, 0, 3644, 3645, 5, 86, 0, 0, 3645, 3646, 5, 79, 0, 0, 3646, 
	    3647, 5, 84, 0, 0, 3647, 722, 1, 0, 0, 0, 3648, 3649, 5, 85, 0, 0, 
	    3649, 3650, 5, 78, 0, 0, 3650, 3651, 5, 83, 0, 0, 3651, 3652, 5, 69, 
	    0, 0, 3652, 3653, 5, 84, 0, 0, 3653, 724, 1, 0, 0, 0, 3654, 3655, 5, 
	    85, 0, 0, 3655, 3656, 5, 80, 0, 0, 3656, 3657, 5, 68, 0, 0, 3657, 3658, 
	    5, 65, 0, 0, 3658, 3659, 5, 84, 0, 0, 3659, 3660, 5, 69, 0, 0, 3660, 
	    726, 1, 0, 0, 0, 3661, 3662, 5, 85, 0, 0, 3662, 3663, 5, 83, 0, 0, 
	    3663, 3664, 5, 69, 0, 0, 3664, 728, 1, 0, 0, 0, 3665, 3666, 5, 85, 
	    0, 0, 3666, 3667, 5, 83, 0, 0, 3667, 3668, 5, 69, 0, 0, 3668, 3669, 
	    5, 82, 0, 0, 3669, 730, 1, 0, 0, 0, 3670, 3671, 5, 85, 0, 0, 3671, 
	    3672, 5, 83, 0, 0, 3672, 3673, 5, 73, 0, 0, 3673, 3674, 5, 78, 0, 0, 
	    3674, 3675, 5, 71, 0, 0, 3675, 732, 1, 0, 0, 0, 3676, 3677, 5, 86, 
	    0, 0, 3677, 3678, 5, 65, 0, 0, 3678, 3679, 5, 76, 0, 0, 3679, 3680, 
	    5, 85, 0, 0, 3680, 3681, 5, 69, 0, 0, 3681, 3682, 5, 83, 0, 0, 3682, 
	    734, 1, 0, 0, 0, 3683, 3684, 5, 86, 0, 0, 3684, 3685, 5, 65, 0, 0, 
	    3685, 3686, 5, 82, 0, 0, 3686, 736, 1, 0, 0, 0, 3687, 3688, 5, 86, 
	    0, 0, 3688, 3689, 5, 65, 0, 0, 3689, 3690, 5, 82, 0, 0, 3690, 3691, 
	    5, 67, 0, 0, 3691, 3692, 5, 72, 0, 0, 3692, 3693, 5, 65, 0, 0, 3693, 
	    3694, 5, 82, 0, 0, 3694, 738, 1, 0, 0, 0, 3695, 3696, 5, 86, 0, 0, 
	    3696, 3697, 5, 65, 0, 0, 3697, 3698, 5, 82, 0, 0, 3698, 3699, 5, 73, 
	    0, 0, 3699, 3700, 5, 65, 0, 0, 3700, 3701, 5, 78, 0, 0, 3701, 3702, 
	    5, 84, 0, 0, 3702, 740, 1, 0, 0, 0, 3703, 3704, 5, 86, 0, 0, 3704, 
	    3705, 5, 69, 0, 0, 3705, 3706, 5, 82, 0, 0, 3706, 3707, 5, 83, 0, 0, 
	    3707, 3708, 5, 73, 0, 0, 3708, 3709, 5, 79, 0, 0, 3709, 3710, 5, 78, 
	    0, 0, 3710, 742, 1, 0, 0, 0, 3711, 3712, 5, 86, 0, 0, 3712, 3713, 5, 
	    73, 0, 0, 3713, 3714, 5, 69, 0, 0, 3714, 3715, 5, 87, 0, 0, 3715, 744, 
	    1, 0, 0, 0, 3716, 3717, 5, 86, 0, 0, 3717, 3718, 5, 73, 0, 0, 3718, 
	    3719, 5, 69, 0, 0, 3719, 3720, 5, 87, 0, 0, 3720, 3721, 5, 83, 0, 0, 
	    3721, 746, 1, 0, 0, 0, 3722, 3723, 5, 86, 0, 0, 3723, 3724, 5, 79, 
	    0, 0, 3724, 3725, 5, 73, 0, 0, 3725, 3726, 5, 68, 0, 0, 3726, 748, 
	    1, 0, 0, 0, 3727, 3728, 5, 87, 0, 0, 3728, 3729, 5, 69, 0, 0, 3729, 
	    3730, 5, 69, 0, 0, 3730, 3731, 5, 75, 0, 0, 3731, 750, 1, 0, 0, 0, 
	    3732, 3733, 5, 87, 0, 0, 3733, 3734, 5, 69, 0, 0, 3734, 3735, 5, 69, 
	    0, 0, 3735, 3736, 5, 75, 0, 0, 3736, 3737, 5, 83, 0, 0, 3737, 752, 
	    1, 0, 0, 0, 3738, 3739, 5, 87, 0, 0, 3739, 3740, 5, 72, 0, 0, 3740, 
	    3741, 5, 69, 0, 0, 3741, 3742, 5, 78, 0, 0, 3742, 754, 1, 0, 0, 0, 
	    3743, 3744, 5, 87, 0, 0, 3744, 3745, 5, 72, 0, 0, 3745, 3746, 5, 69, 
	    0, 0, 3746, 3747, 5, 82, 0, 0, 3747, 3748, 5, 69, 0, 0, 3748, 756, 
	    1, 0, 0, 0, 3749, 3750, 5, 87, 0, 0, 3750, 3751, 5, 72, 0, 0, 3751, 
	    3752, 5, 73, 0, 0, 3752, 3753, 5, 76, 0, 0, 3753, 3754, 5, 69, 0, 0, 
	    3754, 758, 1, 0, 0, 0, 3755, 3756, 5, 87, 0, 0, 3756, 3757, 5, 73, 
	    0, 0, 3757, 3758, 5, 78, 0, 0, 3758, 3759, 5, 68, 0, 0, 3759, 3760, 
	    5, 79, 0, 0, 3760, 3761, 5, 87, 0, 0, 3761, 760, 1, 0, 0, 0, 3762, 
	    3763, 5, 87, 0, 0, 3763, 3764, 5, 73, 0, 0, 3764, 3765, 5, 84, 0, 0, 
	    3765, 3766, 5, 72, 0, 0, 3766, 762, 1, 0, 0, 0, 3767, 3768, 5, 87, 
	    0, 0, 3768, 3769, 5, 73, 0, 0, 3769, 3770, 5, 84, 0, 0, 3770, 3771, 
	    5, 72, 0, 0, 3771, 3772, 5, 73, 0, 0, 3772, 3773, 5, 78, 0, 0, 3773, 
	    764, 1, 0, 0, 0, 3774, 3775, 5, 89, 0, 0, 3775, 3776, 5, 69, 0, 0, 
	    3776, 3777, 5, 65, 0, 0, 3777, 3778, 5, 82, 0, 0, 3778, 766, 1, 0, 
	    0, 0, 3779, 3780, 5, 89, 0, 0, 3780, 3781, 5, 69, 0, 0, 3781, 3782, 
	    5, 65, 0, 0, 3782, 3783, 5, 82, 0, 0, 3783, 3784, 5, 83, 0, 0, 3784, 
	    768, 1, 0, 0, 0, 3785, 3786, 5, 90, 0, 0, 3786, 3787, 5, 79, 0, 0, 
	    3787, 3788, 5, 78, 0, 0, 3788, 3789, 5, 69, 0, 0, 3789, 770, 1, 0, 
	    0, 0, 3790, 3791, 5, 40, 0, 0, 3791, 772, 1, 0, 0, 0, 3792, 3793, 5, 
	    41, 0, 0, 3793, 774, 1, 0, 0, 0, 3794, 3795, 5, 91, 0, 0, 3795, 776, 
	    1, 0, 0, 0, 3796, 3797, 5, 93, 0, 0, 3797, 778, 1, 0, 0, 0, 3798, 3799, 
	    5, 46, 0, 0, 3799, 780, 1, 0, 0, 0, 3800, 3801, 5, 61, 0, 0, 3801, 
	    782, 1, 0, 0, 0, 3802, 3803, 5, 61, 0, 0, 3803, 3804, 5, 61, 0, 0, 
	    3804, 784, 1, 0, 0, 0, 3805, 3806, 5, 33, 0, 0, 3806, 786, 1, 0, 0, 
	    0, 3807, 3808, 5, 60, 0, 0, 3808, 3809, 5, 61, 0, 0, 3809, 3810, 5, 
	    62, 0, 0, 3810, 788, 1, 0, 0, 0, 3811, 3812, 5, 47, 0, 0, 3812, 3813, 
	    5, 42, 0, 0, 3813, 3814, 5, 43, 0, 0, 3814, 790, 1, 0, 0, 0, 3815, 
	    3816, 5, 42, 0, 0, 3816, 3817, 5, 47, 0, 0, 3817, 792, 1, 0, 0, 0, 
	    3818, 3819, 5, 60, 0, 0, 3819, 3823, 5, 62, 0, 0, 3820, 3821, 5, 33, 
	    0, 0, 3821, 3823, 5, 61, 0, 0, 3822, 3818, 1, 0, 0, 0, 3822, 3820, 
	    1, 0, 0, 0, 3823, 794, 1, 0, 0, 0, 3824, 3825, 5, 60, 0, 0, 3825, 796, 
	    1, 0, 0, 0, 3826, 3827, 5, 60, 0, 0, 3827, 3828, 5, 61, 0, 0, 3828, 
	    798, 1, 0, 0, 0, 3829, 3830, 5, 62, 0, 0, 3830, 800, 1, 0, 0, 0, 3831, 
	    3832, 5, 62, 0, 0, 3832, 3833, 5, 61, 0, 0, 3833, 802, 1, 0, 0, 0, 
	    3834, 3835, 5, 43, 0, 0, 3835, 804, 1, 0, 0, 0, 3836, 3837, 5, 45, 
	    0, 0, 3837, 806, 1, 0, 0, 0, 3838, 3839, 5, 42, 0, 0, 3839, 808, 1, 
	    0, 0, 0, 3840, 3841, 5, 47, 0, 0, 3841, 810, 1, 0, 0, 0, 3842, 3843, 
	    5, 37, 0, 0, 3843, 812, 1, 0, 0, 0, 3844, 3845, 5, 124, 0, 0, 3845, 
	    3846, 5, 124, 0, 0, 3846, 814, 1, 0, 0, 0, 3847, 3848, 5, 63, 0, 0, 
	    3848, 816, 1, 0, 0, 0, 3849, 3850, 5, 59, 0, 0, 3850, 818, 1, 0, 0, 
	    0, 3851, 3852, 5, 58, 0, 0, 3852, 820, 1, 0, 0, 0, 3853, 3854, 5, 36, 
	    0, 0, 3854, 822, 1, 0, 0, 0, 3855, 3856, 5, 38, 0, 0, 3856, 824, 1, 
	    0, 0, 0, 3857, 3858, 5, 124, 0, 0, 3858, 826, 1, 0, 0, 0, 3859, 3860, 
	    5, 94, 0, 0, 3860, 828, 1, 0, 0, 0, 3861, 3862, 5, 60, 0, 0, 3862, 
	    3863, 5, 60, 0, 0, 3863, 830, 1, 0, 0, 0, 3864, 3865, 5, 126, 0, 0, 
	    3865, 832, 1, 0, 0, 0, 3866, 3867, 5, 92, 0, 0, 3867, 3868, 9, 0, 0, 
	    0, 3868, 834, 1, 0, 0, 0, 3869, 3874, 5, 39, 0, 0, 3870, 3873, 8, 0, 
	    0, 0, 3871, 3873, 3, 833, 416, 0, 3872, 3870, 1, 0, 0, 0, 3872, 3871, 
	    1, 0, 0, 0, 3873, 3876, 1, 0, 0, 0, 3874, 3872, 1, 0, 0, 0, 3874, 3875, 
	    1, 0, 0, 0, 3875, 3877, 1, 0, 0, 0, 3876, 3874, 1, 0, 0, 0, 3877, 3899, 
	    5, 39, 0, 0, 3878, 3879, 5, 82, 0, 0, 3879, 3880, 5, 39, 0, 0, 3880, 
	    3884, 1, 0, 0, 0, 3881, 3883, 8, 1, 0, 0, 3882, 3881, 1, 0, 0, 0, 3883, 
	    3886, 1, 0, 0, 0, 3884, 3882, 1, 0, 0, 0, 3884, 3885, 1, 0, 0, 0, 3885, 
	    3887, 1, 0, 0, 0, 3886, 3884, 1, 0, 0, 0, 3887, 3899, 5, 39, 0, 0, 
	    3888, 3889, 5, 82, 0, 0, 3889, 3890, 5, 34, 0, 0, 3890, 3894, 1, 0, 
	    0, 0, 3891, 3893, 8, 2, 0, 0, 3892, 3891, 1, 0, 0, 0, 3893, 3896, 1, 
	    0, 0, 0, 3894, 3892, 1, 0, 0, 0, 3894, 3895, 1, 0, 0, 0, 3895, 3897, 
	    1, 0, 0, 0, 3896, 3894, 1, 0, 0, 0, 3897, 3899, 5, 34, 0, 0, 3898, 
	    3869, 1, 0, 0, 0, 3898, 3878, 1, 0, 0, 0, 3898, 3888, 1, 0, 0, 0, 3899, 
	    836, 1, 0, 0, 0, 3900, 3905, 5, 34, 0, 0, 3901, 3904, 8, 3, 0, 0, 3902, 
	    3904, 3, 833, 416, 0, 3903, 3901, 1, 0, 0, 0, 3903, 3902, 1, 0, 0, 
	    0, 3904, 3907, 1, 0, 0, 0, 3905, 3903, 1, 0, 0, 0, 3905, 3906, 1, 0, 
	    0, 0, 3906, 3908, 1, 0, 0, 0, 3907, 3905, 1, 0, 0, 0, 3908, 3909, 5, 
	    34, 0, 0, 3909, 838, 1, 0, 0, 0, 3910, 3911, 5, 85, 0, 0, 3911, 3912, 
	    5, 38, 0, 0, 3912, 3913, 5, 39, 0, 0, 3913, 3919, 1, 0, 0, 0, 3914, 
	    3918, 8, 1, 0, 0, 3915, 3916, 5, 39, 0, 0, 3916, 3918, 5, 39, 0, 0, 
	    3917, 3914, 1, 0, 0, 0, 3917, 3915, 1, 0, 0, 0, 3918, 3921, 1, 0, 0, 
	    0, 3919, 3917, 1, 0, 0, 0, 3919, 3920, 1, 0, 0, 0, 3920, 3922, 1, 0, 
	    0, 0, 3921, 3919, 1, 0, 0, 0, 3922, 3923, 5, 39, 0, 0, 3923, 840, 1, 
	    0, 0, 0, 3924, 3926, 3, 867, 433, 0, 3925, 3924, 1, 0, 0, 0, 3926, 
	    3927, 1, 0, 0, 0, 3927, 3925, 1, 0, 0, 0, 3927, 3928, 1, 0, 0, 0, 3928, 
	    842, 1, 0, 0, 0, 3929, 3931, 3, 867, 433, 0, 3930, 3929, 1, 0, 0, 0, 
	    3931, 3932, 1, 0, 0, 0, 3932, 3930, 1, 0, 0, 0, 3932, 3933, 1, 0, 0, 
	    0, 3933, 3934, 1, 0, 0, 0, 3934, 3935, 5, 76, 0, 0, 3935, 844, 1, 0, 
	    0, 0, 3936, 3938, 3, 867, 433, 0, 3937, 3936, 1, 0, 0, 0, 3938, 3939, 
	    1, 0, 0, 0, 3939, 3937, 1, 0, 0, 0, 3939, 3940, 1, 0, 0, 0, 3940, 3941, 
	    1, 0, 0, 0, 3941, 3942, 5, 83, 0, 0, 3942, 846, 1, 0, 0, 0, 3943, 3945, 
	    3, 867, 433, 0, 3944, 3943, 1, 0, 0, 0, 3945, 3946, 1, 0, 0, 0, 3946, 
	    3944, 1, 0, 0, 0, 3946, 3947, 1, 0, 0, 0, 3947, 3948, 1, 0, 0, 0, 3948, 
	    3949, 5, 89, 0, 0, 3949, 848, 1, 0, 0, 0, 3950, 3952, 3, 867, 433, 
	    0, 3951, 3950, 1, 0, 0, 0, 3952, 3953, 1, 0, 0, 0, 3953, 3951, 1, 0, 
	    0, 0, 3953, 3954, 1, 0, 0, 0, 3954, 3955, 1, 0, 0, 0, 3955, 3956, 3, 
	    865, 432, 0, 3956, 3962, 1, 0, 0, 0, 3957, 3958, 3, 871, 435, 0, 3958, 
	    3959, 3, 865, 432, 0, 3959, 3960, 4, 424, 0, 0, 3960, 3962, 1, 0, 0, 
	    0, 3961, 3951, 1, 0, 0, 0, 3961, 3957, 1, 0, 0, 0, 3962, 850, 1, 0, 
	    0, 0, 3963, 3964, 3, 871, 435, 0, 3964, 3965, 4, 425, 1, 0, 3965, 852, 
	    1, 0, 0, 0, 3966, 3968, 3, 867, 433, 0, 3967, 3966, 1, 0, 0, 0, 3968, 
	    3969, 1, 0, 0, 0, 3969, 3967, 1, 0, 0, 0, 3969, 3970, 1, 0, 0, 0, 3970, 
	    3972, 1, 0, 0, 0, 3971, 3973, 3, 865, 432, 0, 3972, 3971, 1, 0, 0, 
	    0, 3972, 3973, 1, 0, 0, 0, 3973, 3974, 1, 0, 0, 0, 3974, 3975, 5, 70, 
	    0, 0, 3975, 3984, 1, 0, 0, 0, 3976, 3978, 3, 871, 435, 0, 3977, 3979, 
	    3, 865, 432, 0, 3978, 3977, 1, 0, 0, 0, 3978, 3979, 1, 0, 0, 0, 3979, 
	    3980, 1, 0, 0, 0, 3980, 3981, 5, 70, 0, 0, 3981, 3982, 4, 426, 2, 0, 
	    3982, 3984, 1, 0, 0, 0, 3983, 3967, 1, 0, 0, 0, 3983, 3976, 1, 0, 0, 
	    0, 3984, 854, 1, 0, 0, 0, 3985, 3987, 3, 867, 433, 0, 3986, 3985, 1, 
	    0, 0, 0, 3987, 3988, 1, 0, 0, 0, 3988, 3986, 1, 0, 0, 0, 3988, 3989, 
	    1, 0, 0, 0, 3989, 3991, 1, 0, 0, 0, 3990, 3992, 3, 865, 432, 0, 3991, 
	    3990, 1, 0, 0, 0, 3991, 3992, 1, 0, 0, 0, 3992, 3993, 1, 0, 0, 0, 3993, 
	    3994, 5, 68, 0, 0, 3994, 4003, 1, 0, 0, 0, 3995, 3997, 3, 871, 435, 
	    0, 3996, 3998, 3, 865, 432, 0, 3997, 3996, 1, 0, 0, 0, 3997, 3998, 
	    1, 0, 0, 0, 3998, 3999, 1, 0, 0, 0, 3999, 4000, 5, 68, 0, 0, 4000, 
	    4001, 4, 427, 3, 0, 4001, 4003, 1, 0, 0, 0, 4002, 3986, 1, 0, 0, 0, 
	    4002, 3995, 1, 0, 0, 0, 4003, 856, 1, 0, 0, 0, 4004, 4006, 3, 867, 
	    433, 0, 4005, 4004, 1, 0, 0, 0, 4006, 4007, 1, 0, 0, 0, 4007, 4005, 
	    1, 0, 0, 0, 4007, 4008, 1, 0, 0, 0, 4008, 4010, 1, 0, 0, 0, 4009, 4011, 
	    3, 865, 432, 0, 4010, 4009, 1, 0, 0, 0, 4010, 4011, 1, 0, 0, 0, 4011, 
	    4012, 1, 0, 0, 0, 4012, 4013, 5, 66, 0, 0, 4013, 4014, 5, 68, 0, 0, 
	    4014, 4025, 1, 0, 0, 0, 4015, 4017, 3, 871, 435, 0, 4016, 4018, 3, 
	    865, 432, 0, 4017, 4016, 1, 0, 0, 0, 4017, 4018, 1, 0, 0, 0, 4018, 
	    4019, 1, 0, 0, 0, 4019, 4020, 5, 66, 0, 0, 4020, 4021, 5, 68, 0, 0, 
	    4021, 4022, 1, 0, 0, 0, 4022, 4023, 4, 428, 4, 0, 4023, 4025, 1, 0, 
	    0, 0, 4024, 4005, 1, 0, 0, 0, 4024, 4015, 1, 0, 0, 0, 4025, 858, 1, 
	    0, 0, 0, 4026, 4030, 3, 869, 434, 0, 4027, 4030, 3, 867, 433, 0, 4028, 
	    4030, 5, 95, 0, 0, 4029, 4026, 1, 0, 0, 0, 4029, 4027, 1, 0, 0, 0, 
	    4029, 4028, 1, 0, 0, 0, 4030, 4033, 1, 0, 0, 0, 4031, 4029, 1, 0, 0, 
	    0, 4031, 4032, 1, 0, 0, 0, 4032, 4036, 1, 0, 0, 0, 4033, 4031, 1, 0, 
	    0, 0, 4034, 4037, 3, 869, 434, 0, 4035, 4037, 5, 95, 0, 0, 4036, 4034, 
	    1, 0, 0, 0, 4036, 4035, 1, 0, 0, 0, 4037, 4043, 1, 0, 0, 0, 4038, 4042, 
	    3, 869, 434, 0, 4039, 4042, 3, 867, 433, 0, 4040, 4042, 5, 95, 0, 0, 
	    4041, 4038, 1, 0, 0, 0, 4041, 4039, 1, 0, 0, 0, 4041, 4040, 1, 0, 0, 
	    0, 4042, 4045, 1, 0, 0, 0, 4043, 4041, 1, 0, 0, 0, 4043, 4044, 1, 0, 
	    0, 0, 4044, 860, 1, 0, 0, 0, 4045, 4043, 1, 0, 0, 0, 4046, 4052, 5, 
	    96, 0, 0, 4047, 4051, 8, 4, 0, 0, 4048, 4049, 5, 96, 0, 0, 4049, 4051, 
	    5, 96, 0, 0, 4050, 4047, 1, 0, 0, 0, 4050, 4048, 1, 0, 0, 0, 4051, 
	    4054, 1, 0, 0, 0, 4052, 4050, 1, 0, 0, 0, 4052, 4053, 1, 0, 0, 0, 4053, 
	    4055, 1, 0, 0, 0, 4054, 4052, 1, 0, 0, 0, 4055, 4056, 5, 96, 0, 0, 
	    4056, 862, 1, 0, 0, 0, 4057, 4058, 5, 64, 0, 0, 4058, 4059, 3, 859, 
	    429, 0, 4059, 864, 1, 0, 0, 0, 4060, 4062, 5, 69, 0, 0, 4061, 4063, 
	    7, 5, 0, 0, 4062, 4061, 1, 0, 0, 0, 4062, 4063, 1, 0, 0, 0, 4063, 4065, 
	    1, 0, 0, 0, 4064, 4066, 3, 867, 433, 0, 4065, 4064, 1, 0, 0, 0, 4066, 
	    4067, 1, 0, 0, 0, 4067, 4065, 1, 0, 0, 0, 4067, 4068, 1, 0, 0, 0, 4068, 
	    866, 1, 0, 0, 0, 4069, 4070, 7, 6, 0, 0, 4070, 868, 1, 0, 0, 0, 4071, 
	    4072, 7, 7, 0, 0, 4072, 870, 1, 0, 0, 0, 4073, 4075, 3, 867, 433, 0, 
	    4074, 4073, 1, 0, 0, 0, 4075, 4076, 1, 0, 0, 0, 4076, 4074, 1, 0, 0, 
	    0, 4076, 4077, 1, 0, 0, 0, 4077, 4078, 1, 0, 0, 0, 4078, 4082, 5, 46, 
	    0, 0, 4079, 4081, 3, 867, 433, 0, 4080, 4079, 1, 0, 0, 0, 4081, 4084, 
	    1, 0, 0, 0, 4082, 4080, 1, 0, 0, 0, 4082, 4083, 1, 0, 0, 0, 4083, 4092, 
	    1, 0, 0, 0, 4084, 4082, 1, 0, 0, 0, 4085, 4087, 5, 46, 0, 0, 4086, 
	    4088, 3, 867, 433, 0, 4087, 4086, 1, 0, 0, 0, 4088, 4089, 1, 0, 0, 
	    0, 4089, 4087, 1, 0, 0, 0, 4089, 4090, 1, 0, 0, 0, 4090, 4092, 1, 0, 
	    0, 0, 4091, 4074, 1, 0, 0, 0, 4091, 4085, 1, 0, 0, 0, 4092, 872, 1, 
	    0, 0, 0, 4093, 4094, 5, 45, 0, 0, 4094, 4095, 5, 45, 0, 0, 4095, 4099, 
	    1, 0, 0, 0, 4096, 4098, 8, 8, 0, 0, 4097, 4096, 1, 0, 0, 0, 4098, 4101, 
	    1, 0, 0, 0, 4099, 4097, 1, 0, 0, 0, 4099, 4100, 1, 0, 0, 0, 4100, 4103, 
	    1, 0, 0, 0, 4101, 4099, 1, 0, 0, 0, 4102, 4104, 5, 13, 0, 0, 4103, 
	    4102, 1, 0, 0, 0, 4103, 4104, 1, 0, 0, 0, 4104, 4106, 1, 0, 0, 0, 4105, 
	    4107, 5, 10, 0, 0, 4106, 4105, 1, 0, 0, 0, 4106, 4107, 1, 0, 0, 0, 
	    4107, 4108, 1, 0, 0, 0, 4108, 4109, 6, 436, 0, 0, 4109, 874, 1, 0, 
	    0, 0, 4110, 4111, 5, 47, 0, 0, 4111, 4112, 5, 42, 0, 0, 4112, 4117, 
	    1, 0, 0, 0, 4113, 4116, 3, 875, 437, 0, 4114, 4116, 9, 0, 0, 0, 4115, 
	    4113, 1, 0, 0, 0, 4115, 4114, 1, 0, 0, 0, 4116, 4119, 1, 0, 0, 0, 4117, 
	    4118, 1, 0, 0, 0, 4117, 4115, 1, 0, 0, 0, 4118, 4120, 1, 0, 0, 0, 4119, 
	    4117, 1, 0, 0, 0, 4120, 4121, 5, 42, 0, 0, 4121, 4122, 5, 47, 0, 0, 
	    4122, 4123, 1, 0, 0, 0, 4123, 4124, 6, 437, 0, 0, 4124, 876, 1, 0, 
	    0, 0, 4125, 4127, 7, 9, 0, 0, 4126, 4125, 1, 0, 0, 0, 4127, 4128, 1, 
	    0, 0, 0, 4128, 4126, 1, 0, 0, 0, 4128, 4129, 1, 0, 0, 0, 4129, 4130, 
	    1, 0, 0, 0, 4130, 4131, 6, 438, 0, 0, 4131, 878, 1, 0, 0, 0, 4132, 
	    4133, 5, 47, 0, 0, 4133, 4136, 5, 42, 0, 0, 4134, 4136, 7, 10, 0, 0, 
	    4135, 4132, 1, 0, 0, 0, 4135, 4134, 1, 0, 0, 0, 4136, 880, 1, 0, 0, 
	    0, 4137, 4138, 9, 0, 0, 0, 4138, 882, 1, 0, 0, 0, 49, 0, 3822, 3872, 
	    3874, 3884, 3894, 3898, 3903, 3905, 3917, 3919, 3927, 3932, 3939, 3946, 
	    3953, 3961, 3969, 3972, 3978, 3983, 3988, 3991, 3997, 4002, 4007, 4010, 
	    4017, 4024, 4029, 4031, 4036, 4041, 4043, 4050, 4052, 4062, 4067, 4076, 
	    4082, 4089, 4091, 4099, 4103, 4106, 4115, 4117, 4128, 4135, 1, 0, 1, 
	    0
	]);