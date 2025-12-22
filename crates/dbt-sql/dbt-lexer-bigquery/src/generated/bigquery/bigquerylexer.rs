// Generated from crates/dbt-sql/dbt-parser-bigquery/src/Bigquery.g4 by ANTLR 4.13.2
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
	pub const T__4:i32=5; 
	pub const T__5:i32=6; 
	pub const T__6:i32=7; 
	pub const ABORT:i32=8; 
	pub const ABSENT:i32=9; 
	pub const ADD:i32=10; 
	pub const ADMIN:i32=11; 
	pub const AFTER:i32=12; 
	pub const ALL:i32=13; 
	pub const ALTER:i32=14; 
	pub const ANALYZE:i32=15; 
	pub const AND:i32=16; 
	pub const ANTI:i32=17; 
	pub const ANY:i32=18; 
	pub const ARRAY:i32=19; 
	pub const AS:i32=20; 
	pub const ASC:i32=21; 
	pub const AT:i32=22; 
	pub const ATTACH:i32=23; 
	pub const AUTHORIZATION:i32=24; 
	pub const AUTO:i32=25; 
	pub const BACKUP:i32=26; 
	pub const BEGIN:i32=27; 
	pub const BERNOULLI:i32=28; 
	pub const BETWEEN:i32=29; 
	pub const BOTH:i32=30; 
	pub const BREAK:i32=31; 
	pub const BY:i32=32; 
	pub const BZIP2:i32=33; 
	pub const CALL:i32=34; 
	pub const CANCEL:i32=35; 
	pub const CASCADE:i32=36; 
	pub const CASE:i32=37; 
	pub const CASE_SENSITIVE:i32=38; 
	pub const CASE_INSENSITIVE:i32=39; 
	pub const CAST:i32=40; 
	pub const CATALOGS:i32=41; 
	pub const CHARACTER:i32=42; 
	pub const CLONE:i32=43; 
	pub const CLOSE:i32=44; 
	pub const CLUSTER:i32=45; 
	pub const COALESCE:i32=46; 
	pub const COLLATE:i32=47; 
	pub const COLUMN:i32=48; 
	pub const COLUMNS:i32=49; 
	pub const COMMA:i32=50; 
	pub const COMMENT:i32=51; 
	pub const COMMIT:i32=52; 
	pub const COMMITTED:i32=53; 
	pub const COMPOUND:i32=54; 
	pub const COMPRESSION:i32=55; 
	pub const CONDITIONAL:i32=56; 
	pub const CONNECT:i32=57; 
	pub const CONNECTION:i32=58; 
	pub const CONSTRAINT:i32=59; 
	pub const CONTINUE:i32=60; 
	pub const COPARTITION:i32=61; 
	pub const COPY:i32=62; 
	pub const COUNT:i32=63; 
	pub const CREATE:i32=64; 
	pub const CROSS:i32=65; 
	pub const CUBE:i32=66; 
	pub const CURRENT:i32=67; 
	pub const CURRENT_ROLE:i32=68; 
	pub const CUSTOM_HOLIDAY:i32=69; 
	pub const DATA:i32=70; 
	pub const DATABASE:i32=71; 
	pub const DATASHARE:i32=72; 
	pub const DATE:i32=73; 
	pub const DATETIME:i32=74; 
	pub const DAY:i32=75; 
	pub const DAYOFWEEK:i32=76; 
	pub const DAYOFYEAR:i32=77; 
	pub const DATETIME_DIFF:i32=78; 
	pub const DATE_DIFF:i32=79; 
	pub const DEALLOCATE:i32=80; 
	pub const DECLARE:i32=81; 
	pub const DEFAULT:i32=82; 
	pub const DEFAULTS:i32=83; 
	pub const DEFINE:i32=84; 
	pub const DEFINER:i32=85; 
	pub const DELETE:i32=86; 
	pub const DELIMITED:i32=87; 
	pub const DELIMITER:i32=88; 
	pub const DENY:i32=89; 
	pub const DESC:i32=90; 
	pub const DESCRIBE:i32=91; 
	pub const DESCRIPTOR:i32=92; 
	pub const DETERMINISTIC:i32=93; 
	pub const DISTINCT:i32=94; 
	pub const DISTKEY:i32=95; 
	pub const DISTRIBUTED:i32=96; 
	pub const DISTSTYLE:i32=97; 
	pub const DETACH:i32=98; 
	pub const DO:i32=99; 
	pub const DOUBLE:i32=100; 
	pub const DROP:i32=101; 
	pub const ELSE:i32=102; 
	pub const ELSEIF:i32=103; 
	pub const EMPTY:i32=104; 
	pub const ENCODE:i32=105; 
	pub const ENCODING:i32=106; 
	pub const END:i32=107; 
	pub const ERROR:i32=108; 
	pub const ESCAPE:i32=109; 
	pub const EVEN:i32=110; 
	pub const EXCEPT:i32=111; 
	pub const EXCEPTION:i32=112; 
	pub const EXCLUDE:i32=113; 
	pub const EXCLUDING:i32=114; 
	pub const EXECUTE:i32=115; 
	pub const EXISTS:i32=116; 
	pub const EXPLAIN:i32=117; 
	pub const EXTERNAL:i32=118; 
	pub const EXTRACT:i32=119; 
	pub const FALSE:i32=120; 
	pub const FETCH:i32=121; 
	pub const FIELDS:i32=122; 
	pub const FILTER:i32=123; 
	pub const FINAL:i32=124; 
	pub const FIRST:i32=125; 
	pub const FOLLOWING:i32=126; 
	pub const FOR:i32=127; 
	pub const FORMAT:i32=128; 
	pub const FRIDAY:i32=129; 
	pub const FROM:i32=130; 
	pub const FULL:i32=131; 
	pub const FUNCTION:i32=132; 
	pub const FUNCTIONS:i32=133; 
	pub const GENERATED:i32=134; 
	pub const GRACE:i32=135; 
	pub const GRANT:i32=136; 
	pub const GRANTED:i32=137; 
	pub const GRANTS:i32=138; 
	pub const GRAPHVIZ:i32=139; 
	pub const GROUP:i32=140; 
	pub const GROUPING:i32=141; 
	pub const GROUPS:i32=142; 
	pub const GZIP:i32=143; 
	pub const HAVING:i32=144; 
	pub const HEADER:i32=145; 
	pub const HOUR:i32=146; 
	pub const IDENTITY:i32=147; 
	pub const IF:i32=148; 
	pub const IGNORE:i32=149; 
	pub const IMMEDIATE:i32=150; 
	pub const IN:i32=151; 
	pub const INCLUDE:i32=152; 
	pub const INCLUDING:i32=153; 
	pub const INITIAL:i32=154; 
	pub const INNER:i32=155; 
	pub const INPUT:i32=156; 
	pub const INPUTFORMAT:i32=157; 
	pub const INTERLEAVED:i32=158; 
	pub const INSERT:i32=159; 
	pub const INTERSECT:i32=160; 
	pub const INTERVAL:i32=161; 
	pub const INTO:i32=162; 
	pub const INVOKER:i32=163; 
	pub const IO:i32=164; 
	pub const IS:i32=165; 
	pub const ISOLATION:i32=166; 
	pub const ISOWEEK:i32=167; 
	pub const ISOYEAR:i32=168; 
	pub const ITERATE:i32=169; 
	pub const ILIKE:i32=170; 
	pub const JOIN:i32=171; 
	pub const JSON:i32=172; 
	pub const KEEP:i32=173; 
	pub const KEY:i32=174; 
	pub const KEYS:i32=175; 
	pub const LAMBDA:i32=176; 
	pub const LANGUAGE:i32=177; 
	pub const LEAVE:i32=178; 
	pub const LAST:i32=179; 
	pub const LATERAL:i32=180; 
	pub const LEADING:i32=181; 
	pub const LEFT:i32=182; 
	pub const LEVEL:i32=183; 
	pub const LIBRARY:i32=184; 
	pub const LIKE:i32=185; 
	pub const LIMIT:i32=186; 
	pub const LINES:i32=187; 
	pub const LISTAGG:i32=188; 
	pub const LOCAL:i32=189; 
	pub const LOCATION:i32=190; 
	pub const LOCK:i32=191; 
	pub const LOGICAL:i32=192; 
	pub const LOOP:i32=193; 
	pub const MAP:i32=194; 
	pub const MASKING:i32=195; 
	pub const MATCH:i32=196; 
	pub const MATCHED:i32=197; 
	pub const MATCHES:i32=198; 
	pub const MATCH_RECOGNIZE:i32=199; 
	pub const MATERIALIZED:i32=200; 
	pub const MAX:i32=201; 
	pub const MEASURES:i32=202; 
	pub const MERGE:i32=203; 
	pub const MESSAGE:i32=204; 
	pub const MICROSECOND:i32=205; 
	pub const MILLISECOND:i32=206; 
	pub const MIN:i32=207; 
	pub const MINUS_KW:i32=208; 
	pub const MINUTE:i32=209; 
	pub const MODEL:i32=210; 
	pub const MONDAY:i32=211; 
	pub const MONTH:i32=212; 
	pub const NAME:i32=213; 
	pub const NATURAL:i32=214; 
	pub const NEXT:i32=215; 
	pub const NFC:i32=216; 
	pub const NFD:i32=217; 
	pub const NFKC:i32=218; 
	pub const NFKD:i32=219; 
	pub const NO:i32=220; 
	pub const NONE:i32=221; 
	pub const NORMALIZE:i32=222; 
	pub const NOT:i32=223; 
	pub const NULL:i32=224; 
	pub const NULLS:i32=225; 
	pub const OBJECT:i32=226; 
	pub const OF:i32=227; 
	pub const OFFSET:i32=228; 
	pub const OMIT:i32=229; 
	pub const ON:i32=230; 
	pub const ONE:i32=231; 
	pub const ONLY:i32=232; 
	pub const OPTION:i32=233; 
	pub const OPTIONS:i32=234; 
	pub const OR:i32=235; 
	pub const ORDER:i32=236; 
	pub const OUTER:i32=237; 
	pub const OUTPUT:i32=238; 
	pub const OUTPUTFORMAT:i32=239; 
	pub const OVER:i32=240; 
	pub const OVERFLOW:i32=241; 
	pub const PARTITION:i32=242; 
	pub const PARTITIONED:i32=243; 
	pub const PARTITIONS:i32=244; 
	pub const PASSING:i32=245; 
	pub const PAST:i32=246; 
	pub const PATH:i32=247; 
	pub const PATTERN:i32=248; 
	pub const PER:i32=249; 
	pub const PERCENT_KW:i32=250; 
	pub const PERIOD:i32=251; 
	pub const PERMUTE:i32=252; 
	pub const PIVOT:i32=253; 
	pub const POSITION:i32=254; 
	pub const PRECEDING:i32=255; 
	pub const PRECISION:i32=256; 
	pub const PREPARE:i32=257; 
	pub const PRIOR:i32=258; 
	pub const PROCEDURE:i32=259; 
	pub const PRIVILEGES:i32=260; 
	pub const PROPERTIES:i32=261; 
	pub const PRUNE:i32=262; 
	pub const QUALIFY:i32=263; 
	pub const QUARTER:i32=264; 
	pub const QUOTES:i32=265; 
	pub const RAISE:i32=266; 
	pub const RANGE:i32=267; 
	pub const READ:i32=268; 
	pub const RECURSIVE:i32=269; 
	pub const REFRESH:i32=270; 
	pub const RENAME:i32=271; 
	pub const REPEATABLE:i32=272; 
	pub const REPLACE:i32=273; 
	pub const RESET:i32=274; 
	pub const RESPECT:i32=275; 
	pub const RESTRICT:i32=276; 
	pub const RETURN:i32=277; 
	pub const RETURNING:i32=278; 
	pub const REMOTE:i32=279; 
	pub const REPEAT:i32=280; 
	pub const RETURNS:i32=281; 
	pub const REVOKE:i32=282; 
	pub const RIGHT:i32=283; 
	pub const RLS:i32=284; 
	pub const ROLE:i32=285; 
	pub const ROLES:i32=286; 
	pub const ROLLBACK:i32=287; 
	pub const ROLLUP:i32=288; 
	pub const ROW:i32=289; 
	pub const ROWS:i32=290; 
	pub const RUNNING:i32=291; 
	pub const SAFE:i32=292; 
	pub const SAFE_CAST:i32=293; 
	pub const SATURDAY:i32=294; 
	pub const SCALAR:i32=295; 
	pub const SECOND:i32=296; 
	pub const SCHEMA:i32=297; 
	pub const SCHEMAS:i32=298; 
	pub const SECURITY:i32=299; 
	pub const SEEK:i32=300; 
	pub const SELECT:i32=301; 
	pub const SEMI:i32=302; 
	pub const SERDE:i32=303; 
	pub const SERDEPROPERTIES:i32=304; 
	pub const SERIALIZABLE:i32=305; 
	pub const SESSION:i32=306; 
	pub const SET:i32=307; 
	pub const SETS:i32=308; 
	pub const SHOW:i32=309; 
	pub const SIMILAR:i32=310; 
	pub const SNAPSHOT:i32=311; 
	pub const SOME:i32=312; 
	pub const SORTKEY:i32=313; 
	pub const START:i32=314; 
	pub const STATS:i32=315; 
	pub const STORED:i32=316; 
	pub const STRUCT:i32=317; 
	pub const SUBSET:i32=318; 
	pub const SUBSTRING:i32=319; 
	pub const SUNDAY:i32=320; 
	pub const SYSTEM:i32=321; 
	pub const SYSTEM_TIME:i32=322; 
	pub const TABLE:i32=323; 
	pub const TABLES:i32=324; 
	pub const TABLESAMPLE:i32=325; 
	pub const TEMP:i32=326; 
	pub const TEMPORARY:i32=327; 
	pub const TERMINATED:i32=328; 
	pub const TEXT:i32=329; 
	pub const STRING_KW:i32=330; 
	pub const THEN:i32=331; 
	pub const THURSDAY:i32=332; 
	pub const TIES:i32=333; 
	pub const TIME:i32=334; 
	pub const TIMESTAMP:i32=335; 
	pub const TIMESTAMP_DIFF:i32=336; 
	pub const TO:i32=337; 
	pub const TOP:i32=338; 
	pub const TRAILING:i32=339; 
	pub const TARGET:i32=340; 
	pub const SOURCE:i32=341; 
	pub const TRAINING_DATA:i32=342; 
	pub const TRANSACTION:i32=343; 
	pub const TRANSFORM:i32=344; 
	pub const TRIM:i32=345; 
	pub const TRUE:i32=346; 
	pub const TRUNCATE:i32=347; 
	pub const TRY_CAST:i32=348; 
	pub const TUPLE:i32=349; 
	pub const TUESDAY:i32=350; 
	pub const TYPE:i32=351; 
	pub const UESCAPE:i32=352; 
	pub const UNBOUNDED:i32=353; 
	pub const UNCOMMITTED:i32=354; 
	pub const UNCONDITIONAL:i32=355; 
	pub const UNION:i32=356; 
	pub const UNKNOWN:i32=357; 
	pub const UNLOAD:i32=358; 
	pub const UNMATCHED:i32=359; 
	pub const UNNEST:i32=360; 
	pub const UNPIVOT:i32=361; 
	pub const UNSIGNED:i32=362; 
	pub const UNTIL:i32=363; 
	pub const UPDATE:i32=364; 
	pub const USE:i32=365; 
	pub const USER:i32=366; 
	pub const USING:i32=367; 
	pub const UTF16:i32=368; 
	pub const UTF32:i32=369; 
	pub const UTF8:i32=370; 
	pub const VACUUM:i32=371; 
	pub const VALIDATE:i32=372; 
	pub const VALUE:i32=373; 
	pub const VALUES:i32=374; 
	pub const VARYING:i32=375; 
	pub const VERBOSE:i32=376; 
	pub const VERSION:i32=377; 
	pub const VIEW:i32=378; 
	pub const WEDNESDAY:i32=379; 
	pub const WEEK:i32=380; 
	pub const WHEN:i32=381; 
	pub const WHERE:i32=382; 
	pub const WHILE:i32=383; 
	pub const WINDOW:i32=384; 
	pub const WITH:i32=385; 
	pub const WITHOUT:i32=386; 
	pub const WORK:i32=387; 
	pub const WRAPPER:i32=388; 
	pub const WRITE:i32=389; 
	pub const XZ:i32=390; 
	pub const YEAR:i32=391; 
	pub const YES:i32=392; 
	pub const ZONE:i32=393; 
	pub const ZSTD:i32=394; 
	pub const LPAREN:i32=395; 
	pub const RPAREN:i32=396; 
	pub const LBRACKET:i32=397; 
	pub const RBRACKET:i32=398; 
	pub const DOT:i32=399; 
	pub const EQ:i32=400; 
	pub const NEQ:i32=401; 
	pub const LT:i32=402; 
	pub const LTE:i32=403; 
	pub const GT:i32=404; 
	pub const GTE:i32=405; 
	pub const PLUS:i32=406; 
	pub const MINUS:i32=407; 
	pub const ASTERISK:i32=408; 
	pub const SLASH:i32=409; 
	pub const PERCENT:i32=410; 
	pub const CONCAT:i32=411; 
	pub const QUESTION_MARK:i32=412; 
	pub const SEMI_COLON:i32=413; 
	pub const COLON:i32=414; 
	pub const DOLLAR:i32=415; 
	pub const BITWISE_AND:i32=416; 
	pub const BITWISE_OR:i32=417; 
	pub const BITWISE_XOR:i32=418; 
	pub const BITWISE_SHIFT_LEFT:i32=419; 
	pub const POSIX:i32=420; 
	pub const ESCAPE_SEQUENCE:i32=421; 
	pub const QUOTED_STRING:i32=422; 
	pub const TRIPLE_QUOTED_STRING:i32=423; 
	pub const RAW_QUOTED_STRING:i32=424; 
	pub const RAW_TRIPLE_QUOTED_STRING:i32=425; 
	pub const BINARY_LITERAL:i32=426; 
	pub const INTEGER_VALUE:i32=427; 
	pub const HEXADECIMAL_VALUE:i32=428; 
	pub const DECIMAL_VALUE:i32=429; 
	pub const DOUBLE_VALUE:i32=430; 
	pub const IDENTIFIER:i32=431; 
	pub const BACKQUOTED_IDENTIFIER:i32=432; 
	pub const SYSTEM_VARIABLE:i32=433; 
	pub const VARIABLE:i32=434; 
	pub const SIMPLE_COMMENT:i32=435; 
	pub const BIG_QUERY_SIMPLE_COMMENT:i32=436; 
	pub const BRACKETED_COMMENT:i32=437; 
	pub const WS:i32=438; 
	pub const OTHER_WS:i32=439; 
	pub const UNPAIRED_TOKEN:i32=440; 
	pub const UNRECOGNIZED:i32=441;
	pub const channelNames: [&'static str;0+2] = [
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	];

	pub const modeNames: [&'static str;1] = [
		"DEFAULT_MODE"
	];

	pub const ruleNames: [&'static str;444] = [
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "ABORT", "ABSENT", 
		"ADD", "ADMIN", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", "ANY", 
		"ARRAY", "AS", "ASC", "AT", "ATTACH", "AUTHORIZATION", "AUTO", "BACKUP", 
		"BEGIN", "BERNOULLI", "BETWEEN", "BOTH", "BREAK", "BY", "BZIP2", "CALL", 
		"CANCEL", "CASCADE", "CASE", "CASE_SENSITIVE", "CASE_INSENSITIVE", "CAST", 
		"CATALOGS", "CHARACTER", "CLONE", "CLOSE", "CLUSTER", "COALESCE", "COLLATE", 
		"COLUMN", "COLUMNS", "COMMA", "COMMENT", "COMMIT", "COMMITTED", "COMPOUND", 
		"COMPRESSION", "CONDITIONAL", "CONNECT", "CONNECTION", "CONSTRAINT", "CONTINUE", 
		"COPARTITION", "COPY", "COUNT", "CREATE", "CROSS", "CUBE", "CURRENT", 
		"CURRENT_ROLE", "CUSTOM_HOLIDAY", "DATA", "DATABASE", "DATASHARE", "DATE", 
		"DATETIME", "DAY", "DAYOFWEEK", "DAYOFYEAR", "DATETIME_DIFF", "DATE_DIFF", 
		"DEALLOCATE", "DECLARE", "DEFAULT", "DEFAULTS", "DEFINE", "DEFINER", "DELETE", 
		"DELIMITED", "DELIMITER", "DENY", "DESC", "DESCRIBE", "DESCRIPTOR", "DETERMINISTIC", 
		"DISTINCT", "DISTKEY", "DISTRIBUTED", "DISTSTYLE", "DETACH", "DO", "DOUBLE", 
		"DROP", "ELSE", "ELSEIF", "EMPTY", "ENCODE", "ENCODING", "END", "ERROR", 
		"ESCAPE", "EVEN", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUDING", "EXECUTE", 
		"EXISTS", "EXPLAIN", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIELDS", 
		"FILTER", "FINAL", "FIRST", "FOLLOWING", "FOR", "FORMAT", "FRIDAY", "FROM", 
		"FULL", "FUNCTION", "FUNCTIONS", "GENERATED", "GRACE", "GRANT", "GRANTED", 
		"GRANTS", "GRAPHVIZ", "GROUP", "GROUPING", "GROUPS", "GZIP", "HAVING", 
		"HEADER", "HOUR", "IDENTITY", "IF", "IGNORE", "IMMEDIATE", "IN", "INCLUDE", 
		"INCLUDING", "INITIAL", "INNER", "INPUT", "INPUTFORMAT", "INTERLEAVED", 
		"INSERT", "INTERSECT", "INTERVAL", "INTO", "INVOKER", "IO", "IS", "ISOLATION", 
		"ISOWEEK", "ISOYEAR", "ITERATE", "ILIKE", "JOIN", "JSON", "KEEP", "KEY", 
		"KEYS", "LAMBDA", "LANGUAGE", "LEAVE", "LAST", "LATERAL", "LEADING", "LEFT", 
		"LEVEL", "LIBRARY", "LIKE", "LIMIT", "LINES", "LISTAGG", "LOCAL", "LOCATION", 
		"LOCK", "LOGICAL", "LOOP", "MAP", "MASKING", "MATCH", "MATCHED", "MATCHES", 
		"MATCH_RECOGNIZE", "MATERIALIZED", "MAX", "MEASURES", "MERGE", "MESSAGE", 
		"MICROSECOND", "MILLISECOND", "MIN", "MINUS_KW", "MINUTE", "MODEL", "MONDAY", 
		"MONTH", "NAME", "NATURAL", "NEXT", "NFC", "NFD", "NFKC", "NFKD", "NO", 
		"NONE", "NORMALIZE", "NOT", "NULL", "NULLS", "OBJECT", "OF", "OFFSET", 
		"OMIT", "ON", "ONE", "ONLY", "OPTION", "OPTIONS", "OR", "ORDER", "OUTER", 
		"OUTPUT", "OUTPUTFORMAT", "OVER", "OVERFLOW", "PARTITION", "PARTITIONED", 
		"PARTITIONS", "PASSING", "PAST", "PATH", "PATTERN", "PER", "PERCENT_KW", 
		"PERIOD", "PERMUTE", "PIVOT", "POSITION", "PRECEDING", "PRECISION", "PREPARE", 
		"PRIOR", "PROCEDURE", "PRIVILEGES", "PROPERTIES", "PRUNE", "QUALIFY", 
		"QUARTER", "QUOTES", "RAISE", "RANGE", "READ", "RECURSIVE", "REFRESH", 
		"RENAME", "REPEATABLE", "REPLACE", "RESET", "RESPECT", "RESTRICT", "RETURN", 
		"RETURNING", "REMOTE", "REPEAT", "RETURNS", "REVOKE", "RIGHT", "RLS", 
		"ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "RUNNING", "SAFE", 
		"SAFE_CAST", "SATURDAY", "SCALAR", "SECOND", "SCHEMA", "SCHEMAS", "SECURITY", 
		"SEEK", "SELECT", "SEMI", "SERDE", "SERDEPROPERTIES", "SERIALIZABLE", 
		"SESSION", "SET", "SETS", "SHOW", "SIMILAR", "SNAPSHOT", "SOME", "SORTKEY", 
		"START", "STATS", "STORED", "STRUCT", "SUBSET", "SUBSTRING", "SUNDAY", 
		"SYSTEM", "SYSTEM_TIME", "TABLE", "TABLES", "TABLESAMPLE", "TEMP", "TEMPORARY", 
		"TERMINATED", "TEXT", "STRING_KW", "THEN", "THURSDAY", "TIES", "TIME", 
		"TIMESTAMP", "TIMESTAMP_DIFF", "TO", "TOP", "TRAILING", "TARGET", "SOURCE", 
		"TRAINING_DATA", "TRANSACTION", "TRANSFORM", "TRIM", "TRUE", "TRUNCATE", 
		"TRY_CAST", "TUPLE", "TUESDAY", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", 
		"UNCONDITIONAL", "UNION", "UNKNOWN", "UNLOAD", "UNMATCHED", "UNNEST", 
		"UNPIVOT", "UNSIGNED", "UNTIL", "UPDATE", "USE", "USER", "USING", "UTF16", 
		"UTF32", "UTF8", "VACUUM", "VALIDATE", "VALUE", "VALUES", "VARYING", "VERBOSE", 
		"VERSION", "VIEW", "WEDNESDAY", "WEEK", "WHEN", "WHERE", "WHILE", "WINDOW", 
		"WITH", "WITHOUT", "WORK", "WRAPPER", "WRITE", "XZ", "YEAR", "YES", "ZONE", 
		"ZSTD", "LPAREN", "RPAREN", "LBRACKET", "RBRACKET", "DOT", "EQ", "NEQ", 
		"LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", "PERCENT", 
		"CONCAT", "QUESTION_MARK", "SEMI_COLON", "COLON", "DOLLAR", "BITWISE_AND", 
		"BITWISE_OR", "BITWISE_XOR", "BITWISE_SHIFT_LEFT", "POSIX", "ESCAPE_SEQUENCE", 
		"QUOTED_STRING", "TRIPLE_QUOTED_STRING", "RAW_QUOTED_STRING", "RAW_TRIPLE_QUOTED_STRING", 
		"BINARY_LITERAL", "INTEGER_VALUE", "HEXADECIMAL_VALUE", "DECIMAL_VALUE", 
		"DOUBLE_VALUE", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SYSTEM_VARIABLE", 
		"VARIABLE", "EXPONENT", "DIGIT", "LETTER", "SIMPLE_COMMENT", "BIG_QUERY_SIMPLE_COMMENT", 
		"BRACKETED_COMMENT", "WS", "OTHER_WS", "UNPAIRED_TOKEN", "UNRECOGNIZED"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;421] = [
		None, Some("'`'"), Some("'=>'"), Some("'->'"), Some("'{-'"), Some("'-}'"), 
		Some("'{'"), Some("'}'"), Some("'ABORT'"), Some("'ABSENT'"), Some("'ADD'"), 
		Some("'ADMIN'"), Some("'AFTER'"), Some("'ALL'"), Some("'ALTER'"), Some("'ANALYZE'"), 
		Some("'AND'"), Some("'ANTI'"), Some("'ANY'"), Some("'ARRAY'"), Some("'AS'"), 
		Some("'ASC'"), Some("'AT'"), Some("'ATTACH'"), Some("'AUTHORIZATION'"), 
		Some("'AUTO'"), Some("'BACKUP'"), Some("'BEGIN'"), Some("'BERNOULLI'"), 
		Some("'BETWEEN'"), Some("'BOTH'"), Some("'BREAK'"), Some("'BY'"), Some("'BZIP2'"), 
		Some("'CALL'"), Some("'CANCEL'"), Some("'CASCADE'"), Some("'CASE'"), Some("'CASE_SENSITIVE'"), 
		Some("'CASE_INSENSITIVE'"), Some("'CAST'"), Some("'CATALOGS'"), Some("'CHARACTER'"), 
		Some("'CLONE'"), Some("'CLOSE'"), Some("'CLUSTER'"), Some("'COALESCE'"), 
		Some("'COLLATE'"), Some("'COLUMN'"), Some("'COLUMNS'"), Some("','"), Some("'COMMENT'"), 
		Some("'COMMIT'"), Some("'COMMITTED'"), Some("'COMPOUND'"), Some("'COMPRESSION'"), 
		Some("'CONDITIONAL'"), Some("'CONNECT'"), Some("'CONNECTION'"), Some("'CONSTRAINT'"), 
		Some("'CONTINUE'"), Some("'COPARTITION'"), Some("'COPY'"), Some("'COUNT'"), 
		Some("'CREATE'"), Some("'CROSS'"), Some("'CUBE'"), Some("'CURRENT'"), 
		Some("'CURRENT_ROLE'"), Some("'CUSTOM_HOLIDAY'"), Some("'DATA'"), Some("'DATABASE'"), 
		Some("'DATASHARE'"), Some("'DATE'"), Some("'DATETIME'"), Some("'DAY'"), 
		Some("'DAYOFWEEK'"), Some("'DAYOFYEAR'"), Some("'DATETIME_DIFF'"), Some("'DATE_DIFF'"), 
		Some("'DEALLOCATE'"), Some("'DECLARE'"), Some("'DEFAULT'"), Some("'DEFAULTS'"), 
		Some("'DEFINE'"), Some("'DEFINER'"), Some("'DELETE'"), Some("'DELIMITED'"), 
		Some("'DELIMITER'"), Some("'DENY'"), Some("'DESC'"), Some("'DESCRIBE'"), 
		Some("'DESCRIPTOR'"), Some("'DETERMINISTIC'"), Some("'DISTINCT'"), Some("'DISTKEY'"), 
		Some("'DISTRIBUTED'"), Some("'DISTSTYLE'"), Some("'DETACH'"), Some("'DO'"), 
		Some("'DOUBLE'"), Some("'DROP'"), Some("'ELSE'"), Some("'ELSEIF'"), Some("'EMPTY'"), 
		Some("'ENCODE'"), Some("'ENCODING'"), Some("'END'"), Some("'ERROR'"), 
		Some("'ESCAPE'"), Some("'EVEN'"), Some("'EXCEPT'"), Some("'EXCEPTION'"), 
		Some("'EXCLUDE'"), Some("'EXCLUDING'"), Some("'EXECUTE'"), Some("'EXISTS'"), 
		Some("'EXPLAIN'"), Some("'EXTERNAL'"), Some("'EXTRACT'"), Some("'FALSE'"), 
		Some("'FETCH'"), Some("'FIELDS'"), Some("'FILTER'"), Some("'FINAL'"), 
		Some("'FIRST'"), Some("'FOLLOWING'"), Some("'FOR'"), Some("'FORMAT'"), 
		Some("'FRIDAY'"), Some("'FROM'"), Some("'FULL'"), Some("'FUNCTION'"), 
		Some("'FUNCTIONS'"), Some("'GENERATED'"), Some("'GRACE'"), Some("'GRANT'"), 
		Some("'GRANTED'"), Some("'GRANTS'"), Some("'GRAPHVIZ'"), Some("'GROUP'"), 
		Some("'GROUPING'"), Some("'GROUPS'"), Some("'GZIP'"), Some("'HAVING'"), 
		Some("'HEADER'"), Some("'HOUR'"), Some("'IDENTITY'"), Some("'IF'"), Some("'IGNORE'"), 
		Some("'IMMEDIATE'"), Some("'IN'"), Some("'INCLUDE'"), Some("'INCLUDING'"), 
		Some("'INITIAL'"), Some("'INNER'"), Some("'INPUT'"), Some("'INPUTFORMAT'"), 
		Some("'INTERLEAVED'"), Some("'INSERT'"), Some("'INTERSECT'"), Some("'INTERVAL'"), 
		Some("'INTO'"), Some("'INVOKER'"), Some("'IO'"), Some("'IS'"), Some("'ISOLATION'"), 
		Some("'ISOWEEK'"), Some("'ISOYEAR'"), Some("'ITERATE'"), Some("'ILIKE'"), 
		Some("'JOIN'"), Some("'JSON'"), Some("'KEEP'"), Some("'KEY'"), Some("'KEYS'"), 
		Some("'LAMBDA'"), Some("'LANGUAGE'"), Some("'LEAVE'"), Some("'LAST'"), 
		Some("'LATERAL'"), Some("'LEADING'"), Some("'LEFT'"), Some("'LEVEL'"), 
		Some("'LIBRARY'"), Some("'LIKE'"), Some("'LIMIT'"), Some("'LINES'"), Some("'LISTAGG'"), 
		Some("'LOCAL'"), Some("'LOCATION'"), Some("'LOCK'"), Some("'LOGICAL'"), 
		Some("'LOOP'"), Some("'MAP'"), Some("'MASKING'"), Some("'MATCH'"), Some("'MATCHED'"), 
		Some("'MATCHES'"), Some("'MATCH_RECOGNIZE'"), Some("'MATERIALIZED'"), 
		Some("'MAX'"), Some("'MEASURES'"), Some("'MERGE'"), Some("'MESSAGE'"), 
		Some("'MICROSECOND'"), Some("'MILLISECOND'"), Some("'MIN'"), Some("'MINUS'"), 
		Some("'MINUTE'"), Some("'MODEL'"), Some("'MONDAY'"), Some("'MONTH'"), 
		Some("'NAME'"), Some("'NATURAL'"), Some("'NEXT'"), Some("'NFC'"), Some("'NFD'"), 
		Some("'NFKC'"), Some("'NFKD'"), Some("'NO'"), Some("'NONE'"), Some("'NORMALIZE'"), 
		Some("'NOT'"), Some("'NULL'"), Some("'NULLS'"), Some("'OBJECT'"), Some("'OF'"), 
		Some("'OFFSET'"), Some("'OMIT'"), Some("'ON'"), Some("'ONE'"), Some("'ONLY'"), 
		Some("'OPTION'"), Some("'OPTIONS'"), Some("'OR'"), Some("'ORDER'"), Some("'OUTER'"), 
		Some("'OUTPUT'"), Some("'OUTPUTFORMAT'"), Some("'OVER'"), Some("'OVERFLOW'"), 
		Some("'PARTITION'"), Some("'PARTITIONED'"), Some("'PARTITIONS'"), Some("'PASSING'"), 
		Some("'PAST'"), Some("'PATH'"), Some("'PATTERN'"), Some("'PER'"), Some("'PERCENT'"), 
		Some("'PERIOD'"), Some("'PERMUTE'"), Some("'PIVOT'"), Some("'POSITION'"), 
		Some("'PRECEDING'"), Some("'PRECISION'"), Some("'PREPARE'"), Some("'PRIOR'"), 
		Some("'PROCEDURE'"), Some("'PRIVILEGES'"), Some("'PROPERTIES'"), Some("'PRUNE'"), 
		Some("'QUALIFY'"), Some("'QUARTER'"), Some("'QUOTES'"), Some("'RAISE'"), 
		Some("'RANGE'"), Some("'READ'"), Some("'RECURSIVE'"), Some("'REFRESH'"), 
		Some("'RENAME'"), Some("'REPEATABLE'"), Some("'REPLACE'"), Some("'RESET'"), 
		Some("'RESPECT'"), Some("'RESTRICT'"), Some("'RETURN'"), Some("'RETURNING'"), 
		Some("'REMOTE'"), Some("'REPEAT'"), Some("'RETURNS'"), Some("'REVOKE'"), 
		Some("'RIGHT'"), Some("'RLS'"), Some("'ROLE'"), Some("'ROLES'"), Some("'ROLLBACK'"), 
		Some("'ROLLUP'"), Some("'ROW'"), Some("'ROWS'"), Some("'RUNNING'"), Some("'SAFE'"), 
		Some("'SAFE_CAST'"), Some("'SATURDAY'"), Some("'SCALAR'"), Some("'SECOND'"), 
		Some("'SCHEMA'"), Some("'SCHEMAS'"), Some("'SECURITY'"), Some("'SEEK'"), 
		Some("'SELECT'"), Some("'SEMI'"), Some("'SERDE'"), Some("'SERDEPROPERTIES'"), 
		Some("'SERIALIZABLE'"), Some("'SESSION'"), Some("'SET'"), Some("'SETS'"), 
		Some("'SHOW'"), Some("'SIMILAR'"), Some("'SNAPSHOT'"), Some("'SOME'"), 
		Some("'SORTKEY'"), Some("'START'"), Some("'STATS'"), Some("'STORED'"), 
		Some("'STRUCT'"), Some("'SUBSET'"), Some("'SUBSTRING'"), Some("'SUNDAY'"), 
		Some("'SYSTEM'"), Some("'SYSTEM_TIME'"), Some("'TABLE'"), Some("'TABLES'"), 
		Some("'TABLESAMPLE'"), Some("'TEMP'"), Some("'TEMPORARY'"), Some("'TERMINATED'"), 
		Some("'TEXT'"), Some("'STRING'"), Some("'THEN'"), Some("'THURSDAY'"), 
		Some("'TIES'"), Some("'TIME'"), Some("'TIMESTAMP'"), Some("'TIMESTAMP_DIFF'"), 
		Some("'TO'"), Some("'TOP'"), Some("'TRAILING'"), Some("'TARGET'"), Some("'SOURCE'"), 
		Some("'TRAINING_DATA'"), Some("'TRANSACTION'"), Some("'TRANSFORM'"), Some("'TRIM'"), 
		Some("'TRUE'"), Some("'TRUNCATE'"), Some("'TRY_CAST'"), Some("'TUPLE'"), 
		Some("'TUESDAY'"), Some("'TYPE'"), Some("'UESCAPE'"), Some("'UNBOUNDED'"), 
		Some("'UNCOMMITTED'"), Some("'UNCONDITIONAL'"), Some("'UNION'"), Some("'UNKNOWN'"), 
		Some("'UNLOAD'"), Some("'UNMATCHED'"), Some("'UNNEST'"), Some("'UNPIVOT'"), 
		Some("'UNSIGNED'"), Some("'UNTIL'"), Some("'UPDATE'"), Some("'USE'"), 
		Some("'USER'"), Some("'USING'"), Some("'UTF16'"), Some("'UTF32'"), Some("'UTF8'"), 
		Some("'VACUUM'"), Some("'VALIDATE'"), Some("'VALUE'"), Some("'VALUES'"), 
		Some("'VARYING'"), Some("'VERBOSE'"), Some("'VERSION'"), Some("'VIEW'"), 
		Some("'WEDNESDAY'"), Some("'WEEK'"), Some("'WHEN'"), Some("'WHERE'"), 
		Some("'WHILE'"), Some("'WINDOW'"), Some("'WITH'"), Some("'WITHOUT'"), 
		Some("'WORK'"), Some("'WRAPPER'"), Some("'WRITE'"), Some("'XZ'"), Some("'YEAR'"), 
		Some("'YES'"), Some("'ZONE'"), Some("'ZSTD'"), Some("'('"), Some("')'"), 
		Some("'['"), Some("']'"), Some("'.'"), Some("'='"), None, Some("'<'"), 
		Some("'<='"), Some("'>'"), Some("'>='"), Some("'+'"), Some("'-'"), Some("'*'"), 
		Some("'/'"), Some("'%'"), Some("'||'"), Some("'?'"), Some("';'"), Some("':'"), 
		Some("'$'"), Some("'&'"), Some("'|'"), Some("'^'"), Some("'<<'"), Some("'~'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;442]  = [
		None, None, None, None, None, None, None, None, Some("ABORT"), Some("ABSENT"), 
		Some("ADD"), Some("ADMIN"), Some("AFTER"), Some("ALL"), Some("ALTER"), 
		Some("ANALYZE"), Some("AND"), Some("ANTI"), Some("ANY"), Some("ARRAY"), 
		Some("AS"), Some("ASC"), Some("AT"), Some("ATTACH"), Some("AUTHORIZATION"), 
		Some("AUTO"), Some("BACKUP"), Some("BEGIN"), Some("BERNOULLI"), Some("BETWEEN"), 
		Some("BOTH"), Some("BREAK"), Some("BY"), Some("BZIP2"), Some("CALL"), 
		Some("CANCEL"), Some("CASCADE"), Some("CASE"), Some("CASE_SENSITIVE"), 
		Some("CASE_INSENSITIVE"), Some("CAST"), Some("CATALOGS"), Some("CHARACTER"), 
		Some("CLONE"), Some("CLOSE"), Some("CLUSTER"), Some("COALESCE"), Some("COLLATE"), 
		Some("COLUMN"), Some("COLUMNS"), Some("COMMA"), Some("COMMENT"), Some("COMMIT"), 
		Some("COMMITTED"), Some("COMPOUND"), Some("COMPRESSION"), Some("CONDITIONAL"), 
		Some("CONNECT"), Some("CONNECTION"), Some("CONSTRAINT"), Some("CONTINUE"), 
		Some("COPARTITION"), Some("COPY"), Some("COUNT"), Some("CREATE"), Some("CROSS"), 
		Some("CUBE"), Some("CURRENT"), Some("CURRENT_ROLE"), Some("CUSTOM_HOLIDAY"), 
		Some("DATA"), Some("DATABASE"), Some("DATASHARE"), Some("DATE"), Some("DATETIME"), 
		Some("DAY"), Some("DAYOFWEEK"), Some("DAYOFYEAR"), Some("DATETIME_DIFF"), 
		Some("DATE_DIFF"), Some("DEALLOCATE"), Some("DECLARE"), Some("DEFAULT"), 
		Some("DEFAULTS"), Some("DEFINE"), Some("DEFINER"), Some("DELETE"), Some("DELIMITED"), 
		Some("DELIMITER"), Some("DENY"), Some("DESC"), Some("DESCRIBE"), Some("DESCRIPTOR"), 
		Some("DETERMINISTIC"), Some("DISTINCT"), Some("DISTKEY"), Some("DISTRIBUTED"), 
		Some("DISTSTYLE"), Some("DETACH"), Some("DO"), Some("DOUBLE"), Some("DROP"), 
		Some("ELSE"), Some("ELSEIF"), Some("EMPTY"), Some("ENCODE"), Some("ENCODING"), 
		Some("END"), Some("ERROR"), Some("ESCAPE"), Some("EVEN"), Some("EXCEPT"), 
		Some("EXCEPTION"), Some("EXCLUDE"), Some("EXCLUDING"), Some("EXECUTE"), 
		Some("EXISTS"), Some("EXPLAIN"), Some("EXTERNAL"), Some("EXTRACT"), Some("FALSE"), 
		Some("FETCH"), Some("FIELDS"), Some("FILTER"), Some("FINAL"), Some("FIRST"), 
		Some("FOLLOWING"), Some("FOR"), Some("FORMAT"), Some("FRIDAY"), Some("FROM"), 
		Some("FULL"), Some("FUNCTION"), Some("FUNCTIONS"), Some("GENERATED"), 
		Some("GRACE"), Some("GRANT"), Some("GRANTED"), Some("GRANTS"), Some("GRAPHVIZ"), 
		Some("GROUP"), Some("GROUPING"), Some("GROUPS"), Some("GZIP"), Some("HAVING"), 
		Some("HEADER"), Some("HOUR"), Some("IDENTITY"), Some("IF"), Some("IGNORE"), 
		Some("IMMEDIATE"), Some("IN"), Some("INCLUDE"), Some("INCLUDING"), Some("INITIAL"), 
		Some("INNER"), Some("INPUT"), Some("INPUTFORMAT"), Some("INTERLEAVED"), 
		Some("INSERT"), Some("INTERSECT"), Some("INTERVAL"), Some("INTO"), Some("INVOKER"), 
		Some("IO"), Some("IS"), Some("ISOLATION"), Some("ISOWEEK"), Some("ISOYEAR"), 
		Some("ITERATE"), Some("ILIKE"), Some("JOIN"), Some("JSON"), Some("KEEP"), 
		Some("KEY"), Some("KEYS"), Some("LAMBDA"), Some("LANGUAGE"), Some("LEAVE"), 
		Some("LAST"), Some("LATERAL"), Some("LEADING"), Some("LEFT"), Some("LEVEL"), 
		Some("LIBRARY"), Some("LIKE"), Some("LIMIT"), Some("LINES"), Some("LISTAGG"), 
		Some("LOCAL"), Some("LOCATION"), Some("LOCK"), Some("LOGICAL"), Some("LOOP"), 
		Some("MAP"), Some("MASKING"), Some("MATCH"), Some("MATCHED"), Some("MATCHES"), 
		Some("MATCH_RECOGNIZE"), Some("MATERIALIZED"), Some("MAX"), Some("MEASURES"), 
		Some("MERGE"), Some("MESSAGE"), Some("MICROSECOND"), Some("MILLISECOND"), 
		Some("MIN"), Some("MINUS_KW"), Some("MINUTE"), Some("MODEL"), Some("MONDAY"), 
		Some("MONTH"), Some("NAME"), Some("NATURAL"), Some("NEXT"), Some("NFC"), 
		Some("NFD"), Some("NFKC"), Some("NFKD"), Some("NO"), Some("NONE"), Some("NORMALIZE"), 
		Some("NOT"), Some("NULL"), Some("NULLS"), Some("OBJECT"), Some("OF"), 
		Some("OFFSET"), Some("OMIT"), Some("ON"), Some("ONE"), Some("ONLY"), Some("OPTION"), 
		Some("OPTIONS"), Some("OR"), Some("ORDER"), Some("OUTER"), Some("OUTPUT"), 
		Some("OUTPUTFORMAT"), Some("OVER"), Some("OVERFLOW"), Some("PARTITION"), 
		Some("PARTITIONED"), Some("PARTITIONS"), Some("PASSING"), Some("PAST"), 
		Some("PATH"), Some("PATTERN"), Some("PER"), Some("PERCENT_KW"), Some("PERIOD"), 
		Some("PERMUTE"), Some("PIVOT"), Some("POSITION"), Some("PRECEDING"), Some("PRECISION"), 
		Some("PREPARE"), Some("PRIOR"), Some("PROCEDURE"), Some("PRIVILEGES"), 
		Some("PROPERTIES"), Some("PRUNE"), Some("QUALIFY"), Some("QUARTER"), Some("QUOTES"), 
		Some("RAISE"), Some("RANGE"), Some("READ"), Some("RECURSIVE"), Some("REFRESH"), 
		Some("RENAME"), Some("REPEATABLE"), Some("REPLACE"), Some("RESET"), Some("RESPECT"), 
		Some("RESTRICT"), Some("RETURN"), Some("RETURNING"), Some("REMOTE"), Some("REPEAT"), 
		Some("RETURNS"), Some("REVOKE"), Some("RIGHT"), Some("RLS"), Some("ROLE"), 
		Some("ROLES"), Some("ROLLBACK"), Some("ROLLUP"), Some("ROW"), Some("ROWS"), 
		Some("RUNNING"), Some("SAFE"), Some("SAFE_CAST"), Some("SATURDAY"), Some("SCALAR"), 
		Some("SECOND"), Some("SCHEMA"), Some("SCHEMAS"), Some("SECURITY"), Some("SEEK"), 
		Some("SELECT"), Some("SEMI"), Some("SERDE"), Some("SERDEPROPERTIES"), 
		Some("SERIALIZABLE"), Some("SESSION"), Some("SET"), Some("SETS"), Some("SHOW"), 
		Some("SIMILAR"), Some("SNAPSHOT"), Some("SOME"), Some("SORTKEY"), Some("START"), 
		Some("STATS"), Some("STORED"), Some("STRUCT"), Some("SUBSET"), Some("SUBSTRING"), 
		Some("SUNDAY"), Some("SYSTEM"), Some("SYSTEM_TIME"), Some("TABLE"), Some("TABLES"), 
		Some("TABLESAMPLE"), Some("TEMP"), Some("TEMPORARY"), Some("TERMINATED"), 
		Some("TEXT"), Some("STRING_KW"), Some("THEN"), Some("THURSDAY"), Some("TIES"), 
		Some("TIME"), Some("TIMESTAMP"), Some("TIMESTAMP_DIFF"), Some("TO"), Some("TOP"), 
		Some("TRAILING"), Some("TARGET"), Some("SOURCE"), Some("TRAINING_DATA"), 
		Some("TRANSACTION"), Some("TRANSFORM"), Some("TRIM"), Some("TRUE"), Some("TRUNCATE"), 
		Some("TRY_CAST"), Some("TUPLE"), Some("TUESDAY"), Some("TYPE"), Some("UESCAPE"), 
		Some("UNBOUNDED"), Some("UNCOMMITTED"), Some("UNCONDITIONAL"), Some("UNION"), 
		Some("UNKNOWN"), Some("UNLOAD"), Some("UNMATCHED"), Some("UNNEST"), Some("UNPIVOT"), 
		Some("UNSIGNED"), Some("UNTIL"), Some("UPDATE"), Some("USE"), Some("USER"), 
		Some("USING"), Some("UTF16"), Some("UTF32"), Some("UTF8"), Some("VACUUM"), 
		Some("VALIDATE"), Some("VALUE"), Some("VALUES"), Some("VARYING"), Some("VERBOSE"), 
		Some("VERSION"), Some("VIEW"), Some("WEDNESDAY"), Some("WEEK"), Some("WHEN"), 
		Some("WHERE"), Some("WHILE"), Some("WINDOW"), Some("WITH"), Some("WITHOUT"), 
		Some("WORK"), Some("WRAPPER"), Some("WRITE"), Some("XZ"), Some("YEAR"), 
		Some("YES"), Some("ZONE"), Some("ZSTD"), Some("LPAREN"), Some("RPAREN"), 
		Some("LBRACKET"), Some("RBRACKET"), Some("DOT"), Some("EQ"), Some("NEQ"), 
		Some("LT"), Some("LTE"), Some("GT"), Some("GTE"), Some("PLUS"), Some("MINUS"), 
		Some("ASTERISK"), Some("SLASH"), Some("PERCENT"), Some("CONCAT"), Some("QUESTION_MARK"), 
		Some("SEMI_COLON"), Some("COLON"), Some("DOLLAR"), Some("BITWISE_AND"), 
		Some("BITWISE_OR"), Some("BITWISE_XOR"), Some("BITWISE_SHIFT_LEFT"), Some("POSIX"), 
		Some("ESCAPE_SEQUENCE"), Some("QUOTED_STRING"), Some("TRIPLE_QUOTED_STRING"), 
		Some("RAW_QUOTED_STRING"), Some("RAW_TRIPLE_QUOTED_STRING"), Some("BINARY_LITERAL"), 
		Some("INTEGER_VALUE"), Some("HEXADECIMAL_VALUE"), Some("DECIMAL_VALUE"), 
		Some("DOUBLE_VALUE"), Some("IDENTIFIER"), Some("BACKQUOTED_IDENTIFIER"), 
		Some("SYSTEM_VARIABLE"), Some("VARIABLE"), Some("SIMPLE_COMMENT"), Some("BIG_QUERY_SIMPLE_COMMENT"), 
		Some("BRACKETED_COMMENT"), Some("WS"), Some("OTHER_WS"), Some("UNPAIRED_TOKEN"), 
		Some("UNRECOGNIZED")
	];

	static _shared_context_cache: LazyLock<PredictionContextCache> = LazyLock::new(|| PredictionContextCache::new());
	static VOCABULARY: LazyLock<Box<dyn Vocabulary>> = LazyLock::new(|| Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None)));



pub type LexerContext<'input> = BaseRuleContext<'input,EmptyCustomRuleContext<'input,LocalTokenFactory<'input> >>;

pub type LocalTokenFactory<'input> = dbt_antlr4::token_factory::ArenaCommonFactory<'input>;

type From<'a> = <LocalTokenFactory<'a> as TokenFactory<'a> >::From;

pub struct BigqueryLexer<'input, Input:CharStream<From<'input> >> {
	base: BaseLexer<'input,BigqueryLexerActions,Input,LocalTokenFactory<'input>>,
}

dbt_antlr4::tid! { impl<'input,Input> TidAble<'input> for BigqueryLexer<'input,Input> where Input:CharStream<From<'input> > }

impl<'input, Input:CharStream<From<'input> >> Deref for BigqueryLexer<'input,Input>{
	type Target = BaseLexer<'input,BigqueryLexerActions,Input,LocalTokenFactory<'input>>;

	fn deref(&self) -> &Self::Target {
		&self.base
	}
}

impl<'input, Input:CharStream<From<'input> >> DerefMut for BigqueryLexer<'input,Input>{
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.base
	}
}


impl<'input, Input:CharStream<From<'input> >> BigqueryLexer<'input,Input>{
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
        "BigqueryLexer.g4"
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
				BigqueryLexerActions{},
				tf
			)
	    }
	}
}

impl<'input, Input:CharStream<From<'input> >> BigqueryLexer<'input,Input> where &'input LocalTokenFactory<'input>:Default{
	pub fn new(input: Input) -> Self{
		BigqueryLexer::new_with_token_factory(input, <&LocalTokenFactory<'input> as Default>::default())
	}
}

pub struct BigqueryLexerActions {
}

impl BigqueryLexerActions{
}

impl<'input, Input:CharStream<From<'input> >> Actions<'input,BaseLexer<'input,BigqueryLexerActions,Input,LocalTokenFactory<'input>>> for BigqueryLexerActions{
	}

	impl<'input, Input:CharStream<From<'input> >> BigqueryLexer<'input,Input>{

}

impl<'input, Input:CharStream<From<'input> >> LexerRecog<'input,BaseLexer<'input,BigqueryLexerActions,Input,LocalTokenFactory<'input>>> for BigqueryLexerActions{
}
impl<'input> TokenAware<'input> for BigqueryLexerActions{
	type TF = LocalTokenFactory<'input>;
}

impl<'input, Input:CharStream<From<'input> >> TokenSource<'input> for BigqueryLexer<'input,Input>{
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
	    4, 0, 441, 4238, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 
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
	    439, 2, 440, 7, 440, 2, 441, 7, 441, 2, 442, 7, 442, 2, 443, 7, 443, 
	    1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 2, 1, 3, 1, 3, 1, 3, 1, 
	    4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 
	    1, 7, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 8, 1, 9, 1, 9, 1, 9, 1, 
	    9, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 11, 1, 11, 1, 11, 1, 
	    11, 1, 11, 1, 11, 1, 12, 1, 12, 1, 12, 1, 12, 1, 13, 1, 13, 1, 13, 
	    1, 13, 1, 13, 1, 13, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 
	    1, 14, 1, 15, 1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 1, 16, 1, 16, 
	    1, 17, 1, 17, 1, 17, 1, 17, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 
	    1, 19, 1, 19, 1, 19, 1, 20, 1, 20, 1, 20, 1, 20, 1, 21, 1, 21, 1, 21, 
	    1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 1, 22, 1, 23, 1, 23, 1, 23, 
	    1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 1, 23, 
	    1, 23, 1, 24, 1, 24, 1, 24, 1, 24, 1, 24, 1, 25, 1, 25, 1, 25, 1, 25, 
	    1, 25, 1, 25, 1, 25, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 26, 1, 27, 
	    1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 27, 1, 28, 
	    1, 28, 1, 28, 1, 28, 1, 28, 1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 1, 29, 
	    1, 29, 1, 29, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 30, 1, 31, 1, 31, 
	    1, 31, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 33, 1, 33, 1, 33, 
	    1, 33, 1, 33, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 35, 
	    1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 36, 1, 36, 1, 36, 
	    1, 36, 1, 36, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 
	    1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 37, 1, 38, 1, 38, 1, 38, 
	    1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 1, 38, 
	    1, 38, 1, 38, 1, 38, 1, 38, 1, 39, 1, 39, 1, 39, 1, 39, 1, 39, 1, 40, 
	    1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 41, 1, 41, 
	    1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 42, 1, 42, 
	    1, 42, 1, 42, 1, 42, 1, 42, 1, 43, 1, 43, 1, 43, 1, 43, 1, 43, 1, 43, 
	    1, 44, 1, 44, 1, 44, 1, 44, 1, 44, 1, 44, 1, 44, 1, 44, 1, 45, 1, 45, 
	    1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 46, 1, 46, 1, 46, 
	    1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 47, 1, 47, 1, 47, 1, 47, 1, 47, 
	    1, 47, 1, 47, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 1, 48, 
	    1, 49, 1, 49, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 
	    1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 1, 52, 1, 52, 1, 52, 
	    1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 53, 1, 53, 1, 53, 
	    1, 53, 1, 53, 1, 53, 1, 53, 1, 53, 1, 53, 1, 54, 1, 54, 1, 54, 1, 54, 
	    1, 54, 1, 54, 1, 54, 1, 54, 1, 54, 1, 54, 1, 54, 1, 54, 1, 55, 1, 55, 
	    1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 1, 55, 
	    1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 56, 1, 57, 1, 57, 
	    1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 57, 1, 58, 
	    1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 1, 58, 
	    1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 59, 1, 60, 
	    1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 1, 60, 
	    1, 60, 1, 61, 1, 61, 1, 61, 1, 61, 1, 61, 1, 62, 1, 62, 1, 62, 1, 62, 
	    1, 62, 1, 62, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 64, 
	    1, 64, 1, 64, 1, 64, 1, 64, 1, 64, 1, 65, 1, 65, 1, 65, 1, 65, 1, 65, 
	    1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 66, 1, 67, 1, 67, 
	    1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 1, 67, 
	    1, 67, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 
	    1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 68, 1, 69, 1, 69, 1, 69, 1, 69, 
	    1, 69, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 1, 70, 
	    1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 1, 71, 
	    1, 72, 1, 72, 1, 72, 1, 72, 1, 72, 1, 73, 1, 73, 1, 73, 1, 73, 1, 73, 
	    1, 73, 1, 73, 1, 73, 1, 73, 1, 74, 1, 74, 1, 74, 1, 74, 1, 75, 1, 75, 
	    1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 75, 1, 76, 1, 76, 
	    1, 76, 1, 76, 1, 76, 1, 76, 1, 76, 1, 76, 1, 76, 1, 76, 1, 77, 1, 77, 
	    1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 1, 77, 
	    1, 77, 1, 77, 1, 78, 1, 78, 1, 78, 1, 78, 1, 78, 1, 78, 1, 78, 1, 78, 
	    1, 78, 1, 78, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 1, 79, 
	    1, 79, 1, 79, 1, 79, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 1, 80, 
	    1, 80, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 81, 1, 82, 
	    1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 82, 1, 83, 1, 83, 
	    1, 83, 1, 83, 1, 83, 1, 83, 1, 83, 1, 84, 1, 84, 1, 84, 1, 84, 1, 84, 
	    1, 84, 1, 84, 1, 84, 1, 85, 1, 85, 1, 85, 1, 85, 1, 85, 1, 85, 1, 85, 
	    1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 1, 86, 
	    1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 1, 87, 
	    1, 88, 1, 88, 1, 88, 1, 88, 1, 88, 1, 89, 1, 89, 1, 89, 1, 89, 1, 89, 
	    1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 90, 1, 91, 
	    1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 1, 91, 
	    1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 1, 92, 
	    1, 92, 1, 92, 1, 92, 1, 92, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 1, 93, 
	    1, 93, 1, 93, 1, 93, 1, 94, 1, 94, 1, 94, 1, 94, 1, 94, 1, 94, 1, 94, 
	    1, 94, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 1, 95, 
	    1, 95, 1, 95, 1, 95, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 1, 96, 
	    1, 96, 1, 96, 1, 96, 1, 97, 1, 97, 1, 97, 1, 97, 1, 97, 1, 97, 1, 97, 
	    1, 98, 1, 98, 1, 98, 1, 99, 1, 99, 1, 99, 1, 99, 1, 99, 1, 99, 1, 99, 
	    1, 100, 1, 100, 1, 100, 1, 100, 1, 100, 1, 101, 1, 101, 1, 101, 1, 
	    101, 1, 101, 1, 102, 1, 102, 1, 102, 1, 102, 1, 102, 1, 102, 1, 102, 
	    1, 103, 1, 103, 1, 103, 1, 103, 1, 103, 1, 103, 1, 104, 1, 104, 1, 
	    104, 1, 104, 1, 104, 1, 104, 1, 104, 1, 105, 1, 105, 1, 105, 1, 105, 
	    1, 105, 1, 105, 1, 105, 1, 105, 1, 105, 1, 106, 1, 106, 1, 106, 1, 
	    106, 1, 107, 1, 107, 1, 107, 1, 107, 1, 107, 1, 107, 1, 108, 1, 108, 
	    1, 108, 1, 108, 1, 108, 1, 108, 1, 108, 1, 109, 1, 109, 1, 109, 1, 
	    109, 1, 109, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 1, 110, 
	    1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 111, 1, 
	    111, 1, 111, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 1, 112, 
	    1, 112, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 113, 1, 
	    113, 1, 113, 1, 113, 1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 1, 114, 
	    1, 114, 1, 114, 1, 115, 1, 115, 1, 115, 1, 115, 1, 115, 1, 115, 1, 
	    115, 1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 1, 116, 
	    1, 117, 1, 117, 1, 117, 1, 117, 1, 117, 1, 117, 1, 117, 1, 117, 1, 
	    117, 1, 118, 1, 118, 1, 118, 1, 118, 1, 118, 1, 118, 1, 118, 1, 118, 
	    1, 119, 1, 119, 1, 119, 1, 119, 1, 119, 1, 119, 1, 120, 1, 120, 1, 
	    120, 1, 120, 1, 120, 1, 120, 1, 121, 1, 121, 1, 121, 1, 121, 1, 121, 
	    1, 121, 1, 121, 1, 122, 1, 122, 1, 122, 1, 122, 1, 122, 1, 122, 1, 
	    122, 1, 123, 1, 123, 1, 123, 1, 123, 1, 123, 1, 123, 1, 124, 1, 124, 
	    1, 124, 1, 124, 1, 124, 1, 124, 1, 125, 1, 125, 1, 125, 1, 125, 1, 
	    125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 125, 1, 126, 1, 126, 1, 126, 
	    1, 126, 1, 127, 1, 127, 1, 127, 1, 127, 1, 127, 1, 127, 1, 127, 1, 
	    128, 1, 128, 1, 128, 1, 128, 1, 128, 1, 128, 1, 128, 1, 129, 1, 129, 
	    1, 129, 1, 129, 1, 129, 1, 130, 1, 130, 1, 130, 1, 130, 1, 130, 1, 
	    131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 1, 131, 
	    1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 132, 1, 
	    132, 1, 132, 1, 133, 1, 133, 1, 133, 1, 133, 1, 133, 1, 133, 1, 133, 
	    1, 133, 1, 133, 1, 133, 1, 134, 1, 134, 1, 134, 1, 134, 1, 134, 1, 
	    134, 1, 135, 1, 135, 1, 135, 1, 135, 1, 135, 1, 135, 1, 136, 1, 136, 
	    1, 136, 1, 136, 1, 136, 1, 136, 1, 136, 1, 136, 1, 137, 1, 137, 1, 
	    137, 1, 137, 1, 137, 1, 137, 1, 137, 1, 138, 1, 138, 1, 138, 1, 138, 
	    1, 138, 1, 138, 1, 138, 1, 138, 1, 138, 1, 139, 1, 139, 1, 139, 1, 
	    139, 1, 139, 1, 139, 1, 140, 1, 140, 1, 140, 1, 140, 1, 140, 1, 140, 
	    1, 140, 1, 140, 1, 140, 1, 141, 1, 141, 1, 141, 1, 141, 1, 141, 1, 
	    141, 1, 141, 1, 142, 1, 142, 1, 142, 1, 142, 1, 142, 1, 143, 1, 143, 
	    1, 143, 1, 143, 1, 143, 1, 143, 1, 143, 1, 144, 1, 144, 1, 144, 1, 
	    144, 1, 144, 1, 144, 1, 144, 1, 145, 1, 145, 1, 145, 1, 145, 1, 145, 
	    1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 146, 1, 
	    146, 1, 147, 1, 147, 1, 147, 1, 148, 1, 148, 1, 148, 1, 148, 1, 148, 
	    1, 148, 1, 148, 1, 149, 1, 149, 1, 149, 1, 149, 1, 149, 1, 149, 1, 
	    149, 1, 149, 1, 149, 1, 149, 1, 150, 1, 150, 1, 150, 1, 151, 1, 151, 
	    1, 151, 1, 151, 1, 151, 1, 151, 1, 151, 1, 151, 1, 152, 1, 152, 1, 
	    152, 1, 152, 1, 152, 1, 152, 1, 152, 1, 152, 1, 152, 1, 152, 1, 153, 
	    1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 153, 1, 154, 1, 
	    154, 1, 154, 1, 154, 1, 154, 1, 154, 1, 155, 1, 155, 1, 155, 1, 155, 
	    1, 155, 1, 155, 1, 156, 1, 156, 1, 156, 1, 156, 1, 156, 1, 156, 1, 
	    156, 1, 156, 1, 156, 1, 156, 1, 156, 1, 156, 1, 157, 1, 157, 1, 157, 
	    1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 1, 157, 1, 
	    157, 1, 158, 1, 158, 1, 158, 1, 158, 1, 158, 1, 158, 1, 158, 1, 159, 
	    1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 159, 1, 
	    159, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 1, 160, 
	    1, 160, 1, 161, 1, 161, 1, 161, 1, 161, 1, 161, 1, 162, 1, 162, 1, 
	    162, 1, 162, 1, 162, 1, 162, 1, 162, 1, 162, 1, 163, 1, 163, 1, 163, 
	    1, 164, 1, 164, 1, 164, 1, 165, 1, 165, 1, 165, 1, 165, 1, 165, 1, 
	    165, 1, 165, 1, 165, 1, 165, 1, 165, 1, 166, 1, 166, 1, 166, 1, 166, 
	    1, 166, 1, 166, 1, 166, 1, 166, 1, 167, 1, 167, 1, 167, 1, 167, 1, 
	    167, 1, 167, 1, 167, 1, 167, 1, 168, 1, 168, 1, 168, 1, 168, 1, 168, 
	    1, 168, 1, 168, 1, 168, 1, 169, 1, 169, 1, 169, 1, 169, 1, 169, 1, 
	    169, 1, 170, 1, 170, 1, 170, 1, 170, 1, 170, 1, 171, 1, 171, 1, 171, 
	    1, 171, 1, 171, 1, 172, 1, 172, 1, 172, 1, 172, 1, 172, 1, 173, 1, 
	    173, 1, 173, 1, 173, 1, 174, 1, 174, 1, 174, 1, 174, 1, 174, 1, 175, 
	    1, 175, 1, 175, 1, 175, 1, 175, 1, 175, 1, 175, 1, 176, 1, 176, 1, 
	    176, 1, 176, 1, 176, 1, 176, 1, 176, 1, 176, 1, 176, 1, 177, 1, 177, 
	    1, 177, 1, 177, 1, 177, 1, 177, 1, 178, 1, 178, 1, 178, 1, 178, 1, 
	    178, 1, 179, 1, 179, 1, 179, 1, 179, 1, 179, 1, 179, 1, 179, 1, 179, 
	    1, 180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 180, 1, 
	    181, 1, 181, 1, 181, 1, 181, 1, 181, 1, 182, 1, 182, 1, 182, 1, 182, 
	    1, 182, 1, 182, 1, 183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 183, 1, 
	    183, 1, 183, 1, 184, 1, 184, 1, 184, 1, 184, 1, 184, 1, 185, 1, 185, 
	    1, 185, 1, 185, 1, 185, 1, 185, 1, 186, 1, 186, 1, 186, 1, 186, 1, 
	    186, 1, 186, 1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 1, 187, 
	    1, 187, 1, 188, 1, 188, 1, 188, 1, 188, 1, 188, 1, 188, 1, 189, 1, 
	    189, 1, 189, 1, 189, 1, 189, 1, 189, 1, 189, 1, 189, 1, 189, 1, 190, 
	    1, 190, 1, 190, 1, 190, 1, 190, 1, 191, 1, 191, 1, 191, 1, 191, 1, 
	    191, 1, 191, 1, 191, 1, 191, 1, 192, 1, 192, 1, 192, 1, 192, 1, 192, 
	    1, 193, 1, 193, 1, 193, 1, 193, 1, 194, 1, 194, 1, 194, 1, 194, 1, 
	    194, 1, 194, 1, 194, 1, 194, 1, 195, 1, 195, 1, 195, 1, 195, 1, 195, 
	    1, 195, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 196, 1, 
	    196, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 1, 197, 
	    1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 
	    198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 198, 1, 199, 
	    1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 199, 1, 
	    199, 1, 199, 1, 199, 1, 199, 1, 200, 1, 200, 1, 200, 1, 200, 1, 201, 
	    1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 201, 1, 
	    202, 1, 202, 1, 202, 1, 202, 1, 202, 1, 202, 1, 203, 1, 203, 1, 203, 
	    1, 203, 1, 203, 1, 203, 1, 203, 1, 203, 1, 204, 1, 204, 1, 204, 1, 
	    204, 1, 204, 1, 204, 1, 204, 1, 204, 1, 204, 1, 204, 1, 204, 1, 204, 
	    1, 205, 1, 205, 1, 205, 1, 205, 1, 205, 1, 205, 1, 205, 1, 205, 1, 
	    205, 1, 205, 1, 205, 1, 205, 1, 206, 1, 206, 1, 206, 1, 206, 1, 207, 
	    1, 207, 1, 207, 1, 207, 1, 207, 1, 207, 1, 208, 1, 208, 1, 208, 1, 
	    208, 1, 208, 1, 208, 1, 208, 1, 209, 1, 209, 1, 209, 1, 209, 1, 209, 
	    1, 209, 1, 210, 1, 210, 1, 210, 1, 210, 1, 210, 1, 210, 1, 210, 1, 
	    211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 211, 1, 212, 1, 212, 1, 212, 
	    1, 212, 1, 212, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 213, 1, 
	    213, 1, 213, 1, 214, 1, 214, 1, 214, 1, 214, 1, 214, 1, 215, 1, 215, 
	    1, 215, 1, 215, 1, 216, 1, 216, 1, 216, 1, 216, 1, 217, 1, 217, 1, 
	    217, 1, 217, 1, 217, 1, 218, 1, 218, 1, 218, 1, 218, 1, 218, 1, 219, 
	    1, 219, 1, 219, 1, 220, 1, 220, 1, 220, 1, 220, 1, 220, 1, 221, 1, 
	    221, 1, 221, 1, 221, 1, 221, 1, 221, 1, 221, 1, 221, 1, 221, 1, 221, 
	    1, 222, 1, 222, 1, 222, 1, 222, 1, 223, 1, 223, 1, 223, 1, 223, 1, 
	    223, 1, 224, 1, 224, 1, 224, 1, 224, 1, 224, 1, 224, 1, 225, 1, 225, 
	    1, 225, 1, 225, 1, 225, 1, 225, 1, 225, 1, 226, 1, 226, 1, 226, 1, 
	    227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 227, 1, 228, 1, 228, 
	    1, 228, 1, 228, 1, 228, 1, 229, 1, 229, 1, 229, 1, 230, 1, 230, 1, 
	    230, 1, 230, 1, 231, 1, 231, 1, 231, 1, 231, 1, 231, 1, 232, 1, 232, 
	    1, 232, 1, 232, 1, 232, 1, 232, 1, 232, 1, 233, 1, 233, 1, 233, 1, 
	    233, 1, 233, 1, 233, 1, 233, 1, 233, 1, 234, 1, 234, 1, 234, 1, 235, 
	    1, 235, 1, 235, 1, 235, 1, 235, 1, 235, 1, 236, 1, 236, 1, 236, 1, 
	    236, 1, 236, 1, 236, 1, 237, 1, 237, 1, 237, 1, 237, 1, 237, 1, 237, 
	    1, 237, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 
	    238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 238, 1, 239, 1, 239, 1, 239, 
	    1, 239, 1, 239, 1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 240, 1, 
	    240, 1, 240, 1, 240, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 1, 241, 
	    1, 241, 1, 241, 1, 241, 1, 241, 1, 242, 1, 242, 1, 242, 1, 242, 1, 
	    242, 1, 242, 1, 242, 1, 242, 1, 242, 1, 242, 1, 242, 1, 242, 1, 243, 
	    1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 243, 1, 
	    243, 1, 243, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 1, 244, 
	    1, 244, 1, 245, 1, 245, 1, 245, 1, 245, 1, 245, 1, 246, 1, 246, 1, 
	    246, 1, 246, 1, 246, 1, 247, 1, 247, 1, 247, 1, 247, 1, 247, 1, 247, 
	    1, 247, 1, 247, 1, 248, 1, 248, 1, 248, 1, 248, 1, 249, 1, 249, 1, 
	    249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 249, 1, 250, 1, 250, 1, 250, 
	    1, 250, 1, 250, 1, 250, 1, 250, 1, 251, 1, 251, 1, 251, 1, 251, 1, 
	    251, 1, 251, 1, 251, 1, 251, 1, 252, 1, 252, 1, 252, 1, 252, 1, 252, 
	    1, 252, 1, 253, 1, 253, 1, 253, 1, 253, 1, 253, 1, 253, 1, 253, 1, 
	    253, 1, 253, 1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 1, 254, 
	    1, 254, 1, 254, 1, 254, 1, 255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 
	    255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 256, 1, 256, 1, 256, 1, 256, 
	    1, 256, 1, 256, 1, 256, 1, 256, 1, 257, 1, 257, 1, 257, 1, 257, 1, 
	    257, 1, 257, 1, 258, 1, 258, 1, 258, 1, 258, 1, 258, 1, 258, 1, 258, 
	    1, 258, 1, 258, 1, 258, 1, 259, 1, 259, 1, 259, 1, 259, 1, 259, 1, 
	    259, 1, 259, 1, 259, 1, 259, 1, 259, 1, 259, 1, 260, 1, 260, 1, 260, 
	    1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 260, 1, 
	    261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 261, 1, 262, 1, 262, 1, 262, 
	    1, 262, 1, 262, 1, 262, 1, 262, 1, 262, 1, 263, 1, 263, 1, 263, 1, 
	    263, 1, 263, 1, 263, 1, 263, 1, 263, 1, 264, 1, 264, 1, 264, 1, 264, 
	    1, 264, 1, 264, 1, 264, 1, 265, 1, 265, 1, 265, 1, 265, 1, 265, 1, 
	    265, 1, 266, 1, 266, 1, 266, 1, 266, 1, 266, 1, 266, 1, 267, 1, 267, 
	    1, 267, 1, 267, 1, 267, 1, 268, 1, 268, 1, 268, 1, 268, 1, 268, 1, 
	    268, 1, 268, 1, 268, 1, 268, 1, 268, 1, 269, 1, 269, 1, 269, 1, 269, 
	    1, 269, 1, 269, 1, 269, 1, 269, 1, 270, 1, 270, 1, 270, 1, 270, 1, 
	    270, 1, 270, 1, 270, 1, 271, 1, 271, 1, 271, 1, 271, 1, 271, 1, 271, 
	    1, 271, 1, 271, 1, 271, 1, 271, 1, 271, 1, 272, 1, 272, 1, 272, 1, 
	    272, 1, 272, 1, 272, 1, 272, 1, 272, 1, 273, 1, 273, 1, 273, 1, 273, 
	    1, 273, 1, 273, 1, 274, 1, 274, 1, 274, 1, 274, 1, 274, 1, 274, 1, 
	    274, 1, 274, 1, 275, 1, 275, 1, 275, 1, 275, 1, 275, 1, 275, 1, 275, 
	    1, 275, 1, 275, 1, 276, 1, 276, 1, 276, 1, 276, 1, 276, 1, 276, 1, 
	    276, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 1, 277, 
	    1, 277, 1, 277, 1, 278, 1, 278, 1, 278, 1, 278, 1, 278, 1, 278, 1, 
	    278, 1, 279, 1, 279, 1, 279, 1, 279, 1, 279, 1, 279, 1, 279, 1, 280, 
	    1, 280, 1, 280, 1, 280, 1, 280, 1, 280, 1, 280, 1, 280, 1, 281, 1, 
	    281, 1, 281, 1, 281, 1, 281, 1, 281, 1, 281, 1, 282, 1, 282, 1, 282, 
	    1, 282, 1, 282, 1, 282, 1, 283, 1, 283, 1, 283, 1, 283, 1, 284, 1, 
	    284, 1, 284, 1, 284, 1, 284, 1, 285, 1, 285, 1, 285, 1, 285, 1, 285, 
	    1, 285, 1, 286, 1, 286, 1, 286, 1, 286, 1, 286, 1, 286, 1, 286, 1, 
	    286, 1, 286, 1, 287, 1, 287, 1, 287, 1, 287, 1, 287, 1, 287, 1, 287, 
	    1, 288, 1, 288, 1, 288, 1, 288, 1, 289, 1, 289, 1, 289, 1, 289, 1, 
	    289, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 1, 290, 
	    1, 291, 1, 291, 1, 291, 1, 291, 1, 291, 1, 292, 1, 292, 1, 292, 1, 
	    292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 292, 1, 293, 1, 293, 
	    1, 293, 1, 293, 1, 293, 1, 293, 1, 293, 1, 293, 1, 293, 1, 294, 1, 
	    294, 1, 294, 1, 294, 1, 294, 1, 294, 1, 294, 1, 295, 1, 295, 1, 295, 
	    1, 295, 1, 295, 1, 295, 1, 295, 1, 296, 1, 296, 1, 296, 1, 296, 1, 
	    296, 1, 296, 1, 296, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 1, 297, 
	    1, 297, 1, 297, 1, 298, 1, 298, 1, 298, 1, 298, 1, 298, 1, 298, 1, 
	    298, 1, 298, 1, 298, 1, 299, 1, 299, 1, 299, 1, 299, 1, 299, 1, 300, 
	    1, 300, 1, 300, 1, 300, 1, 300, 1, 300, 1, 300, 1, 301, 1, 301, 1, 
	    301, 1, 301, 1, 301, 1, 302, 1, 302, 1, 302, 1, 302, 1, 302, 1, 302, 
	    1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 
	    303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 303, 1, 304, 
	    1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 304, 1, 
	    304, 1, 304, 1, 304, 1, 304, 1, 305, 1, 305, 1, 305, 1, 305, 1, 305, 
	    1, 305, 1, 305, 1, 305, 1, 306, 1, 306, 1, 306, 1, 306, 1, 307, 1, 
	    307, 1, 307, 1, 307, 1, 307, 1, 308, 1, 308, 1, 308, 1, 308, 1, 308, 
	    1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 309, 1, 
	    310, 1, 310, 1, 310, 1, 310, 1, 310, 1, 310, 1, 310, 1, 310, 1, 310, 
	    1, 311, 1, 311, 1, 311, 1, 311, 1, 311, 1, 312, 1, 312, 1, 312, 1, 
	    312, 1, 312, 1, 312, 1, 312, 1, 312, 1, 313, 1, 313, 1, 313, 1, 313, 
	    1, 313, 1, 313, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 1, 314, 1, 
	    315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 315, 1, 316, 1, 316, 
	    1, 316, 1, 316, 1, 316, 1, 316, 1, 316, 1, 317, 1, 317, 1, 317, 1, 
	    317, 1, 317, 1, 317, 1, 317, 1, 318, 1, 318, 1, 318, 1, 318, 1, 318, 
	    1, 318, 1, 318, 1, 318, 1, 318, 1, 318, 1, 319, 1, 319, 1, 319, 1, 
	    319, 1, 319, 1, 319, 1, 319, 1, 320, 1, 320, 1, 320, 1, 320, 1, 320, 
	    1, 320, 1, 320, 1, 321, 1, 321, 1, 321, 1, 321, 1, 321, 1, 321, 1, 
	    321, 1, 321, 1, 321, 1, 321, 1, 321, 1, 321, 1, 322, 1, 322, 1, 322, 
	    1, 322, 1, 322, 1, 322, 1, 323, 1, 323, 1, 323, 1, 323, 1, 323, 1, 
	    323, 1, 323, 1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 
	    1, 324, 1, 324, 1, 324, 1, 324, 1, 324, 1, 325, 1, 325, 1, 325, 1, 
	    325, 1, 325, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 1, 326, 
	    1, 326, 1, 326, 1, 326, 1, 327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 
	    327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 327, 1, 328, 1, 328, 1, 328, 
	    1, 328, 1, 328, 1, 329, 1, 329, 1, 329, 1, 329, 1, 329, 1, 329, 1, 
	    329, 1, 330, 1, 330, 1, 330, 1, 330, 1, 330, 1, 331, 1, 331, 1, 331, 
	    1, 331, 1, 331, 1, 331, 1, 331, 1, 331, 1, 331, 1, 332, 1, 332, 1, 
	    332, 1, 332, 1, 332, 1, 333, 1, 333, 1, 333, 1, 333, 1, 333, 1, 334, 
	    1, 334, 1, 334, 1, 334, 1, 334, 1, 334, 1, 334, 1, 334, 1, 334, 1, 
	    334, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 
	    1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 335, 1, 336, 1, 
	    336, 1, 336, 1, 337, 1, 337, 1, 337, 1, 337, 1, 338, 1, 338, 1, 338, 
	    1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 338, 1, 339, 1, 339, 1, 
	    339, 1, 339, 1, 339, 1, 339, 1, 339, 1, 340, 1, 340, 1, 340, 1, 340, 
	    1, 340, 1, 340, 1, 340, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 
	    341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 1, 341, 
	    1, 342, 1, 342, 1, 342, 1, 342, 1, 342, 1, 342, 1, 342, 1, 342, 1, 
	    342, 1, 342, 1, 342, 1, 342, 1, 343, 1, 343, 1, 343, 1, 343, 1, 343, 
	    1, 343, 1, 343, 1, 343, 1, 343, 1, 343, 1, 344, 1, 344, 1, 344, 1, 
	    344, 1, 344, 1, 345, 1, 345, 1, 345, 1, 345, 1, 345, 1, 346, 1, 346, 
	    1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 346, 1, 347, 1, 
	    347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 347, 1, 348, 
	    1, 348, 1, 348, 1, 348, 1, 348, 1, 348, 1, 349, 1, 349, 1, 349, 1, 
	    349, 1, 349, 1, 349, 1, 349, 1, 349, 1, 350, 1, 350, 1, 350, 1, 350, 
	    1, 350, 1, 351, 1, 351, 1, 351, 1, 351, 1, 351, 1, 351, 1, 351, 1, 
	    351, 1, 352, 1, 352, 1, 352, 1, 352, 1, 352, 1, 352, 1, 352, 1, 352, 
	    1, 352, 1, 352, 1, 353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 
	    353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 353, 1, 354, 1, 354, 1, 354, 
	    1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 1, 354, 1, 
	    354, 1, 354, 1, 354, 1, 355, 1, 355, 1, 355, 1, 355, 1, 355, 1, 355, 
	    1, 356, 1, 356, 1, 356, 1, 356, 1, 356, 1, 356, 1, 356, 1, 356, 1, 
	    357, 1, 357, 1, 357, 1, 357, 1, 357, 1, 357, 1, 357, 1, 358, 1, 358, 
	    1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 358, 1, 
	    359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 359, 1, 360, 1, 360, 
	    1, 360, 1, 360, 1, 360, 1, 360, 1, 360, 1, 360, 1, 361, 1, 361, 1, 
	    361, 1, 361, 1, 361, 1, 361, 1, 361, 1, 361, 1, 361, 1, 362, 1, 362, 
	    1, 362, 1, 362, 1, 362, 1, 362, 1, 363, 1, 363, 1, 363, 1, 363, 1, 
	    363, 1, 363, 1, 363, 1, 364, 1, 364, 1, 364, 1, 364, 1, 365, 1, 365, 
	    1, 365, 1, 365, 1, 365, 1, 366, 1, 366, 1, 366, 1, 366, 1, 366, 1, 
	    366, 1, 367, 1, 367, 1, 367, 1, 367, 1, 367, 1, 367, 1, 368, 1, 368, 
	    1, 368, 1, 368, 1, 368, 1, 368, 1, 369, 1, 369, 1, 369, 1, 369, 1, 
	    369, 1, 370, 1, 370, 1, 370, 1, 370, 1, 370, 1, 370, 1, 370, 1, 371, 
	    1, 371, 1, 371, 1, 371, 1, 371, 1, 371, 1, 371, 1, 371, 1, 371, 1, 
	    372, 1, 372, 1, 372, 1, 372, 1, 372, 1, 372, 1, 373, 1, 373, 1, 373, 
	    1, 373, 1, 373, 1, 373, 1, 373, 1, 374, 1, 374, 1, 374, 1, 374, 1, 
	    374, 1, 374, 1, 374, 1, 374, 1, 375, 1, 375, 1, 375, 1, 375, 1, 375, 
	    1, 375, 1, 375, 1, 375, 1, 376, 1, 376, 1, 376, 1, 376, 1, 376, 1, 
	    376, 1, 376, 1, 376, 1, 377, 1, 377, 1, 377, 1, 377, 1, 377, 1, 378, 
	    1, 378, 1, 378, 1, 378, 1, 378, 1, 378, 1, 378, 1, 378, 1, 378, 1, 
	    378, 1, 379, 1, 379, 1, 379, 1, 379, 1, 379, 1, 380, 1, 380, 1, 380, 
	    1, 380, 1, 380, 1, 381, 1, 381, 1, 381, 1, 381, 1, 381, 1, 381, 1, 
	    382, 1, 382, 1, 382, 1, 382, 1, 382, 1, 382, 1, 383, 1, 383, 1, 383, 
	    1, 383, 1, 383, 1, 383, 1, 383, 1, 384, 1, 384, 1, 384, 1, 384, 1, 
	    384, 1, 385, 1, 385, 1, 385, 1, 385, 1, 385, 1, 385, 1, 385, 1, 385, 
	    1, 386, 1, 386, 1, 386, 1, 386, 1, 386, 1, 387, 1, 387, 1, 387, 1, 
	    387, 1, 387, 1, 387, 1, 387, 1, 387, 1, 388, 1, 388, 1, 388, 1, 388, 
	    1, 388, 1, 388, 1, 389, 1, 389, 1, 389, 1, 390, 1, 390, 1, 390, 1, 
	    390, 1, 390, 1, 391, 1, 391, 1, 391, 1, 391, 1, 392, 1, 392, 1, 392, 
	    1, 392, 1, 392, 1, 393, 1, 393, 1, 393, 1, 393, 1, 393, 1, 394, 1, 
	    394, 1, 395, 1, 395, 1, 396, 1, 396, 1, 397, 1, 397, 1, 398, 1, 398, 
	    1, 399, 1, 399, 1, 400, 1, 400, 1, 400, 1, 400, 3, 400, 3748, 8, 400, 
	    1, 401, 1, 401, 1, 402, 1, 402, 1, 402, 1, 403, 1, 403, 1, 404, 1, 
	    404, 1, 404, 1, 405, 1, 405, 1, 406, 1, 406, 1, 407, 1, 407, 1, 408, 
	    1, 408, 1, 409, 1, 409, 1, 410, 1, 410, 1, 410, 1, 411, 1, 411, 1, 
	    412, 1, 412, 1, 413, 1, 413, 1, 414, 1, 414, 1, 415, 1, 415, 1, 416, 
	    1, 416, 1, 417, 1, 417, 1, 418, 1, 418, 1, 418, 1, 419, 1, 419, 1, 
	    420, 1, 420, 1, 420, 1, 421, 1, 421, 1, 421, 5, 421, 3798, 8, 421, 
	    10, 421, 12, 421, 3801, 9, 421, 1, 421, 1, 421, 1, 421, 1, 421, 5, 
	    421, 3807, 8, 421, 10, 421, 12, 421, 3810, 9, 421, 1, 421, 3, 421, 
	    3813, 8, 421, 1, 422, 1, 422, 1, 422, 1, 422, 1, 422, 5, 422, 3820, 
	    8, 422, 10, 422, 12, 422, 3823, 9, 422, 1, 422, 1, 422, 1, 422, 1, 
	    422, 1, 422, 1, 422, 1, 422, 1, 422, 5, 422, 3833, 8, 422, 10, 422, 
	    12, 422, 3836, 9, 422, 1, 422, 1, 422, 1, 422, 3, 422, 3841, 8, 422, 
	    1, 423, 1, 423, 1, 423, 1, 423, 1, 423, 1, 423, 5, 423, 3849, 8, 423, 
	    10, 423, 12, 423, 3852, 9, 423, 1, 423, 1, 423, 1, 423, 1, 423, 1, 
	    423, 1, 423, 1, 423, 5, 423, 3861, 8, 423, 10, 423, 12, 423, 3864, 
	    9, 423, 1, 423, 3, 423, 3867, 8, 423, 1, 424, 1, 424, 1, 424, 1, 424, 
	    1, 424, 1, 424, 5, 424, 3875, 8, 424, 10, 424, 12, 424, 3878, 9, 424, 
	    1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 1, 424, 1, 
	    424, 5, 424, 3889, 8, 424, 10, 424, 12, 424, 3892, 9, 424, 1, 424, 
	    1, 424, 1, 424, 3, 424, 3897, 8, 424, 1, 425, 1, 425, 1, 425, 1, 425, 
	    5, 425, 3903, 8, 425, 10, 425, 12, 425, 3906, 9, 425, 1, 425, 1, 425, 
	    1, 425, 1, 425, 1, 425, 5, 425, 3913, 8, 425, 10, 425, 12, 425, 3916, 
	    9, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 5, 425, 3923, 8, 425, 
	    10, 425, 12, 425, 3926, 9, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 
	    425, 1, 425, 1, 425, 5, 425, 3935, 8, 425, 10, 425, 12, 425, 3938, 
	    9, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 
	    425, 1, 425, 5, 425, 3949, 8, 425, 10, 425, 12, 425, 3952, 9, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 5, 
	    425, 3962, 8, 425, 10, 425, 12, 425, 3965, 9, 425, 1, 425, 1, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 5, 425, 3973, 8, 425, 10, 425, 12, 
	    425, 3976, 9, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 
	    1, 425, 1, 425, 5, 425, 3986, 8, 425, 10, 425, 12, 425, 3989, 9, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 
	    425, 1, 425, 5, 425, 4001, 8, 425, 10, 425, 12, 425, 4004, 9, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 5, 
	    425, 4014, 8, 425, 10, 425, 12, 425, 4017, 9, 425, 1, 425, 1, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 5, 425, 4025, 8, 425, 10, 425, 12, 
	    425, 4028, 9, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 
	    1, 425, 1, 425, 5, 425, 4038, 8, 425, 10, 425, 12, 425, 4041, 9, 425, 
	    1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 425, 1, 
	    425, 1, 425, 5, 425, 4053, 8, 425, 10, 425, 12, 425, 4056, 9, 425, 
	    1, 425, 1, 425, 1, 425, 3, 425, 4061, 8, 425, 1, 426, 4, 426, 4064, 
	    8, 426, 11, 426, 12, 426, 4065, 1, 427, 1, 427, 1, 427, 1, 427, 4, 
	    427, 4072, 8, 427, 11, 427, 12, 427, 4073, 1, 428, 4, 428, 4077, 8, 
	    428, 11, 428, 12, 428, 4078, 1, 428, 1, 428, 5, 428, 4083, 8, 428, 
	    10, 428, 12, 428, 4086, 9, 428, 1, 428, 1, 428, 4, 428, 4090, 8, 428, 
	    11, 428, 12, 428, 4091, 3, 428, 4094, 8, 428, 1, 429, 4, 429, 4097, 
	    8, 429, 11, 429, 12, 429, 4098, 1, 429, 1, 429, 5, 429, 4103, 8, 429, 
	    10, 429, 12, 429, 4106, 9, 429, 3, 429, 4108, 8, 429, 1, 429, 1, 429, 
	    1, 429, 1, 429, 4, 429, 4114, 8, 429, 11, 429, 12, 429, 4115, 1, 429, 
	    1, 429, 3, 429, 4120, 8, 429, 1, 430, 1, 430, 3, 430, 4124, 8, 430, 
	    1, 430, 1, 430, 1, 430, 5, 430, 4129, 8, 430, 10, 430, 12, 430, 4132, 
	    9, 430, 1, 431, 1, 431, 1, 431, 1, 431, 5, 431, 4138, 8, 431, 10, 431, 
	    12, 431, 4141, 9, 431, 1, 431, 1, 431, 1, 432, 1, 432, 1, 432, 1, 432, 
	    1, 432, 1, 432, 1, 432, 1, 432, 1, 432, 1, 432, 1, 432, 3, 432, 4156, 
	    8, 432, 1, 433, 1, 433, 1, 433, 1, 434, 1, 434, 3, 434, 4163, 8, 434, 
	    1, 434, 4, 434, 4166, 8, 434, 11, 434, 12, 434, 4167, 1, 435, 1, 435, 
	    1, 436, 1, 436, 1, 437, 1, 437, 1, 437, 1, 437, 5, 437, 4178, 8, 437, 
	    10, 437, 12, 437, 4181, 9, 437, 1, 437, 3, 437, 4184, 8, 437, 1, 437, 
	    3, 437, 4187, 8, 437, 1, 437, 1, 437, 1, 438, 1, 438, 5, 438, 4193, 
	    8, 438, 10, 438, 12, 438, 4196, 9, 438, 1, 438, 3, 438, 4199, 8, 438, 
	    1, 438, 3, 438, 4202, 8, 438, 1, 438, 1, 438, 1, 439, 1, 439, 1, 439, 
	    1, 439, 1, 439, 5, 439, 4211, 8, 439, 10, 439, 12, 439, 4214, 9, 439, 
	    1, 439, 1, 439, 1, 439, 1, 439, 1, 439, 1, 440, 4, 440, 4222, 8, 440, 
	    11, 440, 12, 440, 4223, 1, 440, 1, 440, 1, 441, 1, 441, 1, 441, 1, 
	    441, 1, 442, 1, 442, 1, 442, 3, 442, 4235, 8, 442, 1, 443, 1, 443, 
	    11, 3821, 3834, 3876, 3890, 3936, 3950, 3987, 4002, 4039, 4054, 4212, 
	    0, 444, 1, 1, 3, 2, 5, 3, 7, 4, 9, 5, 11, 6, 13, 7, 15, 8, 17, 9, 19, 
	    10, 21, 11, 23, 12, 25, 13, 27, 14, 29, 15, 31, 16, 33, 17, 35, 18, 
	    37, 19, 39, 20, 41, 21, 43, 22, 45, 23, 47, 24, 49, 25, 51, 26, 53, 
	    27, 55, 28, 57, 29, 59, 30, 61, 31, 63, 32, 65, 33, 67, 34, 69, 35, 
	    71, 36, 73, 37, 75, 38, 77, 39, 79, 40, 81, 41, 83, 42, 85, 43, 87, 
	    44, 89, 45, 91, 46, 93, 47, 95, 48, 97, 49, 99, 50, 101, 51, 103, 52, 
	    105, 53, 107, 54, 109, 55, 111, 56, 113, 57, 115, 58, 117, 59, 119, 
	    60, 121, 61, 123, 62, 125, 63, 127, 64, 129, 65, 131, 66, 133, 67, 
	    135, 68, 137, 69, 139, 70, 141, 71, 143, 72, 145, 73, 147, 74, 149, 
	    75, 151, 76, 153, 77, 155, 78, 157, 79, 159, 80, 161, 81, 163, 82, 
	    165, 83, 167, 84, 169, 85, 171, 86, 173, 87, 175, 88, 177, 89, 179, 
	    90, 181, 91, 183, 92, 185, 93, 187, 94, 189, 95, 191, 96, 193, 97, 
	    195, 98, 197, 99, 199, 100, 201, 101, 203, 102, 205, 103, 207, 104, 
	    209, 105, 211, 106, 213, 107, 215, 108, 217, 109, 219, 110, 221, 111, 
	    223, 112, 225, 113, 227, 114, 229, 115, 231, 116, 233, 117, 235, 118, 
	    237, 119, 239, 120, 241, 121, 243, 122, 245, 123, 247, 124, 249, 125, 
	    251, 126, 253, 127, 255, 128, 257, 129, 259, 130, 261, 131, 263, 132, 
	    265, 133, 267, 134, 269, 135, 271, 136, 273, 137, 275, 138, 277, 139, 
	    279, 140, 281, 141, 283, 142, 285, 143, 287, 144, 289, 145, 291, 146, 
	    293, 147, 295, 148, 297, 149, 299, 150, 301, 151, 303, 152, 305, 153, 
	    307, 154, 309, 155, 311, 156, 313, 157, 315, 158, 317, 159, 319, 160, 
	    321, 161, 323, 162, 325, 163, 327, 164, 329, 165, 331, 166, 333, 167, 
	    335, 168, 337, 169, 339, 170, 341, 171, 343, 172, 345, 173, 347, 174, 
	    349, 175, 351, 176, 353, 177, 355, 178, 357, 179, 359, 180, 361, 181, 
	    363, 182, 365, 183, 367, 184, 369, 185, 371, 186, 373, 187, 375, 188, 
	    377, 189, 379, 190, 381, 191, 383, 192, 385, 193, 387, 194, 389, 195, 
	    391, 196, 393, 197, 395, 198, 397, 199, 399, 200, 401, 201, 403, 202, 
	    405, 203, 407, 204, 409, 205, 411, 206, 413, 207, 415, 208, 417, 209, 
	    419, 210, 421, 211, 423, 212, 425, 213, 427, 214, 429, 215, 431, 216, 
	    433, 217, 435, 218, 437, 219, 439, 220, 441, 221, 443, 222, 445, 223, 
	    447, 224, 449, 225, 451, 226, 453, 227, 455, 228, 457, 229, 459, 230, 
	    461, 231, 463, 232, 465, 233, 467, 234, 469, 235, 471, 236, 473, 237, 
	    475, 238, 477, 239, 479, 240, 481, 241, 483, 242, 485, 243, 487, 244, 
	    489, 245, 491, 246, 493, 247, 495, 248, 497, 249, 499, 250, 501, 251, 
	    503, 252, 505, 253, 507, 254, 509, 255, 511, 256, 513, 257, 515, 258, 
	    517, 259, 519, 260, 521, 261, 523, 262, 525, 263, 527, 264, 529, 265, 
	    531, 266, 533, 267, 535, 268, 537, 269, 539, 270, 541, 271, 543, 272, 
	    545, 273, 547, 274, 549, 275, 551, 276, 553, 277, 555, 278, 557, 279, 
	    559, 280, 561, 281, 563, 282, 565, 283, 567, 284, 569, 285, 571, 286, 
	    573, 287, 575, 288, 577, 289, 579, 290, 581, 291, 583, 292, 585, 293, 
	    587, 294, 589, 295, 591, 296, 593, 297, 595, 298, 597, 299, 599, 300, 
	    601, 301, 603, 302, 605, 303, 607, 304, 609, 305, 611, 306, 613, 307, 
	    615, 308, 617, 309, 619, 310, 621, 311, 623, 312, 625, 313, 627, 314, 
	    629, 315, 631, 316, 633, 317, 635, 318, 637, 319, 639, 320, 641, 321, 
	    643, 322, 645, 323, 647, 324, 649, 325, 651, 326, 653, 327, 655, 328, 
	    657, 329, 659, 330, 661, 331, 663, 332, 665, 333, 667, 334, 669, 335, 
	    671, 336, 673, 337, 675, 338, 677, 339, 679, 340, 681, 341, 683, 342, 
	    685, 343, 687, 344, 689, 345, 691, 346, 693, 347, 695, 348, 697, 349, 
	    699, 350, 701, 351, 703, 352, 705, 353, 707, 354, 709, 355, 711, 356, 
	    713, 357, 715, 358, 717, 359, 719, 360, 721, 361, 723, 362, 725, 363, 
	    727, 364, 729, 365, 731, 366, 733, 367, 735, 368, 737, 369, 739, 370, 
	    741, 371, 743, 372, 745, 373, 747, 374, 749, 375, 751, 376, 753, 377, 
	    755, 378, 757, 379, 759, 380, 761, 381, 763, 382, 765, 383, 767, 384, 
	    769, 385, 771, 386, 773, 387, 775, 388, 777, 389, 779, 390, 781, 391, 
	    783, 392, 785, 393, 787, 394, 789, 395, 791, 396, 793, 397, 795, 398, 
	    797, 399, 799, 400, 801, 401, 803, 402, 805, 403, 807, 404, 809, 405, 
	    811, 406, 813, 407, 815, 408, 817, 409, 819, 410, 821, 411, 823, 412, 
	    825, 413, 827, 414, 829, 415, 831, 416, 833, 417, 835, 418, 837, 419, 
	    839, 420, 841, 421, 843, 422, 845, 423, 847, 424, 849, 425, 851, 426, 
	    853, 427, 855, 428, 857, 429, 859, 430, 861, 431, 863, 432, 865, 433, 
	    867, 434, 869, 0, 871, 0, 873, 0, 875, 435, 877, 436, 879, 437, 881, 
	    438, 883, 439, 885, 440, 887, 441, 1, 0, 14, 4, 0, 10, 10, 13, 13, 
	    34, 34, 92, 92, 4, 0, 10, 10, 13, 13, 39, 39, 92, 92, 1, 0, 39, 39, 
	    1, 0, 34, 34, 3, 0, 48, 57, 65, 70, 97, 102, 3, 0, 65, 90, 92, 92, 
	    95, 122, 3, 0, 10, 10, 92, 92, 96, 96, 2, 0, 43, 43, 45, 45, 1, 0, 
	    48, 57, 1, 0, 65, 90, 2, 0, 10, 10, 13, 13, 3, 0, 9, 10, 13, 13, 32, 
	    32, 2, 0, 160, 160, 8239, 8239, 2, 0, 34, 34, 39, 39, 4306, 0, 1, 1, 
	    0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 7, 1, 0, 0, 0, 0, 9, 
	    1, 0, 0, 0, 0, 11, 1, 0, 0, 0, 0, 13, 1, 0, 0, 0, 0, 15, 1, 0, 0, 0, 
	    0, 17, 1, 0, 0, 0, 0, 19, 1, 0, 0, 0, 0, 21, 1, 0, 0, 0, 0, 23, 1, 
	    0, 0, 0, 0, 25, 1, 0, 0, 0, 0, 27, 1, 0, 0, 0, 0, 29, 1, 0, 0, 0, 0, 
	    31, 1, 0, 0, 0, 0, 33, 1, 0, 0, 0, 0, 35, 1, 0, 0, 0, 0, 37, 1, 0, 
	    0, 0, 0, 39, 1, 0, 0, 0, 0, 41, 1, 0, 0, 0, 0, 43, 1, 0, 0, 0, 0, 45, 
	    1, 0, 0, 0, 0, 47, 1, 0, 0, 0, 0, 49, 1, 0, 0, 0, 0, 51, 1, 0, 0, 0, 
	    0, 53, 1, 0, 0, 0, 0, 55, 1, 0, 0, 0, 0, 57, 1, 0, 0, 0, 0, 59, 1, 
	    0, 0, 0, 0, 61, 1, 0, 0, 0, 0, 63, 1, 0, 0, 0, 0, 65, 1, 0, 0, 0, 0, 
	    67, 1, 0, 0, 0, 0, 69, 1, 0, 0, 0, 0, 71, 1, 0, 0, 0, 0, 73, 1, 0, 
	    0, 0, 0, 75, 1, 0, 0, 0, 0, 77, 1, 0, 0, 0, 0, 79, 1, 0, 0, 0, 0, 81, 
	    1, 0, 0, 0, 0, 83, 1, 0, 0, 0, 0, 85, 1, 0, 0, 0, 0, 87, 1, 0, 0, 0, 
	    0, 89, 1, 0, 0, 0, 0, 91, 1, 0, 0, 0, 0, 93, 1, 0, 0, 0, 0, 95, 1, 
	    0, 0, 0, 0, 97, 1, 0, 0, 0, 0, 99, 1, 0, 0, 0, 0, 101, 1, 0, 0, 0, 
	    0, 103, 1, 0, 0, 0, 0, 105, 1, 0, 0, 0, 0, 107, 1, 0, 0, 0, 0, 109, 
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
	    0, 0, 859, 1, 0, 0, 0, 0, 861, 1, 0, 0, 0, 0, 863, 1, 0, 0, 0, 0, 865, 
	    1, 0, 0, 0, 0, 867, 1, 0, 0, 0, 0, 875, 1, 0, 0, 0, 0, 877, 1, 0, 0, 
	    0, 0, 879, 1, 0, 0, 0, 0, 881, 1, 0, 0, 0, 0, 883, 1, 0, 0, 0, 0, 885, 
	    1, 0, 0, 0, 0, 887, 1, 0, 0, 0, 1, 889, 1, 0, 0, 0, 3, 891, 1, 0, 0, 
	    0, 5, 894, 1, 0, 0, 0, 7, 897, 1, 0, 0, 0, 9, 900, 1, 0, 0, 0, 11, 
	    903, 1, 0, 0, 0, 13, 905, 1, 0, 0, 0, 15, 907, 1, 0, 0, 0, 17, 913, 
	    1, 0, 0, 0, 19, 920, 1, 0, 0, 0, 21, 924, 1, 0, 0, 0, 23, 930, 1, 0, 
	    0, 0, 25, 936, 1, 0, 0, 0, 27, 940, 1, 0, 0, 0, 29, 946, 1, 0, 0, 0, 
	    31, 954, 1, 0, 0, 0, 33, 958, 1, 0, 0, 0, 35, 963, 1, 0, 0, 0, 37, 
	    967, 1, 0, 0, 0, 39, 973, 1, 0, 0, 0, 41, 976, 1, 0, 0, 0, 43, 980, 
	    1, 0, 0, 0, 45, 983, 1, 0, 0, 0, 47, 990, 1, 0, 0, 0, 49, 1004, 1, 
	    0, 0, 0, 51, 1009, 1, 0, 0, 0, 53, 1016, 1, 0, 0, 0, 55, 1022, 1, 0, 
	    0, 0, 57, 1032, 1, 0, 0, 0, 59, 1040, 1, 0, 0, 0, 61, 1045, 1, 0, 0, 
	    0, 63, 1051, 1, 0, 0, 0, 65, 1054, 1, 0, 0, 0, 67, 1060, 1, 0, 0, 0, 
	    69, 1065, 1, 0, 0, 0, 71, 1072, 1, 0, 0, 0, 73, 1080, 1, 0, 0, 0, 75, 
	    1085, 1, 0, 0, 0, 77, 1100, 1, 0, 0, 0, 79, 1117, 1, 0, 0, 0, 81, 1122, 
	    1, 0, 0, 0, 83, 1131, 1, 0, 0, 0, 85, 1141, 1, 0, 0, 0, 87, 1147, 1, 
	    0, 0, 0, 89, 1153, 1, 0, 0, 0, 91, 1161, 1, 0, 0, 0, 93, 1170, 1, 0, 
	    0, 0, 95, 1178, 1, 0, 0, 0, 97, 1185, 1, 0, 0, 0, 99, 1193, 1, 0, 0, 
	    0, 101, 1195, 1, 0, 0, 0, 103, 1203, 1, 0, 0, 0, 105, 1210, 1, 0, 0, 
	    0, 107, 1220, 1, 0, 0, 0, 109, 1229, 1, 0, 0, 0, 111, 1241, 1, 0, 0, 
	    0, 113, 1253, 1, 0, 0, 0, 115, 1261, 1, 0, 0, 0, 117, 1272, 1, 0, 0, 
	    0, 119, 1283, 1, 0, 0, 0, 121, 1292, 1, 0, 0, 0, 123, 1304, 1, 0, 0, 
	    0, 125, 1309, 1, 0, 0, 0, 127, 1315, 1, 0, 0, 0, 129, 1322, 1, 0, 0, 
	    0, 131, 1328, 1, 0, 0, 0, 133, 1333, 1, 0, 0, 0, 135, 1341, 1, 0, 0, 
	    0, 137, 1354, 1, 0, 0, 0, 139, 1369, 1, 0, 0, 0, 141, 1374, 1, 0, 0, 
	    0, 143, 1383, 1, 0, 0, 0, 145, 1393, 1, 0, 0, 0, 147, 1398, 1, 0, 0, 
	    0, 149, 1407, 1, 0, 0, 0, 151, 1411, 1, 0, 0, 0, 153, 1421, 1, 0, 0, 
	    0, 155, 1431, 1, 0, 0, 0, 157, 1445, 1, 0, 0, 0, 159, 1455, 1, 0, 0, 
	    0, 161, 1466, 1, 0, 0, 0, 163, 1474, 1, 0, 0, 0, 165, 1482, 1, 0, 0, 
	    0, 167, 1491, 1, 0, 0, 0, 169, 1498, 1, 0, 0, 0, 171, 1506, 1, 0, 0, 
	    0, 173, 1513, 1, 0, 0, 0, 175, 1523, 1, 0, 0, 0, 177, 1533, 1, 0, 0, 
	    0, 179, 1538, 1, 0, 0, 0, 181, 1543, 1, 0, 0, 0, 183, 1552, 1, 0, 0, 
	    0, 185, 1563, 1, 0, 0, 0, 187, 1577, 1, 0, 0, 0, 189, 1586, 1, 0, 0, 
	    0, 191, 1594, 1, 0, 0, 0, 193, 1606, 1, 0, 0, 0, 195, 1616, 1, 0, 0, 
	    0, 197, 1623, 1, 0, 0, 0, 199, 1626, 1, 0, 0, 0, 201, 1633, 1, 0, 0, 
	    0, 203, 1638, 1, 0, 0, 0, 205, 1643, 1, 0, 0, 0, 207, 1650, 1, 0, 0, 
	    0, 209, 1656, 1, 0, 0, 0, 211, 1663, 1, 0, 0, 0, 213, 1672, 1, 0, 0, 
	    0, 215, 1676, 1, 0, 0, 0, 217, 1682, 1, 0, 0, 0, 219, 1689, 1, 0, 0, 
	    0, 221, 1694, 1, 0, 0, 0, 223, 1701, 1, 0, 0, 0, 225, 1711, 1, 0, 0, 
	    0, 227, 1719, 1, 0, 0, 0, 229, 1729, 1, 0, 0, 0, 231, 1737, 1, 0, 0, 
	    0, 233, 1744, 1, 0, 0, 0, 235, 1752, 1, 0, 0, 0, 237, 1761, 1, 0, 0, 
	    0, 239, 1769, 1, 0, 0, 0, 241, 1775, 1, 0, 0, 0, 243, 1781, 1, 0, 0, 
	    0, 245, 1788, 1, 0, 0, 0, 247, 1795, 1, 0, 0, 0, 249, 1801, 1, 0, 0, 
	    0, 251, 1807, 1, 0, 0, 0, 253, 1817, 1, 0, 0, 0, 255, 1821, 1, 0, 0, 
	    0, 257, 1828, 1, 0, 0, 0, 259, 1835, 1, 0, 0, 0, 261, 1840, 1, 0, 0, 
	    0, 263, 1845, 1, 0, 0, 0, 265, 1854, 1, 0, 0, 0, 267, 1864, 1, 0, 0, 
	    0, 269, 1874, 1, 0, 0, 0, 271, 1880, 1, 0, 0, 0, 273, 1886, 1, 0, 0, 
	    0, 275, 1894, 1, 0, 0, 0, 277, 1901, 1, 0, 0, 0, 279, 1910, 1, 0, 0, 
	    0, 281, 1916, 1, 0, 0, 0, 283, 1925, 1, 0, 0, 0, 285, 1932, 1, 0, 0, 
	    0, 287, 1937, 1, 0, 0, 0, 289, 1944, 1, 0, 0, 0, 291, 1951, 1, 0, 0, 
	    0, 293, 1956, 1, 0, 0, 0, 295, 1965, 1, 0, 0, 0, 297, 1968, 1, 0, 0, 
	    0, 299, 1975, 1, 0, 0, 0, 301, 1985, 1, 0, 0, 0, 303, 1988, 1, 0, 0, 
	    0, 305, 1996, 1, 0, 0, 0, 307, 2006, 1, 0, 0, 0, 309, 2014, 1, 0, 0, 
	    0, 311, 2020, 1, 0, 0, 0, 313, 2026, 1, 0, 0, 0, 315, 2038, 1, 0, 0, 
	    0, 317, 2050, 1, 0, 0, 0, 319, 2057, 1, 0, 0, 0, 321, 2067, 1, 0, 0, 
	    0, 323, 2076, 1, 0, 0, 0, 325, 2081, 1, 0, 0, 0, 327, 2089, 1, 0, 0, 
	    0, 329, 2092, 1, 0, 0, 0, 331, 2095, 1, 0, 0, 0, 333, 2105, 1, 0, 0, 
	    0, 335, 2113, 1, 0, 0, 0, 337, 2121, 1, 0, 0, 0, 339, 2129, 1, 0, 0, 
	    0, 341, 2135, 1, 0, 0, 0, 343, 2140, 1, 0, 0, 0, 345, 2145, 1, 0, 0, 
	    0, 347, 2150, 1, 0, 0, 0, 349, 2154, 1, 0, 0, 0, 351, 2159, 1, 0, 0, 
	    0, 353, 2166, 1, 0, 0, 0, 355, 2175, 1, 0, 0, 0, 357, 2181, 1, 0, 0, 
	    0, 359, 2186, 1, 0, 0, 0, 361, 2194, 1, 0, 0, 0, 363, 2202, 1, 0, 0, 
	    0, 365, 2207, 1, 0, 0, 0, 367, 2213, 1, 0, 0, 0, 369, 2221, 1, 0, 0, 
	    0, 371, 2226, 1, 0, 0, 0, 373, 2232, 1, 0, 0, 0, 375, 2238, 1, 0, 0, 
	    0, 377, 2246, 1, 0, 0, 0, 379, 2252, 1, 0, 0, 0, 381, 2261, 1, 0, 0, 
	    0, 383, 2266, 1, 0, 0, 0, 385, 2274, 1, 0, 0, 0, 387, 2279, 1, 0, 0, 
	    0, 389, 2283, 1, 0, 0, 0, 391, 2291, 1, 0, 0, 0, 393, 2297, 1, 0, 0, 
	    0, 395, 2305, 1, 0, 0, 0, 397, 2313, 1, 0, 0, 0, 399, 2329, 1, 0, 0, 
	    0, 401, 2342, 1, 0, 0, 0, 403, 2346, 1, 0, 0, 0, 405, 2355, 1, 0, 0, 
	    0, 407, 2361, 1, 0, 0, 0, 409, 2369, 1, 0, 0, 0, 411, 2381, 1, 0, 0, 
	    0, 413, 2393, 1, 0, 0, 0, 415, 2397, 1, 0, 0, 0, 417, 2403, 1, 0, 0, 
	    0, 419, 2410, 1, 0, 0, 0, 421, 2416, 1, 0, 0, 0, 423, 2423, 1, 0, 0, 
	    0, 425, 2429, 1, 0, 0, 0, 427, 2434, 1, 0, 0, 0, 429, 2442, 1, 0, 0, 
	    0, 431, 2447, 1, 0, 0, 0, 433, 2451, 1, 0, 0, 0, 435, 2455, 1, 0, 0, 
	    0, 437, 2460, 1, 0, 0, 0, 439, 2465, 1, 0, 0, 0, 441, 2468, 1, 0, 0, 
	    0, 443, 2473, 1, 0, 0, 0, 445, 2483, 1, 0, 0, 0, 447, 2487, 1, 0, 0, 
	    0, 449, 2492, 1, 0, 0, 0, 451, 2498, 1, 0, 0, 0, 453, 2505, 1, 0, 0, 
	    0, 455, 2508, 1, 0, 0, 0, 457, 2515, 1, 0, 0, 0, 459, 2520, 1, 0, 0, 
	    0, 461, 2523, 1, 0, 0, 0, 463, 2527, 1, 0, 0, 0, 465, 2532, 1, 0, 0, 
	    0, 467, 2539, 1, 0, 0, 0, 469, 2547, 1, 0, 0, 0, 471, 2550, 1, 0, 0, 
	    0, 473, 2556, 1, 0, 0, 0, 475, 2562, 1, 0, 0, 0, 477, 2569, 1, 0, 0, 
	    0, 479, 2582, 1, 0, 0, 0, 481, 2587, 1, 0, 0, 0, 483, 2596, 1, 0, 0, 
	    0, 485, 2606, 1, 0, 0, 0, 487, 2618, 1, 0, 0, 0, 489, 2629, 1, 0, 0, 
	    0, 491, 2637, 1, 0, 0, 0, 493, 2642, 1, 0, 0, 0, 495, 2647, 1, 0, 0, 
	    0, 497, 2655, 1, 0, 0, 0, 499, 2659, 1, 0, 0, 0, 501, 2667, 1, 0, 0, 
	    0, 503, 2674, 1, 0, 0, 0, 505, 2682, 1, 0, 0, 0, 507, 2688, 1, 0, 0, 
	    0, 509, 2697, 1, 0, 0, 0, 511, 2707, 1, 0, 0, 0, 513, 2717, 1, 0, 0, 
	    0, 515, 2725, 1, 0, 0, 0, 517, 2731, 1, 0, 0, 0, 519, 2741, 1, 0, 0, 
	    0, 521, 2752, 1, 0, 0, 0, 523, 2763, 1, 0, 0, 0, 525, 2769, 1, 0, 0, 
	    0, 527, 2777, 1, 0, 0, 0, 529, 2785, 1, 0, 0, 0, 531, 2792, 1, 0, 0, 
	    0, 533, 2798, 1, 0, 0, 0, 535, 2804, 1, 0, 0, 0, 537, 2809, 1, 0, 0, 
	    0, 539, 2819, 1, 0, 0, 0, 541, 2827, 1, 0, 0, 0, 543, 2834, 1, 0, 0, 
	    0, 545, 2845, 1, 0, 0, 0, 547, 2853, 1, 0, 0, 0, 549, 2859, 1, 0, 0, 
	    0, 551, 2867, 1, 0, 0, 0, 553, 2876, 1, 0, 0, 0, 555, 2883, 1, 0, 0, 
	    0, 557, 2893, 1, 0, 0, 0, 559, 2900, 1, 0, 0, 0, 561, 2907, 1, 0, 0, 
	    0, 563, 2915, 1, 0, 0, 0, 565, 2922, 1, 0, 0, 0, 567, 2928, 1, 0, 0, 
	    0, 569, 2932, 1, 0, 0, 0, 571, 2937, 1, 0, 0, 0, 573, 2943, 1, 0, 0, 
	    0, 575, 2952, 1, 0, 0, 0, 577, 2959, 1, 0, 0, 0, 579, 2963, 1, 0, 0, 
	    0, 581, 2968, 1, 0, 0, 0, 583, 2976, 1, 0, 0, 0, 585, 2981, 1, 0, 0, 
	    0, 587, 2991, 1, 0, 0, 0, 589, 3000, 1, 0, 0, 0, 591, 3007, 1, 0, 0, 
	    0, 593, 3014, 1, 0, 0, 0, 595, 3021, 1, 0, 0, 0, 597, 3029, 1, 0, 0, 
	    0, 599, 3038, 1, 0, 0, 0, 601, 3043, 1, 0, 0, 0, 603, 3050, 1, 0, 0, 
	    0, 605, 3055, 1, 0, 0, 0, 607, 3061, 1, 0, 0, 0, 609, 3077, 1, 0, 0, 
	    0, 611, 3090, 1, 0, 0, 0, 613, 3098, 1, 0, 0, 0, 615, 3102, 1, 0, 0, 
	    0, 617, 3107, 1, 0, 0, 0, 619, 3112, 1, 0, 0, 0, 621, 3120, 1, 0, 0, 
	    0, 623, 3129, 1, 0, 0, 0, 625, 3134, 1, 0, 0, 0, 627, 3142, 1, 0, 0, 
	    0, 629, 3148, 1, 0, 0, 0, 631, 3154, 1, 0, 0, 0, 633, 3161, 1, 0, 0, 
	    0, 635, 3168, 1, 0, 0, 0, 637, 3175, 1, 0, 0, 0, 639, 3185, 1, 0, 0, 
	    0, 641, 3192, 1, 0, 0, 0, 643, 3199, 1, 0, 0, 0, 645, 3211, 1, 0, 0, 
	    0, 647, 3217, 1, 0, 0, 0, 649, 3224, 1, 0, 0, 0, 651, 3236, 1, 0, 0, 
	    0, 653, 3241, 1, 0, 0, 0, 655, 3251, 1, 0, 0, 0, 657, 3262, 1, 0, 0, 
	    0, 659, 3267, 1, 0, 0, 0, 661, 3274, 1, 0, 0, 0, 663, 3279, 1, 0, 0, 
	    0, 665, 3288, 1, 0, 0, 0, 667, 3293, 1, 0, 0, 0, 669, 3298, 1, 0, 0, 
	    0, 671, 3308, 1, 0, 0, 0, 673, 3323, 1, 0, 0, 0, 675, 3326, 1, 0, 0, 
	    0, 677, 3330, 1, 0, 0, 0, 679, 3339, 1, 0, 0, 0, 681, 3346, 1, 0, 0, 
	    0, 683, 3353, 1, 0, 0, 0, 685, 3367, 1, 0, 0, 0, 687, 3379, 1, 0, 0, 
	    0, 689, 3389, 1, 0, 0, 0, 691, 3394, 1, 0, 0, 0, 693, 3399, 1, 0, 0, 
	    0, 695, 3408, 1, 0, 0, 0, 697, 3417, 1, 0, 0, 0, 699, 3423, 1, 0, 0, 
	    0, 701, 3431, 1, 0, 0, 0, 703, 3436, 1, 0, 0, 0, 705, 3444, 1, 0, 0, 
	    0, 707, 3454, 1, 0, 0, 0, 709, 3466, 1, 0, 0, 0, 711, 3480, 1, 0, 0, 
	    0, 713, 3486, 1, 0, 0, 0, 715, 3494, 1, 0, 0, 0, 717, 3501, 1, 0, 0, 
	    0, 719, 3511, 1, 0, 0, 0, 721, 3518, 1, 0, 0, 0, 723, 3526, 1, 0, 0, 
	    0, 725, 3535, 1, 0, 0, 0, 727, 3541, 1, 0, 0, 0, 729, 3548, 1, 0, 0, 
	    0, 731, 3552, 1, 0, 0, 0, 733, 3557, 1, 0, 0, 0, 735, 3563, 1, 0, 0, 
	    0, 737, 3569, 1, 0, 0, 0, 739, 3575, 1, 0, 0, 0, 741, 3580, 1, 0, 0, 
	    0, 743, 3587, 1, 0, 0, 0, 745, 3596, 1, 0, 0, 0, 747, 3602, 1, 0, 0, 
	    0, 749, 3609, 1, 0, 0, 0, 751, 3617, 1, 0, 0, 0, 753, 3625, 1, 0, 0, 
	    0, 755, 3633, 1, 0, 0, 0, 757, 3638, 1, 0, 0, 0, 759, 3648, 1, 0, 0, 
	    0, 761, 3653, 1, 0, 0, 0, 763, 3658, 1, 0, 0, 0, 765, 3664, 1, 0, 0, 
	    0, 767, 3670, 1, 0, 0, 0, 769, 3677, 1, 0, 0, 0, 771, 3682, 1, 0, 0, 
	    0, 773, 3690, 1, 0, 0, 0, 775, 3695, 1, 0, 0, 0, 777, 3703, 1, 0, 0, 
	    0, 779, 3709, 1, 0, 0, 0, 781, 3712, 1, 0, 0, 0, 783, 3717, 1, 0, 0, 
	    0, 785, 3721, 1, 0, 0, 0, 787, 3726, 1, 0, 0, 0, 789, 3731, 1, 0, 0, 
	    0, 791, 3733, 1, 0, 0, 0, 793, 3735, 1, 0, 0, 0, 795, 3737, 1, 0, 0, 
	    0, 797, 3739, 1, 0, 0, 0, 799, 3741, 1, 0, 0, 0, 801, 3747, 1, 0, 0, 
	    0, 803, 3749, 1, 0, 0, 0, 805, 3751, 1, 0, 0, 0, 807, 3754, 1, 0, 0, 
	    0, 809, 3756, 1, 0, 0, 0, 811, 3759, 1, 0, 0, 0, 813, 3761, 1, 0, 0, 
	    0, 815, 3763, 1, 0, 0, 0, 817, 3765, 1, 0, 0, 0, 819, 3767, 1, 0, 0, 
	    0, 821, 3769, 1, 0, 0, 0, 823, 3772, 1, 0, 0, 0, 825, 3774, 1, 0, 0, 
	    0, 827, 3776, 1, 0, 0, 0, 829, 3778, 1, 0, 0, 0, 831, 3780, 1, 0, 0, 
	    0, 833, 3782, 1, 0, 0, 0, 835, 3784, 1, 0, 0, 0, 837, 3786, 1, 0, 0, 
	    0, 839, 3789, 1, 0, 0, 0, 841, 3791, 1, 0, 0, 0, 843, 3812, 1, 0, 0, 
	    0, 845, 3840, 1, 0, 0, 0, 847, 3866, 1, 0, 0, 0, 849, 3896, 1, 0, 0, 
	    0, 851, 4060, 1, 0, 0, 0, 853, 4063, 1, 0, 0, 0, 855, 4067, 1, 0, 0, 
	    0, 857, 4093, 1, 0, 0, 0, 859, 4119, 1, 0, 0, 0, 861, 4123, 1, 0, 0, 
	    0, 863, 4133, 1, 0, 0, 0, 865, 4155, 1, 0, 0, 0, 867, 4157, 1, 0, 0, 
	    0, 869, 4160, 1, 0, 0, 0, 871, 4169, 1, 0, 0, 0, 873, 4171, 1, 0, 0, 
	    0, 875, 4173, 1, 0, 0, 0, 877, 4190, 1, 0, 0, 0, 879, 4205, 1, 0, 0, 
	    0, 881, 4221, 1, 0, 0, 0, 883, 4227, 1, 0, 0, 0, 885, 4234, 1, 0, 0, 
	    0, 887, 4236, 1, 0, 0, 0, 889, 890, 5, 96, 0, 0, 890, 2, 1, 0, 0, 0, 
	    891, 892, 5, 61, 0, 0, 892, 893, 5, 62, 0, 0, 893, 4, 1, 0, 0, 0, 894, 
	    895, 5, 45, 0, 0, 895, 896, 5, 62, 0, 0, 896, 6, 1, 0, 0, 0, 897, 898, 
	    5, 123, 0, 0, 898, 899, 5, 45, 0, 0, 899, 8, 1, 0, 0, 0, 900, 901, 
	    5, 45, 0, 0, 901, 902, 5, 125, 0, 0, 902, 10, 1, 0, 0, 0, 903, 904, 
	    5, 123, 0, 0, 904, 12, 1, 0, 0, 0, 905, 906, 5, 125, 0, 0, 906, 14, 
	    1, 0, 0, 0, 907, 908, 5, 65, 0, 0, 908, 909, 5, 66, 0, 0, 909, 910, 
	    5, 79, 0, 0, 910, 911, 5, 82, 0, 0, 911, 912, 5, 84, 0, 0, 912, 16, 
	    1, 0, 0, 0, 913, 914, 5, 65, 0, 0, 914, 915, 5, 66, 0, 0, 915, 916, 
	    5, 83, 0, 0, 916, 917, 5, 69, 0, 0, 917, 918, 5, 78, 0, 0, 918, 919, 
	    5, 84, 0, 0, 919, 18, 1, 0, 0, 0, 920, 921, 5, 65, 0, 0, 921, 922, 
	    5, 68, 0, 0, 922, 923, 5, 68, 0, 0, 923, 20, 1, 0, 0, 0, 924, 925, 
	    5, 65, 0, 0, 925, 926, 5, 68, 0, 0, 926, 927, 5, 77, 0, 0, 927, 928, 
	    5, 73, 0, 0, 928, 929, 5, 78, 0, 0, 929, 22, 1, 0, 0, 0, 930, 931, 
	    5, 65, 0, 0, 931, 932, 5, 70, 0, 0, 932, 933, 5, 84, 0, 0, 933, 934, 
	    5, 69, 0, 0, 934, 935, 5, 82, 0, 0, 935, 24, 1, 0, 0, 0, 936, 937, 
	    5, 65, 0, 0, 937, 938, 5, 76, 0, 0, 938, 939, 5, 76, 0, 0, 939, 26, 
	    1, 0, 0, 0, 940, 941, 5, 65, 0, 0, 941, 942, 5, 76, 0, 0, 942, 943, 
	    5, 84, 0, 0, 943, 944, 5, 69, 0, 0, 944, 945, 5, 82, 0, 0, 945, 28, 
	    1, 0, 0, 0, 946, 947, 5, 65, 0, 0, 947, 948, 5, 78, 0, 0, 948, 949, 
	    5, 65, 0, 0, 949, 950, 5, 76, 0, 0, 950, 951, 5, 89, 0, 0, 951, 952, 
	    5, 90, 0, 0, 952, 953, 5, 69, 0, 0, 953, 30, 1, 0, 0, 0, 954, 955, 
	    5, 65, 0, 0, 955, 956, 5, 78, 0, 0, 956, 957, 5, 68, 0, 0, 957, 32, 
	    1, 0, 0, 0, 958, 959, 5, 65, 0, 0, 959, 960, 5, 78, 0, 0, 960, 961, 
	    5, 84, 0, 0, 961, 962, 5, 73, 0, 0, 962, 34, 1, 0, 0, 0, 963, 964, 
	    5, 65, 0, 0, 964, 965, 5, 78, 0, 0, 965, 966, 5, 89, 0, 0, 966, 36, 
	    1, 0, 0, 0, 967, 968, 5, 65, 0, 0, 968, 969, 5, 82, 0, 0, 969, 970, 
	    5, 82, 0, 0, 970, 971, 5, 65, 0, 0, 971, 972, 5, 89, 0, 0, 972, 38, 
	    1, 0, 0, 0, 973, 974, 5, 65, 0, 0, 974, 975, 5, 83, 0, 0, 975, 40, 
	    1, 0, 0, 0, 976, 977, 5, 65, 0, 0, 977, 978, 5, 83, 0, 0, 978, 979, 
	    5, 67, 0, 0, 979, 42, 1, 0, 0, 0, 980, 981, 5, 65, 0, 0, 981, 982, 
	    5, 84, 0, 0, 982, 44, 1, 0, 0, 0, 983, 984, 5, 65, 0, 0, 984, 985, 
	    5, 84, 0, 0, 985, 986, 5, 84, 0, 0, 986, 987, 5, 65, 0, 0, 987, 988, 
	    5, 67, 0, 0, 988, 989, 5, 72, 0, 0, 989, 46, 1, 0, 0, 0, 990, 991, 
	    5, 65, 0, 0, 991, 992, 5, 85, 0, 0, 992, 993, 5, 84, 0, 0, 993, 994, 
	    5, 72, 0, 0, 994, 995, 5, 79, 0, 0, 995, 996, 5, 82, 0, 0, 996, 997, 
	    5, 73, 0, 0, 997, 998, 5, 90, 0, 0, 998, 999, 5, 65, 0, 0, 999, 1000, 
	    5, 84, 0, 0, 1000, 1001, 5, 73, 0, 0, 1001, 1002, 5, 79, 0, 0, 1002, 
	    1003, 5, 78, 0, 0, 1003, 48, 1, 0, 0, 0, 1004, 1005, 5, 65, 0, 0, 1005, 
	    1006, 5, 85, 0, 0, 1006, 1007, 5, 84, 0, 0, 1007, 1008, 5, 79, 0, 0, 
	    1008, 50, 1, 0, 0, 0, 1009, 1010, 5, 66, 0, 0, 1010, 1011, 5, 65, 0, 
	    0, 1011, 1012, 5, 67, 0, 0, 1012, 1013, 5, 75, 0, 0, 1013, 1014, 5, 
	    85, 0, 0, 1014, 1015, 5, 80, 0, 0, 1015, 52, 1, 0, 0, 0, 1016, 1017, 
	    5, 66, 0, 0, 1017, 1018, 5, 69, 0, 0, 1018, 1019, 5, 71, 0, 0, 1019, 
	    1020, 5, 73, 0, 0, 1020, 1021, 5, 78, 0, 0, 1021, 54, 1, 0, 0, 0, 1022, 
	    1023, 5, 66, 0, 0, 1023, 1024, 5, 69, 0, 0, 1024, 1025, 5, 82, 0, 0, 
	    1025, 1026, 5, 78, 0, 0, 1026, 1027, 5, 79, 0, 0, 1027, 1028, 5, 85, 
	    0, 0, 1028, 1029, 5, 76, 0, 0, 1029, 1030, 5, 76, 0, 0, 1030, 1031, 
	    5, 73, 0, 0, 1031, 56, 1, 0, 0, 0, 1032, 1033, 5, 66, 0, 0, 1033, 1034, 
	    5, 69, 0, 0, 1034, 1035, 5, 84, 0, 0, 1035, 1036, 5, 87, 0, 0, 1036, 
	    1037, 5, 69, 0, 0, 1037, 1038, 5, 69, 0, 0, 1038, 1039, 5, 78, 0, 0, 
	    1039, 58, 1, 0, 0, 0, 1040, 1041, 5, 66, 0, 0, 1041, 1042, 5, 79, 0, 
	    0, 1042, 1043, 5, 84, 0, 0, 1043, 1044, 5, 72, 0, 0, 1044, 60, 1, 0, 
	    0, 0, 1045, 1046, 5, 66, 0, 0, 1046, 1047, 5, 82, 0, 0, 1047, 1048, 
	    5, 69, 0, 0, 1048, 1049, 5, 65, 0, 0, 1049, 1050, 5, 75, 0, 0, 1050, 
	    62, 1, 0, 0, 0, 1051, 1052, 5, 66, 0, 0, 1052, 1053, 5, 89, 0, 0, 1053, 
	    64, 1, 0, 0, 0, 1054, 1055, 5, 66, 0, 0, 1055, 1056, 5, 90, 0, 0, 1056, 
	    1057, 5, 73, 0, 0, 1057, 1058, 5, 80, 0, 0, 1058, 1059, 5, 50, 0, 0, 
	    1059, 66, 1, 0, 0, 0, 1060, 1061, 5, 67, 0, 0, 1061, 1062, 5, 65, 0, 
	    0, 1062, 1063, 5, 76, 0, 0, 1063, 1064, 5, 76, 0, 0, 1064, 68, 1, 0, 
	    0, 0, 1065, 1066, 5, 67, 0, 0, 1066, 1067, 5, 65, 0, 0, 1067, 1068, 
	    5, 78, 0, 0, 1068, 1069, 5, 67, 0, 0, 1069, 1070, 5, 69, 0, 0, 1070, 
	    1071, 5, 76, 0, 0, 1071, 70, 1, 0, 0, 0, 1072, 1073, 5, 67, 0, 0, 1073, 
	    1074, 5, 65, 0, 0, 1074, 1075, 5, 83, 0, 0, 1075, 1076, 5, 67, 0, 0, 
	    1076, 1077, 5, 65, 0, 0, 1077, 1078, 5, 68, 0, 0, 1078, 1079, 5, 69, 
	    0, 0, 1079, 72, 1, 0, 0, 0, 1080, 1081, 5, 67, 0, 0, 1081, 1082, 5, 
	    65, 0, 0, 1082, 1083, 5, 83, 0, 0, 1083, 1084, 5, 69, 0, 0, 1084, 74, 
	    1, 0, 0, 0, 1085, 1086, 5, 67, 0, 0, 1086, 1087, 5, 65, 0, 0, 1087, 
	    1088, 5, 83, 0, 0, 1088, 1089, 5, 69, 0, 0, 1089, 1090, 5, 95, 0, 0, 
	    1090, 1091, 5, 83, 0, 0, 1091, 1092, 5, 69, 0, 0, 1092, 1093, 5, 78, 
	    0, 0, 1093, 1094, 5, 83, 0, 0, 1094, 1095, 5, 73, 0, 0, 1095, 1096, 
	    5, 84, 0, 0, 1096, 1097, 5, 73, 0, 0, 1097, 1098, 5, 86, 0, 0, 1098, 
	    1099, 5, 69, 0, 0, 1099, 76, 1, 0, 0, 0, 1100, 1101, 5, 67, 0, 0, 1101, 
	    1102, 5, 65, 0, 0, 1102, 1103, 5, 83, 0, 0, 1103, 1104, 5, 69, 0, 0, 
	    1104, 1105, 5, 95, 0, 0, 1105, 1106, 5, 73, 0, 0, 1106, 1107, 5, 78, 
	    0, 0, 1107, 1108, 5, 83, 0, 0, 1108, 1109, 5, 69, 0, 0, 1109, 1110, 
	    5, 78, 0, 0, 1110, 1111, 5, 83, 0, 0, 1111, 1112, 5, 73, 0, 0, 1112, 
	    1113, 5, 84, 0, 0, 1113, 1114, 5, 73, 0, 0, 1114, 1115, 5, 86, 0, 0, 
	    1115, 1116, 5, 69, 0, 0, 1116, 78, 1, 0, 0, 0, 1117, 1118, 5, 67, 0, 
	    0, 1118, 1119, 5, 65, 0, 0, 1119, 1120, 5, 83, 0, 0, 1120, 1121, 5, 
	    84, 0, 0, 1121, 80, 1, 0, 0, 0, 1122, 1123, 5, 67, 0, 0, 1123, 1124, 
	    5, 65, 0, 0, 1124, 1125, 5, 84, 0, 0, 1125, 1126, 5, 65, 0, 0, 1126, 
	    1127, 5, 76, 0, 0, 1127, 1128, 5, 79, 0, 0, 1128, 1129, 5, 71, 0, 0, 
	    1129, 1130, 5, 83, 0, 0, 1130, 82, 1, 0, 0, 0, 1131, 1132, 5, 67, 0, 
	    0, 1132, 1133, 5, 72, 0, 0, 1133, 1134, 5, 65, 0, 0, 1134, 1135, 5, 
	    82, 0, 0, 1135, 1136, 5, 65, 0, 0, 1136, 1137, 5, 67, 0, 0, 1137, 1138, 
	    5, 84, 0, 0, 1138, 1139, 5, 69, 0, 0, 1139, 1140, 5, 82, 0, 0, 1140, 
	    84, 1, 0, 0, 0, 1141, 1142, 5, 67, 0, 0, 1142, 1143, 5, 76, 0, 0, 1143, 
	    1144, 5, 79, 0, 0, 1144, 1145, 5, 78, 0, 0, 1145, 1146, 5, 69, 0, 0, 
	    1146, 86, 1, 0, 0, 0, 1147, 1148, 5, 67, 0, 0, 1148, 1149, 5, 76, 0, 
	    0, 1149, 1150, 5, 79, 0, 0, 1150, 1151, 5, 83, 0, 0, 1151, 1152, 5, 
	    69, 0, 0, 1152, 88, 1, 0, 0, 0, 1153, 1154, 5, 67, 0, 0, 1154, 1155, 
	    5, 76, 0, 0, 1155, 1156, 5, 85, 0, 0, 1156, 1157, 5, 83, 0, 0, 1157, 
	    1158, 5, 84, 0, 0, 1158, 1159, 5, 69, 0, 0, 1159, 1160, 5, 82, 0, 0, 
	    1160, 90, 1, 0, 0, 0, 1161, 1162, 5, 67, 0, 0, 1162, 1163, 5, 79, 0, 
	    0, 1163, 1164, 5, 65, 0, 0, 1164, 1165, 5, 76, 0, 0, 1165, 1166, 5, 
	    69, 0, 0, 1166, 1167, 5, 83, 0, 0, 1167, 1168, 5, 67, 0, 0, 1168, 1169, 
	    5, 69, 0, 0, 1169, 92, 1, 0, 0, 0, 1170, 1171, 5, 67, 0, 0, 1171, 1172, 
	    5, 79, 0, 0, 1172, 1173, 5, 76, 0, 0, 1173, 1174, 5, 76, 0, 0, 1174, 
	    1175, 5, 65, 0, 0, 1175, 1176, 5, 84, 0, 0, 1176, 1177, 5, 69, 0, 0, 
	    1177, 94, 1, 0, 0, 0, 1178, 1179, 5, 67, 0, 0, 1179, 1180, 5, 79, 0, 
	    0, 1180, 1181, 5, 76, 0, 0, 1181, 1182, 5, 85, 0, 0, 1182, 1183, 5, 
	    77, 0, 0, 1183, 1184, 5, 78, 0, 0, 1184, 96, 1, 0, 0, 0, 1185, 1186, 
	    5, 67, 0, 0, 1186, 1187, 5, 79, 0, 0, 1187, 1188, 5, 76, 0, 0, 1188, 
	    1189, 5, 85, 0, 0, 1189, 1190, 5, 77, 0, 0, 1190, 1191, 5, 78, 0, 0, 
	    1191, 1192, 5, 83, 0, 0, 1192, 98, 1, 0, 0, 0, 1193, 1194, 5, 44, 0, 
	    0, 1194, 100, 1, 0, 0, 0, 1195, 1196, 5, 67, 0, 0, 1196, 1197, 5, 79, 
	    0, 0, 1197, 1198, 5, 77, 0, 0, 1198, 1199, 5, 77, 0, 0, 1199, 1200, 
	    5, 69, 0, 0, 1200, 1201, 5, 78, 0, 0, 1201, 1202, 5, 84, 0, 0, 1202, 
	    102, 1, 0, 0, 0, 1203, 1204, 5, 67, 0, 0, 1204, 1205, 5, 79, 0, 0, 
	    1205, 1206, 5, 77, 0, 0, 1206, 1207, 5, 77, 0, 0, 1207, 1208, 5, 73, 
	    0, 0, 1208, 1209, 5, 84, 0, 0, 1209, 104, 1, 0, 0, 0, 1210, 1211, 5, 
	    67, 0, 0, 1211, 1212, 5, 79, 0, 0, 1212, 1213, 5, 77, 0, 0, 1213, 1214, 
	    5, 77, 0, 0, 1214, 1215, 5, 73, 0, 0, 1215, 1216, 5, 84, 0, 0, 1216, 
	    1217, 5, 84, 0, 0, 1217, 1218, 5, 69, 0, 0, 1218, 1219, 5, 68, 0, 0, 
	    1219, 106, 1, 0, 0, 0, 1220, 1221, 5, 67, 0, 0, 1221, 1222, 5, 79, 
	    0, 0, 1222, 1223, 5, 77, 0, 0, 1223, 1224, 5, 80, 0, 0, 1224, 1225, 
	    5, 79, 0, 0, 1225, 1226, 5, 85, 0, 0, 1226, 1227, 5, 78, 0, 0, 1227, 
	    1228, 5, 68, 0, 0, 1228, 108, 1, 0, 0, 0, 1229, 1230, 5, 67, 0, 0, 
	    1230, 1231, 5, 79, 0, 0, 1231, 1232, 5, 77, 0, 0, 1232, 1233, 5, 80, 
	    0, 0, 1233, 1234, 5, 82, 0, 0, 1234, 1235, 5, 69, 0, 0, 1235, 1236, 
	    5, 83, 0, 0, 1236, 1237, 5, 83, 0, 0, 1237, 1238, 5, 73, 0, 0, 1238, 
	    1239, 5, 79, 0, 0, 1239, 1240, 5, 78, 0, 0, 1240, 110, 1, 0, 0, 0, 
	    1241, 1242, 5, 67, 0, 0, 1242, 1243, 5, 79, 0, 0, 1243, 1244, 5, 78, 
	    0, 0, 1244, 1245, 5, 68, 0, 0, 1245, 1246, 5, 73, 0, 0, 1246, 1247, 
	    5, 84, 0, 0, 1247, 1248, 5, 73, 0, 0, 1248, 1249, 5, 79, 0, 0, 1249, 
	    1250, 5, 78, 0, 0, 1250, 1251, 5, 65, 0, 0, 1251, 1252, 5, 76, 0, 0, 
	    1252, 112, 1, 0, 0, 0, 1253, 1254, 5, 67, 0, 0, 1254, 1255, 5, 79, 
	    0, 0, 1255, 1256, 5, 78, 0, 0, 1256, 1257, 5, 78, 0, 0, 1257, 1258, 
	    5, 69, 0, 0, 1258, 1259, 5, 67, 0, 0, 1259, 1260, 5, 84, 0, 0, 1260, 
	    114, 1, 0, 0, 0, 1261, 1262, 5, 67, 0, 0, 1262, 1263, 5, 79, 0, 0, 
	    1263, 1264, 5, 78, 0, 0, 1264, 1265, 5, 78, 0, 0, 1265, 1266, 5, 69, 
	    0, 0, 1266, 1267, 5, 67, 0, 0, 1267, 1268, 5, 84, 0, 0, 1268, 1269, 
	    5, 73, 0, 0, 1269, 1270, 5, 79, 0, 0, 1270, 1271, 5, 78, 0, 0, 1271, 
	    116, 1, 0, 0, 0, 1272, 1273, 5, 67, 0, 0, 1273, 1274, 5, 79, 0, 0, 
	    1274, 1275, 5, 78, 0, 0, 1275, 1276, 5, 83, 0, 0, 1276, 1277, 5, 84, 
	    0, 0, 1277, 1278, 5, 82, 0, 0, 1278, 1279, 5, 65, 0, 0, 1279, 1280, 
	    5, 73, 0, 0, 1280, 1281, 5, 78, 0, 0, 1281, 1282, 5, 84, 0, 0, 1282, 
	    118, 1, 0, 0, 0, 1283, 1284, 5, 67, 0, 0, 1284, 1285, 5, 79, 0, 0, 
	    1285, 1286, 5, 78, 0, 0, 1286, 1287, 5, 84, 0, 0, 1287, 1288, 5, 73, 
	    0, 0, 1288, 1289, 5, 78, 0, 0, 1289, 1290, 5, 85, 0, 0, 1290, 1291, 
	    5, 69, 0, 0, 1291, 120, 1, 0, 0, 0, 1292, 1293, 5, 67, 0, 0, 1293, 
	    1294, 5, 79, 0, 0, 1294, 1295, 5, 80, 0, 0, 1295, 1296, 5, 65, 0, 0, 
	    1296, 1297, 5, 82, 0, 0, 1297, 1298, 5, 84, 0, 0, 1298, 1299, 5, 73, 
	    0, 0, 1299, 1300, 5, 84, 0, 0, 1300, 1301, 5, 73, 0, 0, 1301, 1302, 
	    5, 79, 0, 0, 1302, 1303, 5, 78, 0, 0, 1303, 122, 1, 0, 0, 0, 1304, 
	    1305, 5, 67, 0, 0, 1305, 1306, 5, 79, 0, 0, 1306, 1307, 5, 80, 0, 0, 
	    1307, 1308, 5, 89, 0, 0, 1308, 124, 1, 0, 0, 0, 1309, 1310, 5, 67, 
	    0, 0, 1310, 1311, 5, 79, 0, 0, 1311, 1312, 5, 85, 0, 0, 1312, 1313, 
	    5, 78, 0, 0, 1313, 1314, 5, 84, 0, 0, 1314, 126, 1, 0, 0, 0, 1315, 
	    1316, 5, 67, 0, 0, 1316, 1317, 5, 82, 0, 0, 1317, 1318, 5, 69, 0, 0, 
	    1318, 1319, 5, 65, 0, 0, 1319, 1320, 5, 84, 0, 0, 1320, 1321, 5, 69, 
	    0, 0, 1321, 128, 1, 0, 0, 0, 1322, 1323, 5, 67, 0, 0, 1323, 1324, 5, 
	    82, 0, 0, 1324, 1325, 5, 79, 0, 0, 1325, 1326, 5, 83, 0, 0, 1326, 1327, 
	    5, 83, 0, 0, 1327, 130, 1, 0, 0, 0, 1328, 1329, 5, 67, 0, 0, 1329, 
	    1330, 5, 85, 0, 0, 1330, 1331, 5, 66, 0, 0, 1331, 1332, 5, 69, 0, 0, 
	    1332, 132, 1, 0, 0, 0, 1333, 1334, 5, 67, 0, 0, 1334, 1335, 5, 85, 
	    0, 0, 1335, 1336, 5, 82, 0, 0, 1336, 1337, 5, 82, 0, 0, 1337, 1338, 
	    5, 69, 0, 0, 1338, 1339, 5, 78, 0, 0, 1339, 1340, 5, 84, 0, 0, 1340, 
	    134, 1, 0, 0, 0, 1341, 1342, 5, 67, 0, 0, 1342, 1343, 5, 85, 0, 0, 
	    1343, 1344, 5, 82, 0, 0, 1344, 1345, 5, 82, 0, 0, 1345, 1346, 5, 69, 
	    0, 0, 1346, 1347, 5, 78, 0, 0, 1347, 1348, 5, 84, 0, 0, 1348, 1349, 
	    5, 95, 0, 0, 1349, 1350, 5, 82, 0, 0, 1350, 1351, 5, 79, 0, 0, 1351, 
	    1352, 5, 76, 0, 0, 1352, 1353, 5, 69, 0, 0, 1353, 136, 1, 0, 0, 0, 
	    1354, 1355, 5, 67, 0, 0, 1355, 1356, 5, 85, 0, 0, 1356, 1357, 5, 83, 
	    0, 0, 1357, 1358, 5, 84, 0, 0, 1358, 1359, 5, 79, 0, 0, 1359, 1360, 
	    5, 77, 0, 0, 1360, 1361, 5, 95, 0, 0, 1361, 1362, 5, 72, 0, 0, 1362, 
	    1363, 5, 79, 0, 0, 1363, 1364, 5, 76, 0, 0, 1364, 1365, 5, 73, 0, 0, 
	    1365, 1366, 5, 68, 0, 0, 1366, 1367, 5, 65, 0, 0, 1367, 1368, 5, 89, 
	    0, 0, 1368, 138, 1, 0, 0, 0, 1369, 1370, 5, 68, 0, 0, 1370, 1371, 5, 
	    65, 0, 0, 1371, 1372, 5, 84, 0, 0, 1372, 1373, 5, 65, 0, 0, 1373, 140, 
	    1, 0, 0, 0, 1374, 1375, 5, 68, 0, 0, 1375, 1376, 5, 65, 0, 0, 1376, 
	    1377, 5, 84, 0, 0, 1377, 1378, 5, 65, 0, 0, 1378, 1379, 5, 66, 0, 0, 
	    1379, 1380, 5, 65, 0, 0, 1380, 1381, 5, 83, 0, 0, 1381, 1382, 5, 69, 
	    0, 0, 1382, 142, 1, 0, 0, 0, 1383, 1384, 5, 68, 0, 0, 1384, 1385, 5, 
	    65, 0, 0, 1385, 1386, 5, 84, 0, 0, 1386, 1387, 5, 65, 0, 0, 1387, 1388, 
	    5, 83, 0, 0, 1388, 1389, 5, 72, 0, 0, 1389, 1390, 5, 65, 0, 0, 1390, 
	    1391, 5, 82, 0, 0, 1391, 1392, 5, 69, 0, 0, 1392, 144, 1, 0, 0, 0, 
	    1393, 1394, 5, 68, 0, 0, 1394, 1395, 5, 65, 0, 0, 1395, 1396, 5, 84, 
	    0, 0, 1396, 1397, 5, 69, 0, 0, 1397, 146, 1, 0, 0, 0, 1398, 1399, 5, 
	    68, 0, 0, 1399, 1400, 5, 65, 0, 0, 1400, 1401, 5, 84, 0, 0, 1401, 1402, 
	    5, 69, 0, 0, 1402, 1403, 5, 84, 0, 0, 1403, 1404, 5, 73, 0, 0, 1404, 
	    1405, 5, 77, 0, 0, 1405, 1406, 5, 69, 0, 0, 1406, 148, 1, 0, 0, 0, 
	    1407, 1408, 5, 68, 0, 0, 1408, 1409, 5, 65, 0, 0, 1409, 1410, 5, 89, 
	    0, 0, 1410, 150, 1, 0, 0, 0, 1411, 1412, 5, 68, 0, 0, 1412, 1413, 5, 
	    65, 0, 0, 1413, 1414, 5, 89, 0, 0, 1414, 1415, 5, 79, 0, 0, 1415, 1416, 
	    5, 70, 0, 0, 1416, 1417, 5, 87, 0, 0, 1417, 1418, 5, 69, 0, 0, 1418, 
	    1419, 5, 69, 0, 0, 1419, 1420, 5, 75, 0, 0, 1420, 152, 1, 0, 0, 0, 
	    1421, 1422, 5, 68, 0, 0, 1422, 1423, 5, 65, 0, 0, 1423, 1424, 5, 89, 
	    0, 0, 1424, 1425, 5, 79, 0, 0, 1425, 1426, 5, 70, 0, 0, 1426, 1427, 
	    5, 89, 0, 0, 1427, 1428, 5, 69, 0, 0, 1428, 1429, 5, 65, 0, 0, 1429, 
	    1430, 5, 82, 0, 0, 1430, 154, 1, 0, 0, 0, 1431, 1432, 5, 68, 0, 0, 
	    1432, 1433, 5, 65, 0, 0, 1433, 1434, 5, 84, 0, 0, 1434, 1435, 5, 69, 
	    0, 0, 1435, 1436, 5, 84, 0, 0, 1436, 1437, 5, 73, 0, 0, 1437, 1438, 
	    5, 77, 0, 0, 1438, 1439, 5, 69, 0, 0, 1439, 1440, 5, 95, 0, 0, 1440, 
	    1441, 5, 68, 0, 0, 1441, 1442, 5, 73, 0, 0, 1442, 1443, 5, 70, 0, 0, 
	    1443, 1444, 5, 70, 0, 0, 1444, 156, 1, 0, 0, 0, 1445, 1446, 5, 68, 
	    0, 0, 1446, 1447, 5, 65, 0, 0, 1447, 1448, 5, 84, 0, 0, 1448, 1449, 
	    5, 69, 0, 0, 1449, 1450, 5, 95, 0, 0, 1450, 1451, 5, 68, 0, 0, 1451, 
	    1452, 5, 73, 0, 0, 1452, 1453, 5, 70, 0, 0, 1453, 1454, 5, 70, 0, 0, 
	    1454, 158, 1, 0, 0, 0, 1455, 1456, 5, 68, 0, 0, 1456, 1457, 5, 69, 
	    0, 0, 1457, 1458, 5, 65, 0, 0, 1458, 1459, 5, 76, 0, 0, 1459, 1460, 
	    5, 76, 0, 0, 1460, 1461, 5, 79, 0, 0, 1461, 1462, 5, 67, 0, 0, 1462, 
	    1463, 5, 65, 0, 0, 1463, 1464, 5, 84, 0, 0, 1464, 1465, 5, 69, 0, 0, 
	    1465, 160, 1, 0, 0, 0, 1466, 1467, 5, 68, 0, 0, 1467, 1468, 5, 69, 
	    0, 0, 1468, 1469, 5, 67, 0, 0, 1469, 1470, 5, 76, 0, 0, 1470, 1471, 
	    5, 65, 0, 0, 1471, 1472, 5, 82, 0, 0, 1472, 1473, 5, 69, 0, 0, 1473, 
	    162, 1, 0, 0, 0, 1474, 1475, 5, 68, 0, 0, 1475, 1476, 5, 69, 0, 0, 
	    1476, 1477, 5, 70, 0, 0, 1477, 1478, 5, 65, 0, 0, 1478, 1479, 5, 85, 
	    0, 0, 1479, 1480, 5, 76, 0, 0, 1480, 1481, 5, 84, 0, 0, 1481, 164, 
	    1, 0, 0, 0, 1482, 1483, 5, 68, 0, 0, 1483, 1484, 5, 69, 0, 0, 1484, 
	    1485, 5, 70, 0, 0, 1485, 1486, 5, 65, 0, 0, 1486, 1487, 5, 85, 0, 0, 
	    1487, 1488, 5, 76, 0, 0, 1488, 1489, 5, 84, 0, 0, 1489, 1490, 5, 83, 
	    0, 0, 1490, 166, 1, 0, 0, 0, 1491, 1492, 5, 68, 0, 0, 1492, 1493, 5, 
	    69, 0, 0, 1493, 1494, 5, 70, 0, 0, 1494, 1495, 5, 73, 0, 0, 1495, 1496, 
	    5, 78, 0, 0, 1496, 1497, 5, 69, 0, 0, 1497, 168, 1, 0, 0, 0, 1498, 
	    1499, 5, 68, 0, 0, 1499, 1500, 5, 69, 0, 0, 1500, 1501, 5, 70, 0, 0, 
	    1501, 1502, 5, 73, 0, 0, 1502, 1503, 5, 78, 0, 0, 1503, 1504, 5, 69, 
	    0, 0, 1504, 1505, 5, 82, 0, 0, 1505, 170, 1, 0, 0, 0, 1506, 1507, 5, 
	    68, 0, 0, 1507, 1508, 5, 69, 0, 0, 1508, 1509, 5, 76, 0, 0, 1509, 1510, 
	    5, 69, 0, 0, 1510, 1511, 5, 84, 0, 0, 1511, 1512, 5, 69, 0, 0, 1512, 
	    172, 1, 0, 0, 0, 1513, 1514, 5, 68, 0, 0, 1514, 1515, 5, 69, 0, 0, 
	    1515, 1516, 5, 76, 0, 0, 1516, 1517, 5, 73, 0, 0, 1517, 1518, 5, 77, 
	    0, 0, 1518, 1519, 5, 73, 0, 0, 1519, 1520, 5, 84, 0, 0, 1520, 1521, 
	    5, 69, 0, 0, 1521, 1522, 5, 68, 0, 0, 1522, 174, 1, 0, 0, 0, 1523, 
	    1524, 5, 68, 0, 0, 1524, 1525, 5, 69, 0, 0, 1525, 1526, 5, 76, 0, 0, 
	    1526, 1527, 5, 73, 0, 0, 1527, 1528, 5, 77, 0, 0, 1528, 1529, 5, 73, 
	    0, 0, 1529, 1530, 5, 84, 0, 0, 1530, 1531, 5, 69, 0, 0, 1531, 1532, 
	    5, 82, 0, 0, 1532, 176, 1, 0, 0, 0, 1533, 1534, 5, 68, 0, 0, 1534, 
	    1535, 5, 69, 0, 0, 1535, 1536, 5, 78, 0, 0, 1536, 1537, 5, 89, 0, 0, 
	    1537, 178, 1, 0, 0, 0, 1538, 1539, 5, 68, 0, 0, 1539, 1540, 5, 69, 
	    0, 0, 1540, 1541, 5, 83, 0, 0, 1541, 1542, 5, 67, 0, 0, 1542, 180, 
	    1, 0, 0, 0, 1543, 1544, 5, 68, 0, 0, 1544, 1545, 5, 69, 0, 0, 1545, 
	    1546, 5, 83, 0, 0, 1546, 1547, 5, 67, 0, 0, 1547, 1548, 5, 82, 0, 0, 
	    1548, 1549, 5, 73, 0, 0, 1549, 1550, 5, 66, 0, 0, 1550, 1551, 5, 69, 
	    0, 0, 1551, 182, 1, 0, 0, 0, 1552, 1553, 5, 68, 0, 0, 1553, 1554, 5, 
	    69, 0, 0, 1554, 1555, 5, 83, 0, 0, 1555, 1556, 5, 67, 0, 0, 1556, 1557, 
	    5, 82, 0, 0, 1557, 1558, 5, 73, 0, 0, 1558, 1559, 5, 80, 0, 0, 1559, 
	    1560, 5, 84, 0, 0, 1560, 1561, 5, 79, 0, 0, 1561, 1562, 5, 82, 0, 0, 
	    1562, 184, 1, 0, 0, 0, 1563, 1564, 5, 68, 0, 0, 1564, 1565, 5, 69, 
	    0, 0, 1565, 1566, 5, 84, 0, 0, 1566, 1567, 5, 69, 0, 0, 1567, 1568, 
	    5, 82, 0, 0, 1568, 1569, 5, 77, 0, 0, 1569, 1570, 5, 73, 0, 0, 1570, 
	    1571, 5, 78, 0, 0, 1571, 1572, 5, 73, 0, 0, 1572, 1573, 5, 83, 0, 0, 
	    1573, 1574, 5, 84, 0, 0, 1574, 1575, 5, 73, 0, 0, 1575, 1576, 5, 67, 
	    0, 0, 1576, 186, 1, 0, 0, 0, 1577, 1578, 5, 68, 0, 0, 1578, 1579, 5, 
	    73, 0, 0, 1579, 1580, 5, 83, 0, 0, 1580, 1581, 5, 84, 0, 0, 1581, 1582, 
	    5, 73, 0, 0, 1582, 1583, 5, 78, 0, 0, 1583, 1584, 5, 67, 0, 0, 1584, 
	    1585, 5, 84, 0, 0, 1585, 188, 1, 0, 0, 0, 1586, 1587, 5, 68, 0, 0, 
	    1587, 1588, 5, 73, 0, 0, 1588, 1589, 5, 83, 0, 0, 1589, 1590, 5, 84, 
	    0, 0, 1590, 1591, 5, 75, 0, 0, 1591, 1592, 5, 69, 0, 0, 1592, 1593, 
	    5, 89, 0, 0, 1593, 190, 1, 0, 0, 0, 1594, 1595, 5, 68, 0, 0, 1595, 
	    1596, 5, 73, 0, 0, 1596, 1597, 5, 83, 0, 0, 1597, 1598, 5, 84, 0, 0, 
	    1598, 1599, 5, 82, 0, 0, 1599, 1600, 5, 73, 0, 0, 1600, 1601, 5, 66, 
	    0, 0, 1601, 1602, 5, 85, 0, 0, 1602, 1603, 5, 84, 0, 0, 1603, 1604, 
	    5, 69, 0, 0, 1604, 1605, 5, 68, 0, 0, 1605, 192, 1, 0, 0, 0, 1606, 
	    1607, 5, 68, 0, 0, 1607, 1608, 5, 73, 0, 0, 1608, 1609, 5, 83, 0, 0, 
	    1609, 1610, 5, 84, 0, 0, 1610, 1611, 5, 83, 0, 0, 1611, 1612, 5, 84, 
	    0, 0, 1612, 1613, 5, 89, 0, 0, 1613, 1614, 5, 76, 0, 0, 1614, 1615, 
	    5, 69, 0, 0, 1615, 194, 1, 0, 0, 0, 1616, 1617, 5, 68, 0, 0, 1617, 
	    1618, 5, 69, 0, 0, 1618, 1619, 5, 84, 0, 0, 1619, 1620, 5, 65, 0, 0, 
	    1620, 1621, 5, 67, 0, 0, 1621, 1622, 5, 72, 0, 0, 1622, 196, 1, 0, 
	    0, 0, 1623, 1624, 5, 68, 0, 0, 1624, 1625, 5, 79, 0, 0, 1625, 198, 
	    1, 0, 0, 0, 1626, 1627, 5, 68, 0, 0, 1627, 1628, 5, 79, 0, 0, 1628, 
	    1629, 5, 85, 0, 0, 1629, 1630, 5, 66, 0, 0, 1630, 1631, 5, 76, 0, 0, 
	    1631, 1632, 5, 69, 0, 0, 1632, 200, 1, 0, 0, 0, 1633, 1634, 5, 68, 
	    0, 0, 1634, 1635, 5, 82, 0, 0, 1635, 1636, 5, 79, 0, 0, 1636, 1637, 
	    5, 80, 0, 0, 1637, 202, 1, 0, 0, 0, 1638, 1639, 5, 69, 0, 0, 1639, 
	    1640, 5, 76, 0, 0, 1640, 1641, 5, 83, 0, 0, 1641, 1642, 5, 69, 0, 0, 
	    1642, 204, 1, 0, 0, 0, 1643, 1644, 5, 69, 0, 0, 1644, 1645, 5, 76, 
	    0, 0, 1645, 1646, 5, 83, 0, 0, 1646, 1647, 5, 69, 0, 0, 1647, 1648, 
	    5, 73, 0, 0, 1648, 1649, 5, 70, 0, 0, 1649, 206, 1, 0, 0, 0, 1650, 
	    1651, 5, 69, 0, 0, 1651, 1652, 5, 77, 0, 0, 1652, 1653, 5, 80, 0, 0, 
	    1653, 1654, 5, 84, 0, 0, 1654, 1655, 5, 89, 0, 0, 1655, 208, 1, 0, 
	    0, 0, 1656, 1657, 5, 69, 0, 0, 1657, 1658, 5, 78, 0, 0, 1658, 1659, 
	    5, 67, 0, 0, 1659, 1660, 5, 79, 0, 0, 1660, 1661, 5, 68, 0, 0, 1661, 
	    1662, 5, 69, 0, 0, 1662, 210, 1, 0, 0, 0, 1663, 1664, 5, 69, 0, 0, 
	    1664, 1665, 5, 78, 0, 0, 1665, 1666, 5, 67, 0, 0, 1666, 1667, 5, 79, 
	    0, 0, 1667, 1668, 5, 68, 0, 0, 1668, 1669, 5, 73, 0, 0, 1669, 1670, 
	    5, 78, 0, 0, 1670, 1671, 5, 71, 0, 0, 1671, 212, 1, 0, 0, 0, 1672, 
	    1673, 5, 69, 0, 0, 1673, 1674, 5, 78, 0, 0, 1674, 1675, 5, 68, 0, 0, 
	    1675, 214, 1, 0, 0, 0, 1676, 1677, 5, 69, 0, 0, 1677, 1678, 5, 82, 
	    0, 0, 1678, 1679, 5, 82, 0, 0, 1679, 1680, 5, 79, 0, 0, 1680, 1681, 
	    5, 82, 0, 0, 1681, 216, 1, 0, 0, 0, 1682, 1683, 5, 69, 0, 0, 1683, 
	    1684, 5, 83, 0, 0, 1684, 1685, 5, 67, 0, 0, 1685, 1686, 5, 65, 0, 0, 
	    1686, 1687, 5, 80, 0, 0, 1687, 1688, 5, 69, 0, 0, 1688, 218, 1, 0, 
	    0, 0, 1689, 1690, 5, 69, 0, 0, 1690, 1691, 5, 86, 0, 0, 1691, 1692, 
	    5, 69, 0, 0, 1692, 1693, 5, 78, 0, 0, 1693, 220, 1, 0, 0, 0, 1694, 
	    1695, 5, 69, 0, 0, 1695, 1696, 5, 88, 0, 0, 1696, 1697, 5, 67, 0, 0, 
	    1697, 1698, 5, 69, 0, 0, 1698, 1699, 5, 80, 0, 0, 1699, 1700, 5, 84, 
	    0, 0, 1700, 222, 1, 0, 0, 0, 1701, 1702, 5, 69, 0, 0, 1702, 1703, 5, 
	    88, 0, 0, 1703, 1704, 5, 67, 0, 0, 1704, 1705, 5, 69, 0, 0, 1705, 1706, 
	    5, 80, 0, 0, 1706, 1707, 5, 84, 0, 0, 1707, 1708, 5, 73, 0, 0, 1708, 
	    1709, 5, 79, 0, 0, 1709, 1710, 5, 78, 0, 0, 1710, 224, 1, 0, 0, 0, 
	    1711, 1712, 5, 69, 0, 0, 1712, 1713, 5, 88, 0, 0, 1713, 1714, 5, 67, 
	    0, 0, 1714, 1715, 5, 76, 0, 0, 1715, 1716, 5, 85, 0, 0, 1716, 1717, 
	    5, 68, 0, 0, 1717, 1718, 5, 69, 0, 0, 1718, 226, 1, 0, 0, 0, 1719, 
	    1720, 5, 69, 0, 0, 1720, 1721, 5, 88, 0, 0, 1721, 1722, 5, 67, 0, 0, 
	    1722, 1723, 5, 76, 0, 0, 1723, 1724, 5, 85, 0, 0, 1724, 1725, 5, 68, 
	    0, 0, 1725, 1726, 5, 73, 0, 0, 1726, 1727, 5, 78, 0, 0, 1727, 1728, 
	    5, 71, 0, 0, 1728, 228, 1, 0, 0, 0, 1729, 1730, 5, 69, 0, 0, 1730, 
	    1731, 5, 88, 0, 0, 1731, 1732, 5, 69, 0, 0, 1732, 1733, 5, 67, 0, 0, 
	    1733, 1734, 5, 85, 0, 0, 1734, 1735, 5, 84, 0, 0, 1735, 1736, 5, 69, 
	    0, 0, 1736, 230, 1, 0, 0, 0, 1737, 1738, 5, 69, 0, 0, 1738, 1739, 5, 
	    88, 0, 0, 1739, 1740, 5, 73, 0, 0, 1740, 1741, 5, 83, 0, 0, 1741, 1742, 
	    5, 84, 0, 0, 1742, 1743, 5, 83, 0, 0, 1743, 232, 1, 0, 0, 0, 1744, 
	    1745, 5, 69, 0, 0, 1745, 1746, 5, 88, 0, 0, 1746, 1747, 5, 80, 0, 0, 
	    1747, 1748, 5, 76, 0, 0, 1748, 1749, 5, 65, 0, 0, 1749, 1750, 5, 73, 
	    0, 0, 1750, 1751, 5, 78, 0, 0, 1751, 234, 1, 0, 0, 0, 1752, 1753, 5, 
	    69, 0, 0, 1753, 1754, 5, 88, 0, 0, 1754, 1755, 5, 84, 0, 0, 1755, 1756, 
	    5, 69, 0, 0, 1756, 1757, 5, 82, 0, 0, 1757, 1758, 5, 78, 0, 0, 1758, 
	    1759, 5, 65, 0, 0, 1759, 1760, 5, 76, 0, 0, 1760, 236, 1, 0, 0, 0, 
	    1761, 1762, 5, 69, 0, 0, 1762, 1763, 5, 88, 0, 0, 1763, 1764, 5, 84, 
	    0, 0, 1764, 1765, 5, 82, 0, 0, 1765, 1766, 5, 65, 0, 0, 1766, 1767, 
	    5, 67, 0, 0, 1767, 1768, 5, 84, 0, 0, 1768, 238, 1, 0, 0, 0, 1769, 
	    1770, 5, 70, 0, 0, 1770, 1771, 5, 65, 0, 0, 1771, 1772, 5, 76, 0, 0, 
	    1772, 1773, 5, 83, 0, 0, 1773, 1774, 5, 69, 0, 0, 1774, 240, 1, 0, 
	    0, 0, 1775, 1776, 5, 70, 0, 0, 1776, 1777, 5, 69, 0, 0, 1777, 1778, 
	    5, 84, 0, 0, 1778, 1779, 5, 67, 0, 0, 1779, 1780, 5, 72, 0, 0, 1780, 
	    242, 1, 0, 0, 0, 1781, 1782, 5, 70, 0, 0, 1782, 1783, 5, 73, 0, 0, 
	    1783, 1784, 5, 69, 0, 0, 1784, 1785, 5, 76, 0, 0, 1785, 1786, 5, 68, 
	    0, 0, 1786, 1787, 5, 83, 0, 0, 1787, 244, 1, 0, 0, 0, 1788, 1789, 5, 
	    70, 0, 0, 1789, 1790, 5, 73, 0, 0, 1790, 1791, 5, 76, 0, 0, 1791, 1792, 
	    5, 84, 0, 0, 1792, 1793, 5, 69, 0, 0, 1793, 1794, 5, 82, 0, 0, 1794, 
	    246, 1, 0, 0, 0, 1795, 1796, 5, 70, 0, 0, 1796, 1797, 5, 73, 0, 0, 
	    1797, 1798, 5, 78, 0, 0, 1798, 1799, 5, 65, 0, 0, 1799, 1800, 5, 76, 
	    0, 0, 1800, 248, 1, 0, 0, 0, 1801, 1802, 5, 70, 0, 0, 1802, 1803, 5, 
	    73, 0, 0, 1803, 1804, 5, 82, 0, 0, 1804, 1805, 5, 83, 0, 0, 1805, 1806, 
	    5, 84, 0, 0, 1806, 250, 1, 0, 0, 0, 1807, 1808, 5, 70, 0, 0, 1808, 
	    1809, 5, 79, 0, 0, 1809, 1810, 5, 76, 0, 0, 1810, 1811, 5, 76, 0, 0, 
	    1811, 1812, 5, 79, 0, 0, 1812, 1813, 5, 87, 0, 0, 1813, 1814, 5, 73, 
	    0, 0, 1814, 1815, 5, 78, 0, 0, 1815, 1816, 5, 71, 0, 0, 1816, 252, 
	    1, 0, 0, 0, 1817, 1818, 5, 70, 0, 0, 1818, 1819, 5, 79, 0, 0, 1819, 
	    1820, 5, 82, 0, 0, 1820, 254, 1, 0, 0, 0, 1821, 1822, 5, 70, 0, 0, 
	    1822, 1823, 5, 79, 0, 0, 1823, 1824, 5, 82, 0, 0, 1824, 1825, 5, 77, 
	    0, 0, 1825, 1826, 5, 65, 0, 0, 1826, 1827, 5, 84, 0, 0, 1827, 256, 
	    1, 0, 0, 0, 1828, 1829, 5, 70, 0, 0, 1829, 1830, 5, 82, 0, 0, 1830, 
	    1831, 5, 73, 0, 0, 1831, 1832, 5, 68, 0, 0, 1832, 1833, 5, 65, 0, 0, 
	    1833, 1834, 5, 89, 0, 0, 1834, 258, 1, 0, 0, 0, 1835, 1836, 5, 70, 
	    0, 0, 1836, 1837, 5, 82, 0, 0, 1837, 1838, 5, 79, 0, 0, 1838, 1839, 
	    5, 77, 0, 0, 1839, 260, 1, 0, 0, 0, 1840, 1841, 5, 70, 0, 0, 1841, 
	    1842, 5, 85, 0, 0, 1842, 1843, 5, 76, 0, 0, 1843, 1844, 5, 76, 0, 0, 
	    1844, 262, 1, 0, 0, 0, 1845, 1846, 5, 70, 0, 0, 1846, 1847, 5, 85, 
	    0, 0, 1847, 1848, 5, 78, 0, 0, 1848, 1849, 5, 67, 0, 0, 1849, 1850, 
	    5, 84, 0, 0, 1850, 1851, 5, 73, 0, 0, 1851, 1852, 5, 79, 0, 0, 1852, 
	    1853, 5, 78, 0, 0, 1853, 264, 1, 0, 0, 0, 1854, 1855, 5, 70, 0, 0, 
	    1855, 1856, 5, 85, 0, 0, 1856, 1857, 5, 78, 0, 0, 1857, 1858, 5, 67, 
	    0, 0, 1858, 1859, 5, 84, 0, 0, 1859, 1860, 5, 73, 0, 0, 1860, 1861, 
	    5, 79, 0, 0, 1861, 1862, 5, 78, 0, 0, 1862, 1863, 5, 83, 0, 0, 1863, 
	    266, 1, 0, 0, 0, 1864, 1865, 5, 71, 0, 0, 1865, 1866, 5, 69, 0, 0, 
	    1866, 1867, 5, 78, 0, 0, 1867, 1868, 5, 69, 0, 0, 1868, 1869, 5, 82, 
	    0, 0, 1869, 1870, 5, 65, 0, 0, 1870, 1871, 5, 84, 0, 0, 1871, 1872, 
	    5, 69, 0, 0, 1872, 1873, 5, 68, 0, 0, 1873, 268, 1, 0, 0, 0, 1874, 
	    1875, 5, 71, 0, 0, 1875, 1876, 5, 82, 0, 0, 1876, 1877, 5, 65, 0, 0, 
	    1877, 1878, 5, 67, 0, 0, 1878, 1879, 5, 69, 0, 0, 1879, 270, 1, 0, 
	    0, 0, 1880, 1881, 5, 71, 0, 0, 1881, 1882, 5, 82, 0, 0, 1882, 1883, 
	    5, 65, 0, 0, 1883, 1884, 5, 78, 0, 0, 1884, 1885, 5, 84, 0, 0, 1885, 
	    272, 1, 0, 0, 0, 1886, 1887, 5, 71, 0, 0, 1887, 1888, 5, 82, 0, 0, 
	    1888, 1889, 5, 65, 0, 0, 1889, 1890, 5, 78, 0, 0, 1890, 1891, 5, 84, 
	    0, 0, 1891, 1892, 5, 69, 0, 0, 1892, 1893, 5, 68, 0, 0, 1893, 274, 
	    1, 0, 0, 0, 1894, 1895, 5, 71, 0, 0, 1895, 1896, 5, 82, 0, 0, 1896, 
	    1897, 5, 65, 0, 0, 1897, 1898, 5, 78, 0, 0, 1898, 1899, 5, 84, 0, 0, 
	    1899, 1900, 5, 83, 0, 0, 1900, 276, 1, 0, 0, 0, 1901, 1902, 5, 71, 
	    0, 0, 1902, 1903, 5, 82, 0, 0, 1903, 1904, 5, 65, 0, 0, 1904, 1905, 
	    5, 80, 0, 0, 1905, 1906, 5, 72, 0, 0, 1906, 1907, 5, 86, 0, 0, 1907, 
	    1908, 5, 73, 0, 0, 1908, 1909, 5, 90, 0, 0, 1909, 278, 1, 0, 0, 0, 
	    1910, 1911, 5, 71, 0, 0, 1911, 1912, 5, 82, 0, 0, 1912, 1913, 5, 79, 
	    0, 0, 1913, 1914, 5, 85, 0, 0, 1914, 1915, 5, 80, 0, 0, 1915, 280, 
	    1, 0, 0, 0, 1916, 1917, 5, 71, 0, 0, 1917, 1918, 5, 82, 0, 0, 1918, 
	    1919, 5, 79, 0, 0, 1919, 1920, 5, 85, 0, 0, 1920, 1921, 5, 80, 0, 0, 
	    1921, 1922, 5, 73, 0, 0, 1922, 1923, 5, 78, 0, 0, 1923, 1924, 5, 71, 
	    0, 0, 1924, 282, 1, 0, 0, 0, 1925, 1926, 5, 71, 0, 0, 1926, 1927, 5, 
	    82, 0, 0, 1927, 1928, 5, 79, 0, 0, 1928, 1929, 5, 85, 0, 0, 1929, 1930, 
	    5, 80, 0, 0, 1930, 1931, 5, 83, 0, 0, 1931, 284, 1, 0, 0, 0, 1932, 
	    1933, 5, 71, 0, 0, 1933, 1934, 5, 90, 0, 0, 1934, 1935, 5, 73, 0, 0, 
	    1935, 1936, 5, 80, 0, 0, 1936, 286, 1, 0, 0, 0, 1937, 1938, 5, 72, 
	    0, 0, 1938, 1939, 5, 65, 0, 0, 1939, 1940, 5, 86, 0, 0, 1940, 1941, 
	    5, 73, 0, 0, 1941, 1942, 5, 78, 0, 0, 1942, 1943, 5, 71, 0, 0, 1943, 
	    288, 1, 0, 0, 0, 1944, 1945, 5, 72, 0, 0, 1945, 1946, 5, 69, 0, 0, 
	    1946, 1947, 5, 65, 0, 0, 1947, 1948, 5, 68, 0, 0, 1948, 1949, 5, 69, 
	    0, 0, 1949, 1950, 5, 82, 0, 0, 1950, 290, 1, 0, 0, 0, 1951, 1952, 5, 
	    72, 0, 0, 1952, 1953, 5, 79, 0, 0, 1953, 1954, 5, 85, 0, 0, 1954, 1955, 
	    5, 82, 0, 0, 1955, 292, 1, 0, 0, 0, 1956, 1957, 5, 73, 0, 0, 1957, 
	    1958, 5, 68, 0, 0, 1958, 1959, 5, 69, 0, 0, 1959, 1960, 5, 78, 0, 0, 
	    1960, 1961, 5, 84, 0, 0, 1961, 1962, 5, 73, 0, 0, 1962, 1963, 5, 84, 
	    0, 0, 1963, 1964, 5, 89, 0, 0, 1964, 294, 1, 0, 0, 0, 1965, 1966, 5, 
	    73, 0, 0, 1966, 1967, 5, 70, 0, 0, 1967, 296, 1, 0, 0, 0, 1968, 1969, 
	    5, 73, 0, 0, 1969, 1970, 5, 71, 0, 0, 1970, 1971, 5, 78, 0, 0, 1971, 
	    1972, 5, 79, 0, 0, 1972, 1973, 5, 82, 0, 0, 1973, 1974, 5, 69, 0, 0, 
	    1974, 298, 1, 0, 0, 0, 1975, 1976, 5, 73, 0, 0, 1976, 1977, 5, 77, 
	    0, 0, 1977, 1978, 5, 77, 0, 0, 1978, 1979, 5, 69, 0, 0, 1979, 1980, 
	    5, 68, 0, 0, 1980, 1981, 5, 73, 0, 0, 1981, 1982, 5, 65, 0, 0, 1982, 
	    1983, 5, 84, 0, 0, 1983, 1984, 5, 69, 0, 0, 1984, 300, 1, 0, 0, 0, 
	    1985, 1986, 5, 73, 0, 0, 1986, 1987, 5, 78, 0, 0, 1987, 302, 1, 0, 
	    0, 0, 1988, 1989, 5, 73, 0, 0, 1989, 1990, 5, 78, 0, 0, 1990, 1991, 
	    5, 67, 0, 0, 1991, 1992, 5, 76, 0, 0, 1992, 1993, 5, 85, 0, 0, 1993, 
	    1994, 5, 68, 0, 0, 1994, 1995, 5, 69, 0, 0, 1995, 304, 1, 0, 0, 0, 
	    1996, 1997, 5, 73, 0, 0, 1997, 1998, 5, 78, 0, 0, 1998, 1999, 5, 67, 
	    0, 0, 1999, 2000, 5, 76, 0, 0, 2000, 2001, 5, 85, 0, 0, 2001, 2002, 
	    5, 68, 0, 0, 2002, 2003, 5, 73, 0, 0, 2003, 2004, 5, 78, 0, 0, 2004, 
	    2005, 5, 71, 0, 0, 2005, 306, 1, 0, 0, 0, 2006, 2007, 5, 73, 0, 0, 
	    2007, 2008, 5, 78, 0, 0, 2008, 2009, 5, 73, 0, 0, 2009, 2010, 5, 84, 
	    0, 0, 2010, 2011, 5, 73, 0, 0, 2011, 2012, 5, 65, 0, 0, 2012, 2013, 
	    5, 76, 0, 0, 2013, 308, 1, 0, 0, 0, 2014, 2015, 5, 73, 0, 0, 2015, 
	    2016, 5, 78, 0, 0, 2016, 2017, 5, 78, 0, 0, 2017, 2018, 5, 69, 0, 0, 
	    2018, 2019, 5, 82, 0, 0, 2019, 310, 1, 0, 0, 0, 2020, 2021, 5, 73, 
	    0, 0, 2021, 2022, 5, 78, 0, 0, 2022, 2023, 5, 80, 0, 0, 2023, 2024, 
	    5, 85, 0, 0, 2024, 2025, 5, 84, 0, 0, 2025, 312, 1, 0, 0, 0, 2026, 
	    2027, 5, 73, 0, 0, 2027, 2028, 5, 78, 0, 0, 2028, 2029, 5, 80, 0, 0, 
	    2029, 2030, 5, 85, 0, 0, 2030, 2031, 5, 84, 0, 0, 2031, 2032, 5, 70, 
	    0, 0, 2032, 2033, 5, 79, 0, 0, 2033, 2034, 5, 82, 0, 0, 2034, 2035, 
	    5, 77, 0, 0, 2035, 2036, 5, 65, 0, 0, 2036, 2037, 5, 84, 0, 0, 2037, 
	    314, 1, 0, 0, 0, 2038, 2039, 5, 73, 0, 0, 2039, 2040, 5, 78, 0, 0, 
	    2040, 2041, 5, 84, 0, 0, 2041, 2042, 5, 69, 0, 0, 2042, 2043, 5, 82, 
	    0, 0, 2043, 2044, 5, 76, 0, 0, 2044, 2045, 5, 69, 0, 0, 2045, 2046, 
	    5, 65, 0, 0, 2046, 2047, 5, 86, 0, 0, 2047, 2048, 5, 69, 0, 0, 2048, 
	    2049, 5, 68, 0, 0, 2049, 316, 1, 0, 0, 0, 2050, 2051, 5, 73, 0, 0, 
	    2051, 2052, 5, 78, 0, 0, 2052, 2053, 5, 83, 0, 0, 2053, 2054, 5, 69, 
	    0, 0, 2054, 2055, 5, 82, 0, 0, 2055, 2056, 5, 84, 0, 0, 2056, 318, 
	    1, 0, 0, 0, 2057, 2058, 5, 73, 0, 0, 2058, 2059, 5, 78, 0, 0, 2059, 
	    2060, 5, 84, 0, 0, 2060, 2061, 5, 69, 0, 0, 2061, 2062, 5, 82, 0, 0, 
	    2062, 2063, 5, 83, 0, 0, 2063, 2064, 5, 69, 0, 0, 2064, 2065, 5, 67, 
	    0, 0, 2065, 2066, 5, 84, 0, 0, 2066, 320, 1, 0, 0, 0, 2067, 2068, 5, 
	    73, 0, 0, 2068, 2069, 5, 78, 0, 0, 2069, 2070, 5, 84, 0, 0, 2070, 2071, 
	    5, 69, 0, 0, 2071, 2072, 5, 82, 0, 0, 2072, 2073, 5, 86, 0, 0, 2073, 
	    2074, 5, 65, 0, 0, 2074, 2075, 5, 76, 0, 0, 2075, 322, 1, 0, 0, 0, 
	    2076, 2077, 5, 73, 0, 0, 2077, 2078, 5, 78, 0, 0, 2078, 2079, 5, 84, 
	    0, 0, 2079, 2080, 5, 79, 0, 0, 2080, 324, 1, 0, 0, 0, 2081, 2082, 5, 
	    73, 0, 0, 2082, 2083, 5, 78, 0, 0, 2083, 2084, 5, 86, 0, 0, 2084, 2085, 
	    5, 79, 0, 0, 2085, 2086, 5, 75, 0, 0, 2086, 2087, 5, 69, 0, 0, 2087, 
	    2088, 5, 82, 0, 0, 2088, 326, 1, 0, 0, 0, 2089, 2090, 5, 73, 0, 0, 
	    2090, 2091, 5, 79, 0, 0, 2091, 328, 1, 0, 0, 0, 2092, 2093, 5, 73, 
	    0, 0, 2093, 2094, 5, 83, 0, 0, 2094, 330, 1, 0, 0, 0, 2095, 2096, 5, 
	    73, 0, 0, 2096, 2097, 5, 83, 0, 0, 2097, 2098, 5, 79, 0, 0, 2098, 2099, 
	    5, 76, 0, 0, 2099, 2100, 5, 65, 0, 0, 2100, 2101, 5, 84, 0, 0, 2101, 
	    2102, 5, 73, 0, 0, 2102, 2103, 5, 79, 0, 0, 2103, 2104, 5, 78, 0, 0, 
	    2104, 332, 1, 0, 0, 0, 2105, 2106, 5, 73, 0, 0, 2106, 2107, 5, 83, 
	    0, 0, 2107, 2108, 5, 79, 0, 0, 2108, 2109, 5, 87, 0, 0, 2109, 2110, 
	    5, 69, 0, 0, 2110, 2111, 5, 69, 0, 0, 2111, 2112, 5, 75, 0, 0, 2112, 
	    334, 1, 0, 0, 0, 2113, 2114, 5, 73, 0, 0, 2114, 2115, 5, 83, 0, 0, 
	    2115, 2116, 5, 79, 0, 0, 2116, 2117, 5, 89, 0, 0, 2117, 2118, 5, 69, 
	    0, 0, 2118, 2119, 5, 65, 0, 0, 2119, 2120, 5, 82, 0, 0, 2120, 336, 
	    1, 0, 0, 0, 2121, 2122, 5, 73, 0, 0, 2122, 2123, 5, 84, 0, 0, 2123, 
	    2124, 5, 69, 0, 0, 2124, 2125, 5, 82, 0, 0, 2125, 2126, 5, 65, 0, 0, 
	    2126, 2127, 5, 84, 0, 0, 2127, 2128, 5, 69, 0, 0, 2128, 338, 1, 0, 
	    0, 0, 2129, 2130, 5, 73, 0, 0, 2130, 2131, 5, 76, 0, 0, 2131, 2132, 
	    5, 73, 0, 0, 2132, 2133, 5, 75, 0, 0, 2133, 2134, 5, 69, 0, 0, 2134, 
	    340, 1, 0, 0, 0, 2135, 2136, 5, 74, 0, 0, 2136, 2137, 5, 79, 0, 0, 
	    2137, 2138, 5, 73, 0, 0, 2138, 2139, 5, 78, 0, 0, 2139, 342, 1, 0, 
	    0, 0, 2140, 2141, 5, 74, 0, 0, 2141, 2142, 5, 83, 0, 0, 2142, 2143, 
	    5, 79, 0, 0, 2143, 2144, 5, 78, 0, 0, 2144, 344, 1, 0, 0, 0, 2145, 
	    2146, 5, 75, 0, 0, 2146, 2147, 5, 69, 0, 0, 2147, 2148, 5, 69, 0, 0, 
	    2148, 2149, 5, 80, 0, 0, 2149, 346, 1, 0, 0, 0, 2150, 2151, 5, 75, 
	    0, 0, 2151, 2152, 5, 69, 0, 0, 2152, 2153, 5, 89, 0, 0, 2153, 348, 
	    1, 0, 0, 0, 2154, 2155, 5, 75, 0, 0, 2155, 2156, 5, 69, 0, 0, 2156, 
	    2157, 5, 89, 0, 0, 2157, 2158, 5, 83, 0, 0, 2158, 350, 1, 0, 0, 0, 
	    2159, 2160, 5, 76, 0, 0, 2160, 2161, 5, 65, 0, 0, 2161, 2162, 5, 77, 
	    0, 0, 2162, 2163, 5, 66, 0, 0, 2163, 2164, 5, 68, 0, 0, 2164, 2165, 
	    5, 65, 0, 0, 2165, 352, 1, 0, 0, 0, 2166, 2167, 5, 76, 0, 0, 2167, 
	    2168, 5, 65, 0, 0, 2168, 2169, 5, 78, 0, 0, 2169, 2170, 5, 71, 0, 0, 
	    2170, 2171, 5, 85, 0, 0, 2171, 2172, 5, 65, 0, 0, 2172, 2173, 5, 71, 
	    0, 0, 2173, 2174, 5, 69, 0, 0, 2174, 354, 1, 0, 0, 0, 2175, 2176, 5, 
	    76, 0, 0, 2176, 2177, 5, 69, 0, 0, 2177, 2178, 5, 65, 0, 0, 2178, 2179, 
	    5, 86, 0, 0, 2179, 2180, 5, 69, 0, 0, 2180, 356, 1, 0, 0, 0, 2181, 
	    2182, 5, 76, 0, 0, 2182, 2183, 5, 65, 0, 0, 2183, 2184, 5, 83, 0, 0, 
	    2184, 2185, 5, 84, 0, 0, 2185, 358, 1, 0, 0, 0, 2186, 2187, 5, 76, 
	    0, 0, 2187, 2188, 5, 65, 0, 0, 2188, 2189, 5, 84, 0, 0, 2189, 2190, 
	    5, 69, 0, 0, 2190, 2191, 5, 82, 0, 0, 2191, 2192, 5, 65, 0, 0, 2192, 
	    2193, 5, 76, 0, 0, 2193, 360, 1, 0, 0, 0, 2194, 2195, 5, 76, 0, 0, 
	    2195, 2196, 5, 69, 0, 0, 2196, 2197, 5, 65, 0, 0, 2197, 2198, 5, 68, 
	    0, 0, 2198, 2199, 5, 73, 0, 0, 2199, 2200, 5, 78, 0, 0, 2200, 2201, 
	    5, 71, 0, 0, 2201, 362, 1, 0, 0, 0, 2202, 2203, 5, 76, 0, 0, 2203, 
	    2204, 5, 69, 0, 0, 2204, 2205, 5, 70, 0, 0, 2205, 2206, 5, 84, 0, 0, 
	    2206, 364, 1, 0, 0, 0, 2207, 2208, 5, 76, 0, 0, 2208, 2209, 5, 69, 
	    0, 0, 2209, 2210, 5, 86, 0, 0, 2210, 2211, 5, 69, 0, 0, 2211, 2212, 
	    5, 76, 0, 0, 2212, 366, 1, 0, 0, 0, 2213, 2214, 5, 76, 0, 0, 2214, 
	    2215, 5, 73, 0, 0, 2215, 2216, 5, 66, 0, 0, 2216, 2217, 5, 82, 0, 0, 
	    2217, 2218, 5, 65, 0, 0, 2218, 2219, 5, 82, 0, 0, 2219, 2220, 5, 89, 
	    0, 0, 2220, 368, 1, 0, 0, 0, 2221, 2222, 5, 76, 0, 0, 2222, 2223, 5, 
	    73, 0, 0, 2223, 2224, 5, 75, 0, 0, 2224, 2225, 5, 69, 0, 0, 2225, 370, 
	    1, 0, 0, 0, 2226, 2227, 5, 76, 0, 0, 2227, 2228, 5, 73, 0, 0, 2228, 
	    2229, 5, 77, 0, 0, 2229, 2230, 5, 73, 0, 0, 2230, 2231, 5, 84, 0, 0, 
	    2231, 372, 1, 0, 0, 0, 2232, 2233, 5, 76, 0, 0, 2233, 2234, 5, 73, 
	    0, 0, 2234, 2235, 5, 78, 0, 0, 2235, 2236, 5, 69, 0, 0, 2236, 2237, 
	    5, 83, 0, 0, 2237, 374, 1, 0, 0, 0, 2238, 2239, 5, 76, 0, 0, 2239, 
	    2240, 5, 73, 0, 0, 2240, 2241, 5, 83, 0, 0, 2241, 2242, 5, 84, 0, 0, 
	    2242, 2243, 5, 65, 0, 0, 2243, 2244, 5, 71, 0, 0, 2244, 2245, 5, 71, 
	    0, 0, 2245, 376, 1, 0, 0, 0, 2246, 2247, 5, 76, 0, 0, 2247, 2248, 5, 
	    79, 0, 0, 2248, 2249, 5, 67, 0, 0, 2249, 2250, 5, 65, 0, 0, 2250, 2251, 
	    5, 76, 0, 0, 2251, 378, 1, 0, 0, 0, 2252, 2253, 5, 76, 0, 0, 2253, 
	    2254, 5, 79, 0, 0, 2254, 2255, 5, 67, 0, 0, 2255, 2256, 5, 65, 0, 0, 
	    2256, 2257, 5, 84, 0, 0, 2257, 2258, 5, 73, 0, 0, 2258, 2259, 5, 79, 
	    0, 0, 2259, 2260, 5, 78, 0, 0, 2260, 380, 1, 0, 0, 0, 2261, 2262, 5, 
	    76, 0, 0, 2262, 2263, 5, 79, 0, 0, 2263, 2264, 5, 67, 0, 0, 2264, 2265, 
	    5, 75, 0, 0, 2265, 382, 1, 0, 0, 0, 2266, 2267, 5, 76, 0, 0, 2267, 
	    2268, 5, 79, 0, 0, 2268, 2269, 5, 71, 0, 0, 2269, 2270, 5, 73, 0, 0, 
	    2270, 2271, 5, 67, 0, 0, 2271, 2272, 5, 65, 0, 0, 2272, 2273, 5, 76, 
	    0, 0, 2273, 384, 1, 0, 0, 0, 2274, 2275, 5, 76, 0, 0, 2275, 2276, 5, 
	    79, 0, 0, 2276, 2277, 5, 79, 0, 0, 2277, 2278, 5, 80, 0, 0, 2278, 386, 
	    1, 0, 0, 0, 2279, 2280, 5, 77, 0, 0, 2280, 2281, 5, 65, 0, 0, 2281, 
	    2282, 5, 80, 0, 0, 2282, 388, 1, 0, 0, 0, 2283, 2284, 5, 77, 0, 0, 
	    2284, 2285, 5, 65, 0, 0, 2285, 2286, 5, 83, 0, 0, 2286, 2287, 5, 75, 
	    0, 0, 2287, 2288, 5, 73, 0, 0, 2288, 2289, 5, 78, 0, 0, 2289, 2290, 
	    5, 71, 0, 0, 2290, 390, 1, 0, 0, 0, 2291, 2292, 5, 77, 0, 0, 2292, 
	    2293, 5, 65, 0, 0, 2293, 2294, 5, 84, 0, 0, 2294, 2295, 5, 67, 0, 0, 
	    2295, 2296, 5, 72, 0, 0, 2296, 392, 1, 0, 0, 0, 2297, 2298, 5, 77, 
	    0, 0, 2298, 2299, 5, 65, 0, 0, 2299, 2300, 5, 84, 0, 0, 2300, 2301, 
	    5, 67, 0, 0, 2301, 2302, 5, 72, 0, 0, 2302, 2303, 5, 69, 0, 0, 2303, 
	    2304, 5, 68, 0, 0, 2304, 394, 1, 0, 0, 0, 2305, 2306, 5, 77, 0, 0, 
	    2306, 2307, 5, 65, 0, 0, 2307, 2308, 5, 84, 0, 0, 2308, 2309, 5, 67, 
	    0, 0, 2309, 2310, 5, 72, 0, 0, 2310, 2311, 5, 69, 0, 0, 2311, 2312, 
	    5, 83, 0, 0, 2312, 396, 1, 0, 0, 0, 2313, 2314, 5, 77, 0, 0, 2314, 
	    2315, 5, 65, 0, 0, 2315, 2316, 5, 84, 0, 0, 2316, 2317, 5, 67, 0, 0, 
	    2317, 2318, 5, 72, 0, 0, 2318, 2319, 5, 95, 0, 0, 2319, 2320, 5, 82, 
	    0, 0, 2320, 2321, 5, 69, 0, 0, 2321, 2322, 5, 67, 0, 0, 2322, 2323, 
	    5, 79, 0, 0, 2323, 2324, 5, 71, 0, 0, 2324, 2325, 5, 78, 0, 0, 2325, 
	    2326, 5, 73, 0, 0, 2326, 2327, 5, 90, 0, 0, 2327, 2328, 5, 69, 0, 0, 
	    2328, 398, 1, 0, 0, 0, 2329, 2330, 5, 77, 0, 0, 2330, 2331, 5, 65, 
	    0, 0, 2331, 2332, 5, 84, 0, 0, 2332, 2333, 5, 69, 0, 0, 2333, 2334, 
	    5, 82, 0, 0, 2334, 2335, 5, 73, 0, 0, 2335, 2336, 5, 65, 0, 0, 2336, 
	    2337, 5, 76, 0, 0, 2337, 2338, 5, 73, 0, 0, 2338, 2339, 5, 90, 0, 0, 
	    2339, 2340, 5, 69, 0, 0, 2340, 2341, 5, 68, 0, 0, 2341, 400, 1, 0, 
	    0, 0, 2342, 2343, 5, 77, 0, 0, 2343, 2344, 5, 65, 0, 0, 2344, 2345, 
	    5, 88, 0, 0, 2345, 402, 1, 0, 0, 0, 2346, 2347, 5, 77, 0, 0, 2347, 
	    2348, 5, 69, 0, 0, 2348, 2349, 5, 65, 0, 0, 2349, 2350, 5, 83, 0, 0, 
	    2350, 2351, 5, 85, 0, 0, 2351, 2352, 5, 82, 0, 0, 2352, 2353, 5, 69, 
	    0, 0, 2353, 2354, 5, 83, 0, 0, 2354, 404, 1, 0, 0, 0, 2355, 2356, 5, 
	    77, 0, 0, 2356, 2357, 5, 69, 0, 0, 2357, 2358, 5, 82, 0, 0, 2358, 2359, 
	    5, 71, 0, 0, 2359, 2360, 5, 69, 0, 0, 2360, 406, 1, 0, 0, 0, 2361, 
	    2362, 5, 77, 0, 0, 2362, 2363, 5, 69, 0, 0, 2363, 2364, 5, 83, 0, 0, 
	    2364, 2365, 5, 83, 0, 0, 2365, 2366, 5, 65, 0, 0, 2366, 2367, 5, 71, 
	    0, 0, 2367, 2368, 5, 69, 0, 0, 2368, 408, 1, 0, 0, 0, 2369, 2370, 5, 
	    77, 0, 0, 2370, 2371, 5, 73, 0, 0, 2371, 2372, 5, 67, 0, 0, 2372, 2373, 
	    5, 82, 0, 0, 2373, 2374, 5, 79, 0, 0, 2374, 2375, 5, 83, 0, 0, 2375, 
	    2376, 5, 69, 0, 0, 2376, 2377, 5, 67, 0, 0, 2377, 2378, 5, 79, 0, 0, 
	    2378, 2379, 5, 78, 0, 0, 2379, 2380, 5, 68, 0, 0, 2380, 410, 1, 0, 
	    0, 0, 2381, 2382, 5, 77, 0, 0, 2382, 2383, 5, 73, 0, 0, 2383, 2384, 
	    5, 76, 0, 0, 2384, 2385, 5, 76, 0, 0, 2385, 2386, 5, 73, 0, 0, 2386, 
	    2387, 5, 83, 0, 0, 2387, 2388, 5, 69, 0, 0, 2388, 2389, 5, 67, 0, 0, 
	    2389, 2390, 5, 79, 0, 0, 2390, 2391, 5, 78, 0, 0, 2391, 2392, 5, 68, 
	    0, 0, 2392, 412, 1, 0, 0, 0, 2393, 2394, 5, 77, 0, 0, 2394, 2395, 5, 
	    73, 0, 0, 2395, 2396, 5, 78, 0, 0, 2396, 414, 1, 0, 0, 0, 2397, 2398, 
	    5, 77, 0, 0, 2398, 2399, 5, 73, 0, 0, 2399, 2400, 5, 78, 0, 0, 2400, 
	    2401, 5, 85, 0, 0, 2401, 2402, 5, 83, 0, 0, 2402, 416, 1, 0, 0, 0, 
	    2403, 2404, 5, 77, 0, 0, 2404, 2405, 5, 73, 0, 0, 2405, 2406, 5, 78, 
	    0, 0, 2406, 2407, 5, 85, 0, 0, 2407, 2408, 5, 84, 0, 0, 2408, 2409, 
	    5, 69, 0, 0, 2409, 418, 1, 0, 0, 0, 2410, 2411, 5, 77, 0, 0, 2411, 
	    2412, 5, 79, 0, 0, 2412, 2413, 5, 68, 0, 0, 2413, 2414, 5, 69, 0, 0, 
	    2414, 2415, 5, 76, 0, 0, 2415, 420, 1, 0, 0, 0, 2416, 2417, 5, 77, 
	    0, 0, 2417, 2418, 5, 79, 0, 0, 2418, 2419, 5, 78, 0, 0, 2419, 2420, 
	    5, 68, 0, 0, 2420, 2421, 5, 65, 0, 0, 2421, 2422, 5, 89, 0, 0, 2422, 
	    422, 1, 0, 0, 0, 2423, 2424, 5, 77, 0, 0, 2424, 2425, 5, 79, 0, 0, 
	    2425, 2426, 5, 78, 0, 0, 2426, 2427, 5, 84, 0, 0, 2427, 2428, 5, 72, 
	    0, 0, 2428, 424, 1, 0, 0, 0, 2429, 2430, 5, 78, 0, 0, 2430, 2431, 5, 
	    65, 0, 0, 2431, 2432, 5, 77, 0, 0, 2432, 2433, 5, 69, 0, 0, 2433, 426, 
	    1, 0, 0, 0, 2434, 2435, 5, 78, 0, 0, 2435, 2436, 5, 65, 0, 0, 2436, 
	    2437, 5, 84, 0, 0, 2437, 2438, 5, 85, 0, 0, 2438, 2439, 5, 82, 0, 0, 
	    2439, 2440, 5, 65, 0, 0, 2440, 2441, 5, 76, 0, 0, 2441, 428, 1, 0, 
	    0, 0, 2442, 2443, 5, 78, 0, 0, 2443, 2444, 5, 69, 0, 0, 2444, 2445, 
	    5, 88, 0, 0, 2445, 2446, 5, 84, 0, 0, 2446, 430, 1, 0, 0, 0, 2447, 
	    2448, 5, 78, 0, 0, 2448, 2449, 5, 70, 0, 0, 2449, 2450, 5, 67, 0, 0, 
	    2450, 432, 1, 0, 0, 0, 2451, 2452, 5, 78, 0, 0, 2452, 2453, 5, 70, 
	    0, 0, 2453, 2454, 5, 68, 0, 0, 2454, 434, 1, 0, 0, 0, 2455, 2456, 5, 
	    78, 0, 0, 2456, 2457, 5, 70, 0, 0, 2457, 2458, 5, 75, 0, 0, 2458, 2459, 
	    5, 67, 0, 0, 2459, 436, 1, 0, 0, 0, 2460, 2461, 5, 78, 0, 0, 2461, 
	    2462, 5, 70, 0, 0, 2462, 2463, 5, 75, 0, 0, 2463, 2464, 5, 68, 0, 0, 
	    2464, 438, 1, 0, 0, 0, 2465, 2466, 5, 78, 0, 0, 2466, 2467, 5, 79, 
	    0, 0, 2467, 440, 1, 0, 0, 0, 2468, 2469, 5, 78, 0, 0, 2469, 2470, 5, 
	    79, 0, 0, 2470, 2471, 5, 78, 0, 0, 2471, 2472, 5, 69, 0, 0, 2472, 442, 
	    1, 0, 0, 0, 2473, 2474, 5, 78, 0, 0, 2474, 2475, 5, 79, 0, 0, 2475, 
	    2476, 5, 82, 0, 0, 2476, 2477, 5, 77, 0, 0, 2477, 2478, 5, 65, 0, 0, 
	    2478, 2479, 5, 76, 0, 0, 2479, 2480, 5, 73, 0, 0, 2480, 2481, 5, 90, 
	    0, 0, 2481, 2482, 5, 69, 0, 0, 2482, 444, 1, 0, 0, 0, 2483, 2484, 5, 
	    78, 0, 0, 2484, 2485, 5, 79, 0, 0, 2485, 2486, 5, 84, 0, 0, 2486, 446, 
	    1, 0, 0, 0, 2487, 2488, 5, 78, 0, 0, 2488, 2489, 5, 85, 0, 0, 2489, 
	    2490, 5, 76, 0, 0, 2490, 2491, 5, 76, 0, 0, 2491, 448, 1, 0, 0, 0, 
	    2492, 2493, 5, 78, 0, 0, 2493, 2494, 5, 85, 0, 0, 2494, 2495, 5, 76, 
	    0, 0, 2495, 2496, 5, 76, 0, 0, 2496, 2497, 5, 83, 0, 0, 2497, 450, 
	    1, 0, 0, 0, 2498, 2499, 5, 79, 0, 0, 2499, 2500, 5, 66, 0, 0, 2500, 
	    2501, 5, 74, 0, 0, 2501, 2502, 5, 69, 0, 0, 2502, 2503, 5, 67, 0, 0, 
	    2503, 2504, 5, 84, 0, 0, 2504, 452, 1, 0, 0, 0, 2505, 2506, 5, 79, 
	    0, 0, 2506, 2507, 5, 70, 0, 0, 2507, 454, 1, 0, 0, 0, 2508, 2509, 5, 
	    79, 0, 0, 2509, 2510, 5, 70, 0, 0, 2510, 2511, 5, 70, 0, 0, 2511, 2512, 
	    5, 83, 0, 0, 2512, 2513, 5, 69, 0, 0, 2513, 2514, 5, 84, 0, 0, 2514, 
	    456, 1, 0, 0, 0, 2515, 2516, 5, 79, 0, 0, 2516, 2517, 5, 77, 0, 0, 
	    2517, 2518, 5, 73, 0, 0, 2518, 2519, 5, 84, 0, 0, 2519, 458, 1, 0, 
	    0, 0, 2520, 2521, 5, 79, 0, 0, 2521, 2522, 5, 78, 0, 0, 2522, 460, 
	    1, 0, 0, 0, 2523, 2524, 5, 79, 0, 0, 2524, 2525, 5, 78, 0, 0, 2525, 
	    2526, 5, 69, 0, 0, 2526, 462, 1, 0, 0, 0, 2527, 2528, 5, 79, 0, 0, 
	    2528, 2529, 5, 78, 0, 0, 2529, 2530, 5, 76, 0, 0, 2530, 2531, 5, 89, 
	    0, 0, 2531, 464, 1, 0, 0, 0, 2532, 2533, 5, 79, 0, 0, 2533, 2534, 5, 
	    80, 0, 0, 2534, 2535, 5, 84, 0, 0, 2535, 2536, 5, 73, 0, 0, 2536, 2537, 
	    5, 79, 0, 0, 2537, 2538, 5, 78, 0, 0, 2538, 466, 1, 0, 0, 0, 2539, 
	    2540, 5, 79, 0, 0, 2540, 2541, 5, 80, 0, 0, 2541, 2542, 5, 84, 0, 0, 
	    2542, 2543, 5, 73, 0, 0, 2543, 2544, 5, 79, 0, 0, 2544, 2545, 5, 78, 
	    0, 0, 2545, 2546, 5, 83, 0, 0, 2546, 468, 1, 0, 0, 0, 2547, 2548, 5, 
	    79, 0, 0, 2548, 2549, 5, 82, 0, 0, 2549, 470, 1, 0, 0, 0, 2550, 2551, 
	    5, 79, 0, 0, 2551, 2552, 5, 82, 0, 0, 2552, 2553, 5, 68, 0, 0, 2553, 
	    2554, 5, 69, 0, 0, 2554, 2555, 5, 82, 0, 0, 2555, 472, 1, 0, 0, 0, 
	    2556, 2557, 5, 79, 0, 0, 2557, 2558, 5, 85, 0, 0, 2558, 2559, 5, 84, 
	    0, 0, 2559, 2560, 5, 69, 0, 0, 2560, 2561, 5, 82, 0, 0, 2561, 474, 
	    1, 0, 0, 0, 2562, 2563, 5, 79, 0, 0, 2563, 2564, 5, 85, 0, 0, 2564, 
	    2565, 5, 84, 0, 0, 2565, 2566, 5, 80, 0, 0, 2566, 2567, 5, 85, 0, 0, 
	    2567, 2568, 5, 84, 0, 0, 2568, 476, 1, 0, 0, 0, 2569, 2570, 5, 79, 
	    0, 0, 2570, 2571, 5, 85, 0, 0, 2571, 2572, 5, 84, 0, 0, 2572, 2573, 
	    5, 80, 0, 0, 2573, 2574, 5, 85, 0, 0, 2574, 2575, 5, 84, 0, 0, 2575, 
	    2576, 5, 70, 0, 0, 2576, 2577, 5, 79, 0, 0, 2577, 2578, 5, 82, 0, 0, 
	    2578, 2579, 5, 77, 0, 0, 2579, 2580, 5, 65, 0, 0, 2580, 2581, 5, 84, 
	    0, 0, 2581, 478, 1, 0, 0, 0, 2582, 2583, 5, 79, 0, 0, 2583, 2584, 5, 
	    86, 0, 0, 2584, 2585, 5, 69, 0, 0, 2585, 2586, 5, 82, 0, 0, 2586, 480, 
	    1, 0, 0, 0, 2587, 2588, 5, 79, 0, 0, 2588, 2589, 5, 86, 0, 0, 2589, 
	    2590, 5, 69, 0, 0, 2590, 2591, 5, 82, 0, 0, 2591, 2592, 5, 70, 0, 0, 
	    2592, 2593, 5, 76, 0, 0, 2593, 2594, 5, 79, 0, 0, 2594, 2595, 5, 87, 
	    0, 0, 2595, 482, 1, 0, 0, 0, 2596, 2597, 5, 80, 0, 0, 2597, 2598, 5, 
	    65, 0, 0, 2598, 2599, 5, 82, 0, 0, 2599, 2600, 5, 84, 0, 0, 2600, 2601, 
	    5, 73, 0, 0, 2601, 2602, 5, 84, 0, 0, 2602, 2603, 5, 73, 0, 0, 2603, 
	    2604, 5, 79, 0, 0, 2604, 2605, 5, 78, 0, 0, 2605, 484, 1, 0, 0, 0, 
	    2606, 2607, 5, 80, 0, 0, 2607, 2608, 5, 65, 0, 0, 2608, 2609, 5, 82, 
	    0, 0, 2609, 2610, 5, 84, 0, 0, 2610, 2611, 5, 73, 0, 0, 2611, 2612, 
	    5, 84, 0, 0, 2612, 2613, 5, 73, 0, 0, 2613, 2614, 5, 79, 0, 0, 2614, 
	    2615, 5, 78, 0, 0, 2615, 2616, 5, 69, 0, 0, 2616, 2617, 5, 68, 0, 0, 
	    2617, 486, 1, 0, 0, 0, 2618, 2619, 5, 80, 0, 0, 2619, 2620, 5, 65, 
	    0, 0, 2620, 2621, 5, 82, 0, 0, 2621, 2622, 5, 84, 0, 0, 2622, 2623, 
	    5, 73, 0, 0, 2623, 2624, 5, 84, 0, 0, 2624, 2625, 5, 73, 0, 0, 2625, 
	    2626, 5, 79, 0, 0, 2626, 2627, 5, 78, 0, 0, 2627, 2628, 5, 83, 0, 0, 
	    2628, 488, 1, 0, 0, 0, 2629, 2630, 5, 80, 0, 0, 2630, 2631, 5, 65, 
	    0, 0, 2631, 2632, 5, 83, 0, 0, 2632, 2633, 5, 83, 0, 0, 2633, 2634, 
	    5, 73, 0, 0, 2634, 2635, 5, 78, 0, 0, 2635, 2636, 5, 71, 0, 0, 2636, 
	    490, 1, 0, 0, 0, 2637, 2638, 5, 80, 0, 0, 2638, 2639, 5, 65, 0, 0, 
	    2639, 2640, 5, 83, 0, 0, 2640, 2641, 5, 84, 0, 0, 2641, 492, 1, 0, 
	    0, 0, 2642, 2643, 5, 80, 0, 0, 2643, 2644, 5, 65, 0, 0, 2644, 2645, 
	    5, 84, 0, 0, 2645, 2646, 5, 72, 0, 0, 2646, 494, 1, 0, 0, 0, 2647, 
	    2648, 5, 80, 0, 0, 2648, 2649, 5, 65, 0, 0, 2649, 2650, 5, 84, 0, 0, 
	    2650, 2651, 5, 84, 0, 0, 2651, 2652, 5, 69, 0, 0, 2652, 2653, 5, 82, 
	    0, 0, 2653, 2654, 5, 78, 0, 0, 2654, 496, 1, 0, 0, 0, 2655, 2656, 5, 
	    80, 0, 0, 2656, 2657, 5, 69, 0, 0, 2657, 2658, 5, 82, 0, 0, 2658, 498, 
	    1, 0, 0, 0, 2659, 2660, 5, 80, 0, 0, 2660, 2661, 5, 69, 0, 0, 2661, 
	    2662, 5, 82, 0, 0, 2662, 2663, 5, 67, 0, 0, 2663, 2664, 5, 69, 0, 0, 
	    2664, 2665, 5, 78, 0, 0, 2665, 2666, 5, 84, 0, 0, 2666, 500, 1, 0, 
	    0, 0, 2667, 2668, 5, 80, 0, 0, 2668, 2669, 5, 69, 0, 0, 2669, 2670, 
	    5, 82, 0, 0, 2670, 2671, 5, 73, 0, 0, 2671, 2672, 5, 79, 0, 0, 2672, 
	    2673, 5, 68, 0, 0, 2673, 502, 1, 0, 0, 0, 2674, 2675, 5, 80, 0, 0, 
	    2675, 2676, 5, 69, 0, 0, 2676, 2677, 5, 82, 0, 0, 2677, 2678, 5, 77, 
	    0, 0, 2678, 2679, 5, 85, 0, 0, 2679, 2680, 5, 84, 0, 0, 2680, 2681, 
	    5, 69, 0, 0, 2681, 504, 1, 0, 0, 0, 2682, 2683, 5, 80, 0, 0, 2683, 
	    2684, 5, 73, 0, 0, 2684, 2685, 5, 86, 0, 0, 2685, 2686, 5, 79, 0, 0, 
	    2686, 2687, 5, 84, 0, 0, 2687, 506, 1, 0, 0, 0, 2688, 2689, 5, 80, 
	    0, 0, 2689, 2690, 5, 79, 0, 0, 2690, 2691, 5, 83, 0, 0, 2691, 2692, 
	    5, 73, 0, 0, 2692, 2693, 5, 84, 0, 0, 2693, 2694, 5, 73, 0, 0, 2694, 
	    2695, 5, 79, 0, 0, 2695, 2696, 5, 78, 0, 0, 2696, 508, 1, 0, 0, 0, 
	    2697, 2698, 5, 80, 0, 0, 2698, 2699, 5, 82, 0, 0, 2699, 2700, 5, 69, 
	    0, 0, 2700, 2701, 5, 67, 0, 0, 2701, 2702, 5, 69, 0, 0, 2702, 2703, 
	    5, 68, 0, 0, 2703, 2704, 5, 73, 0, 0, 2704, 2705, 5, 78, 0, 0, 2705, 
	    2706, 5, 71, 0, 0, 2706, 510, 1, 0, 0, 0, 2707, 2708, 5, 80, 0, 0, 
	    2708, 2709, 5, 82, 0, 0, 2709, 2710, 5, 69, 0, 0, 2710, 2711, 5, 67, 
	    0, 0, 2711, 2712, 5, 73, 0, 0, 2712, 2713, 5, 83, 0, 0, 2713, 2714, 
	    5, 73, 0, 0, 2714, 2715, 5, 79, 0, 0, 2715, 2716, 5, 78, 0, 0, 2716, 
	    512, 1, 0, 0, 0, 2717, 2718, 5, 80, 0, 0, 2718, 2719, 5, 82, 0, 0, 
	    2719, 2720, 5, 69, 0, 0, 2720, 2721, 5, 80, 0, 0, 2721, 2722, 5, 65, 
	    0, 0, 2722, 2723, 5, 82, 0, 0, 2723, 2724, 5, 69, 0, 0, 2724, 514, 
	    1, 0, 0, 0, 2725, 2726, 5, 80, 0, 0, 2726, 2727, 5, 82, 0, 0, 2727, 
	    2728, 5, 73, 0, 0, 2728, 2729, 5, 79, 0, 0, 2729, 2730, 5, 82, 0, 0, 
	    2730, 516, 1, 0, 0, 0, 2731, 2732, 5, 80, 0, 0, 2732, 2733, 5, 82, 
	    0, 0, 2733, 2734, 5, 79, 0, 0, 2734, 2735, 5, 67, 0, 0, 2735, 2736, 
	    5, 69, 0, 0, 2736, 2737, 5, 68, 0, 0, 2737, 2738, 5, 85, 0, 0, 2738, 
	    2739, 5, 82, 0, 0, 2739, 2740, 5, 69, 0, 0, 2740, 518, 1, 0, 0, 0, 
	    2741, 2742, 5, 80, 0, 0, 2742, 2743, 5, 82, 0, 0, 2743, 2744, 5, 73, 
	    0, 0, 2744, 2745, 5, 86, 0, 0, 2745, 2746, 5, 73, 0, 0, 2746, 2747, 
	    5, 76, 0, 0, 2747, 2748, 5, 69, 0, 0, 2748, 2749, 5, 71, 0, 0, 2749, 
	    2750, 5, 69, 0, 0, 2750, 2751, 5, 83, 0, 0, 2751, 520, 1, 0, 0, 0, 
	    2752, 2753, 5, 80, 0, 0, 2753, 2754, 5, 82, 0, 0, 2754, 2755, 5, 79, 
	    0, 0, 2755, 2756, 5, 80, 0, 0, 2756, 2757, 5, 69, 0, 0, 2757, 2758, 
	    5, 82, 0, 0, 2758, 2759, 5, 84, 0, 0, 2759, 2760, 5, 73, 0, 0, 2760, 
	    2761, 5, 69, 0, 0, 2761, 2762, 5, 83, 0, 0, 2762, 522, 1, 0, 0, 0, 
	    2763, 2764, 5, 80, 0, 0, 2764, 2765, 5, 82, 0, 0, 2765, 2766, 5, 85, 
	    0, 0, 2766, 2767, 5, 78, 0, 0, 2767, 2768, 5, 69, 0, 0, 2768, 524, 
	    1, 0, 0, 0, 2769, 2770, 5, 81, 0, 0, 2770, 2771, 5, 85, 0, 0, 2771, 
	    2772, 5, 65, 0, 0, 2772, 2773, 5, 76, 0, 0, 2773, 2774, 5, 73, 0, 0, 
	    2774, 2775, 5, 70, 0, 0, 2775, 2776, 5, 89, 0, 0, 2776, 526, 1, 0, 
	    0, 0, 2777, 2778, 5, 81, 0, 0, 2778, 2779, 5, 85, 0, 0, 2779, 2780, 
	    5, 65, 0, 0, 2780, 2781, 5, 82, 0, 0, 2781, 2782, 5, 84, 0, 0, 2782, 
	    2783, 5, 69, 0, 0, 2783, 2784, 5, 82, 0, 0, 2784, 528, 1, 0, 0, 0, 
	    2785, 2786, 5, 81, 0, 0, 2786, 2787, 5, 85, 0, 0, 2787, 2788, 5, 79, 
	    0, 0, 2788, 2789, 5, 84, 0, 0, 2789, 2790, 5, 69, 0, 0, 2790, 2791, 
	    5, 83, 0, 0, 2791, 530, 1, 0, 0, 0, 2792, 2793, 5, 82, 0, 0, 2793, 
	    2794, 5, 65, 0, 0, 2794, 2795, 5, 73, 0, 0, 2795, 2796, 5, 83, 0, 0, 
	    2796, 2797, 5, 69, 0, 0, 2797, 532, 1, 0, 0, 0, 2798, 2799, 5, 82, 
	    0, 0, 2799, 2800, 5, 65, 0, 0, 2800, 2801, 5, 78, 0, 0, 2801, 2802, 
	    5, 71, 0, 0, 2802, 2803, 5, 69, 0, 0, 2803, 534, 1, 0, 0, 0, 2804, 
	    2805, 5, 82, 0, 0, 2805, 2806, 5, 69, 0, 0, 2806, 2807, 5, 65, 0, 0, 
	    2807, 2808, 5, 68, 0, 0, 2808, 536, 1, 0, 0, 0, 2809, 2810, 5, 82, 
	    0, 0, 2810, 2811, 5, 69, 0, 0, 2811, 2812, 5, 67, 0, 0, 2812, 2813, 
	    5, 85, 0, 0, 2813, 2814, 5, 82, 0, 0, 2814, 2815, 5, 83, 0, 0, 2815, 
	    2816, 5, 73, 0, 0, 2816, 2817, 5, 86, 0, 0, 2817, 2818, 5, 69, 0, 0, 
	    2818, 538, 1, 0, 0, 0, 2819, 2820, 5, 82, 0, 0, 2820, 2821, 5, 69, 
	    0, 0, 2821, 2822, 5, 70, 0, 0, 2822, 2823, 5, 82, 0, 0, 2823, 2824, 
	    5, 69, 0, 0, 2824, 2825, 5, 83, 0, 0, 2825, 2826, 5, 72, 0, 0, 2826, 
	    540, 1, 0, 0, 0, 2827, 2828, 5, 82, 0, 0, 2828, 2829, 5, 69, 0, 0, 
	    2829, 2830, 5, 78, 0, 0, 2830, 2831, 5, 65, 0, 0, 2831, 2832, 5, 77, 
	    0, 0, 2832, 2833, 5, 69, 0, 0, 2833, 542, 1, 0, 0, 0, 2834, 2835, 5, 
	    82, 0, 0, 2835, 2836, 5, 69, 0, 0, 2836, 2837, 5, 80, 0, 0, 2837, 2838, 
	    5, 69, 0, 0, 2838, 2839, 5, 65, 0, 0, 2839, 2840, 5, 84, 0, 0, 2840, 
	    2841, 5, 65, 0, 0, 2841, 2842, 5, 66, 0, 0, 2842, 2843, 5, 76, 0, 0, 
	    2843, 2844, 5, 69, 0, 0, 2844, 544, 1, 0, 0, 0, 2845, 2846, 5, 82, 
	    0, 0, 2846, 2847, 5, 69, 0, 0, 2847, 2848, 5, 80, 0, 0, 2848, 2849, 
	    5, 76, 0, 0, 2849, 2850, 5, 65, 0, 0, 2850, 2851, 5, 67, 0, 0, 2851, 
	    2852, 5, 69, 0, 0, 2852, 546, 1, 0, 0, 0, 2853, 2854, 5, 82, 0, 0, 
	    2854, 2855, 5, 69, 0, 0, 2855, 2856, 5, 83, 0, 0, 2856, 2857, 5, 69, 
	    0, 0, 2857, 2858, 5, 84, 0, 0, 2858, 548, 1, 0, 0, 0, 2859, 2860, 5, 
	    82, 0, 0, 2860, 2861, 5, 69, 0, 0, 2861, 2862, 5, 83, 0, 0, 2862, 2863, 
	    5, 80, 0, 0, 2863, 2864, 5, 69, 0, 0, 2864, 2865, 5, 67, 0, 0, 2865, 
	    2866, 5, 84, 0, 0, 2866, 550, 1, 0, 0, 0, 2867, 2868, 5, 82, 0, 0, 
	    2868, 2869, 5, 69, 0, 0, 2869, 2870, 5, 83, 0, 0, 2870, 2871, 5, 84, 
	    0, 0, 2871, 2872, 5, 82, 0, 0, 2872, 2873, 5, 73, 0, 0, 2873, 2874, 
	    5, 67, 0, 0, 2874, 2875, 5, 84, 0, 0, 2875, 552, 1, 0, 0, 0, 2876, 
	    2877, 5, 82, 0, 0, 2877, 2878, 5, 69, 0, 0, 2878, 2879, 5, 84, 0, 0, 
	    2879, 2880, 5, 85, 0, 0, 2880, 2881, 5, 82, 0, 0, 2881, 2882, 5, 78, 
	    0, 0, 2882, 554, 1, 0, 0, 0, 2883, 2884, 5, 82, 0, 0, 2884, 2885, 5, 
	    69, 0, 0, 2885, 2886, 5, 84, 0, 0, 2886, 2887, 5, 85, 0, 0, 2887, 2888, 
	    5, 82, 0, 0, 2888, 2889, 5, 78, 0, 0, 2889, 2890, 5, 73, 0, 0, 2890, 
	    2891, 5, 78, 0, 0, 2891, 2892, 5, 71, 0, 0, 2892, 556, 1, 0, 0, 0, 
	    2893, 2894, 5, 82, 0, 0, 2894, 2895, 5, 69, 0, 0, 2895, 2896, 5, 77, 
	    0, 0, 2896, 2897, 5, 79, 0, 0, 2897, 2898, 5, 84, 0, 0, 2898, 2899, 
	    5, 69, 0, 0, 2899, 558, 1, 0, 0, 0, 2900, 2901, 5, 82, 0, 0, 2901, 
	    2902, 5, 69, 0, 0, 2902, 2903, 5, 80, 0, 0, 2903, 2904, 5, 69, 0, 0, 
	    2904, 2905, 5, 65, 0, 0, 2905, 2906, 5, 84, 0, 0, 2906, 560, 1, 0, 
	    0, 0, 2907, 2908, 5, 82, 0, 0, 2908, 2909, 5, 69, 0, 0, 2909, 2910, 
	    5, 84, 0, 0, 2910, 2911, 5, 85, 0, 0, 2911, 2912, 5, 82, 0, 0, 2912, 
	    2913, 5, 78, 0, 0, 2913, 2914, 5, 83, 0, 0, 2914, 562, 1, 0, 0, 0, 
	    2915, 2916, 5, 82, 0, 0, 2916, 2917, 5, 69, 0, 0, 2917, 2918, 5, 86, 
	    0, 0, 2918, 2919, 5, 79, 0, 0, 2919, 2920, 5, 75, 0, 0, 2920, 2921, 
	    5, 69, 0, 0, 2921, 564, 1, 0, 0, 0, 2922, 2923, 5, 82, 0, 0, 2923, 
	    2924, 5, 73, 0, 0, 2924, 2925, 5, 71, 0, 0, 2925, 2926, 5, 72, 0, 0, 
	    2926, 2927, 5, 84, 0, 0, 2927, 566, 1, 0, 0, 0, 2928, 2929, 5, 82, 
	    0, 0, 2929, 2930, 5, 76, 0, 0, 2930, 2931, 5, 83, 0, 0, 2931, 568, 
	    1, 0, 0, 0, 2932, 2933, 5, 82, 0, 0, 2933, 2934, 5, 79, 0, 0, 2934, 
	    2935, 5, 76, 0, 0, 2935, 2936, 5, 69, 0, 0, 2936, 570, 1, 0, 0, 0, 
	    2937, 2938, 5, 82, 0, 0, 2938, 2939, 5, 79, 0, 0, 2939, 2940, 5, 76, 
	    0, 0, 2940, 2941, 5, 69, 0, 0, 2941, 2942, 5, 83, 0, 0, 2942, 572, 
	    1, 0, 0, 0, 2943, 2944, 5, 82, 0, 0, 2944, 2945, 5, 79, 0, 0, 2945, 
	    2946, 5, 76, 0, 0, 2946, 2947, 5, 76, 0, 0, 2947, 2948, 5, 66, 0, 0, 
	    2948, 2949, 5, 65, 0, 0, 2949, 2950, 5, 67, 0, 0, 2950, 2951, 5, 75, 
	    0, 0, 2951, 574, 1, 0, 0, 0, 2952, 2953, 5, 82, 0, 0, 2953, 2954, 5, 
	    79, 0, 0, 2954, 2955, 5, 76, 0, 0, 2955, 2956, 5, 76, 0, 0, 2956, 2957, 
	    5, 85, 0, 0, 2957, 2958, 5, 80, 0, 0, 2958, 576, 1, 0, 0, 0, 2959, 
	    2960, 5, 82, 0, 0, 2960, 2961, 5, 79, 0, 0, 2961, 2962, 5, 87, 0, 0, 
	    2962, 578, 1, 0, 0, 0, 2963, 2964, 5, 82, 0, 0, 2964, 2965, 5, 79, 
	    0, 0, 2965, 2966, 5, 87, 0, 0, 2966, 2967, 5, 83, 0, 0, 2967, 580, 
	    1, 0, 0, 0, 2968, 2969, 5, 82, 0, 0, 2969, 2970, 5, 85, 0, 0, 2970, 
	    2971, 5, 78, 0, 0, 2971, 2972, 5, 78, 0, 0, 2972, 2973, 5, 73, 0, 0, 
	    2973, 2974, 5, 78, 0, 0, 2974, 2975, 5, 71, 0, 0, 2975, 582, 1, 0, 
	    0, 0, 2976, 2977, 5, 83, 0, 0, 2977, 2978, 5, 65, 0, 0, 2978, 2979, 
	    5, 70, 0, 0, 2979, 2980, 5, 69, 0, 0, 2980, 584, 1, 0, 0, 0, 2981, 
	    2982, 5, 83, 0, 0, 2982, 2983, 5, 65, 0, 0, 2983, 2984, 5, 70, 0, 0, 
	    2984, 2985, 5, 69, 0, 0, 2985, 2986, 5, 95, 0, 0, 2986, 2987, 5, 67, 
	    0, 0, 2987, 2988, 5, 65, 0, 0, 2988, 2989, 5, 83, 0, 0, 2989, 2990, 
	    5, 84, 0, 0, 2990, 586, 1, 0, 0, 0, 2991, 2992, 5, 83, 0, 0, 2992, 
	    2993, 5, 65, 0, 0, 2993, 2994, 5, 84, 0, 0, 2994, 2995, 5, 85, 0, 0, 
	    2995, 2996, 5, 82, 0, 0, 2996, 2997, 5, 68, 0, 0, 2997, 2998, 5, 65, 
	    0, 0, 2998, 2999, 5, 89, 0, 0, 2999, 588, 1, 0, 0, 0, 3000, 3001, 5, 
	    83, 0, 0, 3001, 3002, 5, 67, 0, 0, 3002, 3003, 5, 65, 0, 0, 3003, 3004, 
	    5, 76, 0, 0, 3004, 3005, 5, 65, 0, 0, 3005, 3006, 5, 82, 0, 0, 3006, 
	    590, 1, 0, 0, 0, 3007, 3008, 5, 83, 0, 0, 3008, 3009, 5, 69, 0, 0, 
	    3009, 3010, 5, 67, 0, 0, 3010, 3011, 5, 79, 0, 0, 3011, 3012, 5, 78, 
	    0, 0, 3012, 3013, 5, 68, 0, 0, 3013, 592, 1, 0, 0, 0, 3014, 3015, 5, 
	    83, 0, 0, 3015, 3016, 5, 67, 0, 0, 3016, 3017, 5, 72, 0, 0, 3017, 3018, 
	    5, 69, 0, 0, 3018, 3019, 5, 77, 0, 0, 3019, 3020, 5, 65, 0, 0, 3020, 
	    594, 1, 0, 0, 0, 3021, 3022, 5, 83, 0, 0, 3022, 3023, 5, 67, 0, 0, 
	    3023, 3024, 5, 72, 0, 0, 3024, 3025, 5, 69, 0, 0, 3025, 3026, 5, 77, 
	    0, 0, 3026, 3027, 5, 65, 0, 0, 3027, 3028, 5, 83, 0, 0, 3028, 596, 
	    1, 0, 0, 0, 3029, 3030, 5, 83, 0, 0, 3030, 3031, 5, 69, 0, 0, 3031, 
	    3032, 5, 67, 0, 0, 3032, 3033, 5, 85, 0, 0, 3033, 3034, 5, 82, 0, 0, 
	    3034, 3035, 5, 73, 0, 0, 3035, 3036, 5, 84, 0, 0, 3036, 3037, 5, 89, 
	    0, 0, 3037, 598, 1, 0, 0, 0, 3038, 3039, 5, 83, 0, 0, 3039, 3040, 5, 
	    69, 0, 0, 3040, 3041, 5, 69, 0, 0, 3041, 3042, 5, 75, 0, 0, 3042, 600, 
	    1, 0, 0, 0, 3043, 3044, 5, 83, 0, 0, 3044, 3045, 5, 69, 0, 0, 3045, 
	    3046, 5, 76, 0, 0, 3046, 3047, 5, 69, 0, 0, 3047, 3048, 5, 67, 0, 0, 
	    3048, 3049, 5, 84, 0, 0, 3049, 602, 1, 0, 0, 0, 3050, 3051, 5, 83, 
	    0, 0, 3051, 3052, 5, 69, 0, 0, 3052, 3053, 5, 77, 0, 0, 3053, 3054, 
	    5, 73, 0, 0, 3054, 604, 1, 0, 0, 0, 3055, 3056, 5, 83, 0, 0, 3056, 
	    3057, 5, 69, 0, 0, 3057, 3058, 5, 82, 0, 0, 3058, 3059, 5, 68, 0, 0, 
	    3059, 3060, 5, 69, 0, 0, 3060, 606, 1, 0, 0, 0, 3061, 3062, 5, 83, 
	    0, 0, 3062, 3063, 5, 69, 0, 0, 3063, 3064, 5, 82, 0, 0, 3064, 3065, 
	    5, 68, 0, 0, 3065, 3066, 5, 69, 0, 0, 3066, 3067, 5, 80, 0, 0, 3067, 
	    3068, 5, 82, 0, 0, 3068, 3069, 5, 79, 0, 0, 3069, 3070, 5, 80, 0, 0, 
	    3070, 3071, 5, 69, 0, 0, 3071, 3072, 5, 82, 0, 0, 3072, 3073, 5, 84, 
	    0, 0, 3073, 3074, 5, 73, 0, 0, 3074, 3075, 5, 69, 0, 0, 3075, 3076, 
	    5, 83, 0, 0, 3076, 608, 1, 0, 0, 0, 3077, 3078, 5, 83, 0, 0, 3078, 
	    3079, 5, 69, 0, 0, 3079, 3080, 5, 82, 0, 0, 3080, 3081, 5, 73, 0, 0, 
	    3081, 3082, 5, 65, 0, 0, 3082, 3083, 5, 76, 0, 0, 3083, 3084, 5, 73, 
	    0, 0, 3084, 3085, 5, 90, 0, 0, 3085, 3086, 5, 65, 0, 0, 3086, 3087, 
	    5, 66, 0, 0, 3087, 3088, 5, 76, 0, 0, 3088, 3089, 5, 69, 0, 0, 3089, 
	    610, 1, 0, 0, 0, 3090, 3091, 5, 83, 0, 0, 3091, 3092, 5, 69, 0, 0, 
	    3092, 3093, 5, 83, 0, 0, 3093, 3094, 5, 83, 0, 0, 3094, 3095, 5, 73, 
	    0, 0, 3095, 3096, 5, 79, 0, 0, 3096, 3097, 5, 78, 0, 0, 3097, 612, 
	    1, 0, 0, 0, 3098, 3099, 5, 83, 0, 0, 3099, 3100, 5, 69, 0, 0, 3100, 
	    3101, 5, 84, 0, 0, 3101, 614, 1, 0, 0, 0, 3102, 3103, 5, 83, 0, 0, 
	    3103, 3104, 5, 69, 0, 0, 3104, 3105, 5, 84, 0, 0, 3105, 3106, 5, 83, 
	    0, 0, 3106, 616, 1, 0, 0, 0, 3107, 3108, 5, 83, 0, 0, 3108, 3109, 5, 
	    72, 0, 0, 3109, 3110, 5, 79, 0, 0, 3110, 3111, 5, 87, 0, 0, 3111, 618, 
	    1, 0, 0, 0, 3112, 3113, 5, 83, 0, 0, 3113, 3114, 5, 73, 0, 0, 3114, 
	    3115, 5, 77, 0, 0, 3115, 3116, 5, 73, 0, 0, 3116, 3117, 5, 76, 0, 0, 
	    3117, 3118, 5, 65, 0, 0, 3118, 3119, 5, 82, 0, 0, 3119, 620, 1, 0, 
	    0, 0, 3120, 3121, 5, 83, 0, 0, 3121, 3122, 5, 78, 0, 0, 3122, 3123, 
	    5, 65, 0, 0, 3123, 3124, 5, 80, 0, 0, 3124, 3125, 5, 83, 0, 0, 3125, 
	    3126, 5, 72, 0, 0, 3126, 3127, 5, 79, 0, 0, 3127, 3128, 5, 84, 0, 0, 
	    3128, 622, 1, 0, 0, 0, 3129, 3130, 5, 83, 0, 0, 3130, 3131, 5, 79, 
	    0, 0, 3131, 3132, 5, 77, 0, 0, 3132, 3133, 5, 69, 0, 0, 3133, 624, 
	    1, 0, 0, 0, 3134, 3135, 5, 83, 0, 0, 3135, 3136, 5, 79, 0, 0, 3136, 
	    3137, 5, 82, 0, 0, 3137, 3138, 5, 84, 0, 0, 3138, 3139, 5, 75, 0, 0, 
	    3139, 3140, 5, 69, 0, 0, 3140, 3141, 5, 89, 0, 0, 3141, 626, 1, 0, 
	    0, 0, 3142, 3143, 5, 83, 0, 0, 3143, 3144, 5, 84, 0, 0, 3144, 3145, 
	    5, 65, 0, 0, 3145, 3146, 5, 82, 0, 0, 3146, 3147, 5, 84, 0, 0, 3147, 
	    628, 1, 0, 0, 0, 3148, 3149, 5, 83, 0, 0, 3149, 3150, 5, 84, 0, 0, 
	    3150, 3151, 5, 65, 0, 0, 3151, 3152, 5, 84, 0, 0, 3152, 3153, 5, 83, 
	    0, 0, 3153, 630, 1, 0, 0, 0, 3154, 3155, 5, 83, 0, 0, 3155, 3156, 5, 
	    84, 0, 0, 3156, 3157, 5, 79, 0, 0, 3157, 3158, 5, 82, 0, 0, 3158, 3159, 
	    5, 69, 0, 0, 3159, 3160, 5, 68, 0, 0, 3160, 632, 1, 0, 0, 0, 3161, 
	    3162, 5, 83, 0, 0, 3162, 3163, 5, 84, 0, 0, 3163, 3164, 5, 82, 0, 0, 
	    3164, 3165, 5, 85, 0, 0, 3165, 3166, 5, 67, 0, 0, 3166, 3167, 5, 84, 
	    0, 0, 3167, 634, 1, 0, 0, 0, 3168, 3169, 5, 83, 0, 0, 3169, 3170, 5, 
	    85, 0, 0, 3170, 3171, 5, 66, 0, 0, 3171, 3172, 5, 83, 0, 0, 3172, 3173, 
	    5, 69, 0, 0, 3173, 3174, 5, 84, 0, 0, 3174, 636, 1, 0, 0, 0, 3175, 
	    3176, 5, 83, 0, 0, 3176, 3177, 5, 85, 0, 0, 3177, 3178, 5, 66, 0, 0, 
	    3178, 3179, 5, 83, 0, 0, 3179, 3180, 5, 84, 0, 0, 3180, 3181, 5, 82, 
	    0, 0, 3181, 3182, 5, 73, 0, 0, 3182, 3183, 5, 78, 0, 0, 3183, 3184, 
	    5, 71, 0, 0, 3184, 638, 1, 0, 0, 0, 3185, 3186, 5, 83, 0, 0, 3186, 
	    3187, 5, 85, 0, 0, 3187, 3188, 5, 78, 0, 0, 3188, 3189, 5, 68, 0, 0, 
	    3189, 3190, 5, 65, 0, 0, 3190, 3191, 5, 89, 0, 0, 3191, 640, 1, 0, 
	    0, 0, 3192, 3193, 5, 83, 0, 0, 3193, 3194, 5, 89, 0, 0, 3194, 3195, 
	    5, 83, 0, 0, 3195, 3196, 5, 84, 0, 0, 3196, 3197, 5, 69, 0, 0, 3197, 
	    3198, 5, 77, 0, 0, 3198, 642, 1, 0, 0, 0, 3199, 3200, 5, 83, 0, 0, 
	    3200, 3201, 5, 89, 0, 0, 3201, 3202, 5, 83, 0, 0, 3202, 3203, 5, 84, 
	    0, 0, 3203, 3204, 5, 69, 0, 0, 3204, 3205, 5, 77, 0, 0, 3205, 3206, 
	    5, 95, 0, 0, 3206, 3207, 5, 84, 0, 0, 3207, 3208, 5, 73, 0, 0, 3208, 
	    3209, 5, 77, 0, 0, 3209, 3210, 5, 69, 0, 0, 3210, 644, 1, 0, 0, 0, 
	    3211, 3212, 5, 84, 0, 0, 3212, 3213, 5, 65, 0, 0, 3213, 3214, 5, 66, 
	    0, 0, 3214, 3215, 5, 76, 0, 0, 3215, 3216, 5, 69, 0, 0, 3216, 646, 
	    1, 0, 0, 0, 3217, 3218, 5, 84, 0, 0, 3218, 3219, 5, 65, 0, 0, 3219, 
	    3220, 5, 66, 0, 0, 3220, 3221, 5, 76, 0, 0, 3221, 3222, 5, 69, 0, 0, 
	    3222, 3223, 5, 83, 0, 0, 3223, 648, 1, 0, 0, 0, 3224, 3225, 5, 84, 
	    0, 0, 3225, 3226, 5, 65, 0, 0, 3226, 3227, 5, 66, 0, 0, 3227, 3228, 
	    5, 76, 0, 0, 3228, 3229, 5, 69, 0, 0, 3229, 3230, 5, 83, 0, 0, 3230, 
	    3231, 5, 65, 0, 0, 3231, 3232, 5, 77, 0, 0, 3232, 3233, 5, 80, 0, 0, 
	    3233, 3234, 5, 76, 0, 0, 3234, 3235, 5, 69, 0, 0, 3235, 650, 1, 0, 
	    0, 0, 3236, 3237, 5, 84, 0, 0, 3237, 3238, 5, 69, 0, 0, 3238, 3239, 
	    5, 77, 0, 0, 3239, 3240, 5, 80, 0, 0, 3240, 652, 1, 0, 0, 0, 3241, 
	    3242, 5, 84, 0, 0, 3242, 3243, 5, 69, 0, 0, 3243, 3244, 5, 77, 0, 0, 
	    3244, 3245, 5, 80, 0, 0, 3245, 3246, 5, 79, 0, 0, 3246, 3247, 5, 82, 
	    0, 0, 3247, 3248, 5, 65, 0, 0, 3248, 3249, 5, 82, 0, 0, 3249, 3250, 
	    5, 89, 0, 0, 3250, 654, 1, 0, 0, 0, 3251, 3252, 5, 84, 0, 0, 3252, 
	    3253, 5, 69, 0, 0, 3253, 3254, 5, 82, 0, 0, 3254, 3255, 5, 77, 0, 0, 
	    3255, 3256, 5, 73, 0, 0, 3256, 3257, 5, 78, 0, 0, 3257, 3258, 5, 65, 
	    0, 0, 3258, 3259, 5, 84, 0, 0, 3259, 3260, 5, 69, 0, 0, 3260, 3261, 
	    5, 68, 0, 0, 3261, 656, 1, 0, 0, 0, 3262, 3263, 5, 84, 0, 0, 3263, 
	    3264, 5, 69, 0, 0, 3264, 3265, 5, 88, 0, 0, 3265, 3266, 5, 84, 0, 0, 
	    3266, 658, 1, 0, 0, 0, 3267, 3268, 5, 83, 0, 0, 3268, 3269, 5, 84, 
	    0, 0, 3269, 3270, 5, 82, 0, 0, 3270, 3271, 5, 73, 0, 0, 3271, 3272, 
	    5, 78, 0, 0, 3272, 3273, 5, 71, 0, 0, 3273, 660, 1, 0, 0, 0, 3274, 
	    3275, 5, 84, 0, 0, 3275, 3276, 5, 72, 0, 0, 3276, 3277, 5, 69, 0, 0, 
	    3277, 3278, 5, 78, 0, 0, 3278, 662, 1, 0, 0, 0, 3279, 3280, 5, 84, 
	    0, 0, 3280, 3281, 5, 72, 0, 0, 3281, 3282, 5, 85, 0, 0, 3282, 3283, 
	    5, 82, 0, 0, 3283, 3284, 5, 83, 0, 0, 3284, 3285, 5, 68, 0, 0, 3285, 
	    3286, 5, 65, 0, 0, 3286, 3287, 5, 89, 0, 0, 3287, 664, 1, 0, 0, 0, 
	    3288, 3289, 5, 84, 0, 0, 3289, 3290, 5, 73, 0, 0, 3290, 3291, 5, 69, 
	    0, 0, 3291, 3292, 5, 83, 0, 0, 3292, 666, 1, 0, 0, 0, 3293, 3294, 5, 
	    84, 0, 0, 3294, 3295, 5, 73, 0, 0, 3295, 3296, 5, 77, 0, 0, 3296, 3297, 
	    5, 69, 0, 0, 3297, 668, 1, 0, 0, 0, 3298, 3299, 5, 84, 0, 0, 3299, 
	    3300, 5, 73, 0, 0, 3300, 3301, 5, 77, 0, 0, 3301, 3302, 5, 69, 0, 0, 
	    3302, 3303, 5, 83, 0, 0, 3303, 3304, 5, 84, 0, 0, 3304, 3305, 5, 65, 
	    0, 0, 3305, 3306, 5, 77, 0, 0, 3306, 3307, 5, 80, 0, 0, 3307, 670, 
	    1, 0, 0, 0, 3308, 3309, 5, 84, 0, 0, 3309, 3310, 5, 73, 0, 0, 3310, 
	    3311, 5, 77, 0, 0, 3311, 3312, 5, 69, 0, 0, 3312, 3313, 5, 83, 0, 0, 
	    3313, 3314, 5, 84, 0, 0, 3314, 3315, 5, 65, 0, 0, 3315, 3316, 5, 77, 
	    0, 0, 3316, 3317, 5, 80, 0, 0, 3317, 3318, 5, 95, 0, 0, 3318, 3319, 
	    5, 68, 0, 0, 3319, 3320, 5, 73, 0, 0, 3320, 3321, 5, 70, 0, 0, 3321, 
	    3322, 5, 70, 0, 0, 3322, 672, 1, 0, 0, 0, 3323, 3324, 5, 84, 0, 0, 
	    3324, 3325, 5, 79, 0, 0, 3325, 674, 1, 0, 0, 0, 3326, 3327, 5, 84, 
	    0, 0, 3327, 3328, 5, 79, 0, 0, 3328, 3329, 5, 80, 0, 0, 3329, 676, 
	    1, 0, 0, 0, 3330, 3331, 5, 84, 0, 0, 3331, 3332, 5, 82, 0, 0, 3332, 
	    3333, 5, 65, 0, 0, 3333, 3334, 5, 73, 0, 0, 3334, 3335, 5, 76, 0, 0, 
	    3335, 3336, 5, 73, 0, 0, 3336, 3337, 5, 78, 0, 0, 3337, 3338, 5, 71, 
	    0, 0, 3338, 678, 1, 0, 0, 0, 3339, 3340, 5, 84, 0, 0, 3340, 3341, 5, 
	    65, 0, 0, 3341, 3342, 5, 82, 0, 0, 3342, 3343, 5, 71, 0, 0, 3343, 3344, 
	    5, 69, 0, 0, 3344, 3345, 5, 84, 0, 0, 3345, 680, 1, 0, 0, 0, 3346, 
	    3347, 5, 83, 0, 0, 3347, 3348, 5, 79, 0, 0, 3348, 3349, 5, 85, 0, 0, 
	    3349, 3350, 5, 82, 0, 0, 3350, 3351, 5, 67, 0, 0, 3351, 3352, 5, 69, 
	    0, 0, 3352, 682, 1, 0, 0, 0, 3353, 3354, 5, 84, 0, 0, 3354, 3355, 5, 
	    82, 0, 0, 3355, 3356, 5, 65, 0, 0, 3356, 3357, 5, 73, 0, 0, 3357, 3358, 
	    5, 78, 0, 0, 3358, 3359, 5, 73, 0, 0, 3359, 3360, 5, 78, 0, 0, 3360, 
	    3361, 5, 71, 0, 0, 3361, 3362, 5, 95, 0, 0, 3362, 3363, 5, 68, 0, 0, 
	    3363, 3364, 5, 65, 0, 0, 3364, 3365, 5, 84, 0, 0, 3365, 3366, 5, 65, 
	    0, 0, 3366, 684, 1, 0, 0, 0, 3367, 3368, 5, 84, 0, 0, 3368, 3369, 5, 
	    82, 0, 0, 3369, 3370, 5, 65, 0, 0, 3370, 3371, 5, 78, 0, 0, 3371, 3372, 
	    5, 83, 0, 0, 3372, 3373, 5, 65, 0, 0, 3373, 3374, 5, 67, 0, 0, 3374, 
	    3375, 5, 84, 0, 0, 3375, 3376, 5, 73, 0, 0, 3376, 3377, 5, 79, 0, 0, 
	    3377, 3378, 5, 78, 0, 0, 3378, 686, 1, 0, 0, 0, 3379, 3380, 5, 84, 
	    0, 0, 3380, 3381, 5, 82, 0, 0, 3381, 3382, 5, 65, 0, 0, 3382, 3383, 
	    5, 78, 0, 0, 3383, 3384, 5, 83, 0, 0, 3384, 3385, 5, 70, 0, 0, 3385, 
	    3386, 5, 79, 0, 0, 3386, 3387, 5, 82, 0, 0, 3387, 3388, 5, 77, 0, 0, 
	    3388, 688, 1, 0, 0, 0, 3389, 3390, 5, 84, 0, 0, 3390, 3391, 5, 82, 
	    0, 0, 3391, 3392, 5, 73, 0, 0, 3392, 3393, 5, 77, 0, 0, 3393, 690, 
	    1, 0, 0, 0, 3394, 3395, 5, 84, 0, 0, 3395, 3396, 5, 82, 0, 0, 3396, 
	    3397, 5, 85, 0, 0, 3397, 3398, 5, 69, 0, 0, 3398, 692, 1, 0, 0, 0, 
	    3399, 3400, 5, 84, 0, 0, 3400, 3401, 5, 82, 0, 0, 3401, 3402, 5, 85, 
	    0, 0, 3402, 3403, 5, 78, 0, 0, 3403, 3404, 5, 67, 0, 0, 3404, 3405, 
	    5, 65, 0, 0, 3405, 3406, 5, 84, 0, 0, 3406, 3407, 5, 69, 0, 0, 3407, 
	    694, 1, 0, 0, 0, 3408, 3409, 5, 84, 0, 0, 3409, 3410, 5, 82, 0, 0, 
	    3410, 3411, 5, 89, 0, 0, 3411, 3412, 5, 95, 0, 0, 3412, 3413, 5, 67, 
	    0, 0, 3413, 3414, 5, 65, 0, 0, 3414, 3415, 5, 83, 0, 0, 3415, 3416, 
	    5, 84, 0, 0, 3416, 696, 1, 0, 0, 0, 3417, 3418, 5, 84, 0, 0, 3418, 
	    3419, 5, 85, 0, 0, 3419, 3420, 5, 80, 0, 0, 3420, 3421, 5, 76, 0, 0, 
	    3421, 3422, 5, 69, 0, 0, 3422, 698, 1, 0, 0, 0, 3423, 3424, 5, 84, 
	    0, 0, 3424, 3425, 5, 85, 0, 0, 3425, 3426, 5, 69, 0, 0, 3426, 3427, 
	    5, 83, 0, 0, 3427, 3428, 5, 68, 0, 0, 3428, 3429, 5, 65, 0, 0, 3429, 
	    3430, 5, 89, 0, 0, 3430, 700, 1, 0, 0, 0, 3431, 3432, 5, 84, 0, 0, 
	    3432, 3433, 5, 89, 0, 0, 3433, 3434, 5, 80, 0, 0, 3434, 3435, 5, 69, 
	    0, 0, 3435, 702, 1, 0, 0, 0, 3436, 3437, 5, 85, 0, 0, 3437, 3438, 5, 
	    69, 0, 0, 3438, 3439, 5, 83, 0, 0, 3439, 3440, 5, 67, 0, 0, 3440, 3441, 
	    5, 65, 0, 0, 3441, 3442, 5, 80, 0, 0, 3442, 3443, 5, 69, 0, 0, 3443, 
	    704, 1, 0, 0, 0, 3444, 3445, 5, 85, 0, 0, 3445, 3446, 5, 78, 0, 0, 
	    3446, 3447, 5, 66, 0, 0, 3447, 3448, 5, 79, 0, 0, 3448, 3449, 5, 85, 
	    0, 0, 3449, 3450, 5, 78, 0, 0, 3450, 3451, 5, 68, 0, 0, 3451, 3452, 
	    5, 69, 0, 0, 3452, 3453, 5, 68, 0, 0, 3453, 706, 1, 0, 0, 0, 3454, 
	    3455, 5, 85, 0, 0, 3455, 3456, 5, 78, 0, 0, 3456, 3457, 5, 67, 0, 0, 
	    3457, 3458, 5, 79, 0, 0, 3458, 3459, 5, 77, 0, 0, 3459, 3460, 5, 77, 
	    0, 0, 3460, 3461, 5, 73, 0, 0, 3461, 3462, 5, 84, 0, 0, 3462, 3463, 
	    5, 84, 0, 0, 3463, 3464, 5, 69, 0, 0, 3464, 3465, 5, 68, 0, 0, 3465, 
	    708, 1, 0, 0, 0, 3466, 3467, 5, 85, 0, 0, 3467, 3468, 5, 78, 0, 0, 
	    3468, 3469, 5, 67, 0, 0, 3469, 3470, 5, 79, 0, 0, 3470, 3471, 5, 78, 
	    0, 0, 3471, 3472, 5, 68, 0, 0, 3472, 3473, 5, 73, 0, 0, 3473, 3474, 
	    5, 84, 0, 0, 3474, 3475, 5, 73, 0, 0, 3475, 3476, 5, 79, 0, 0, 3476, 
	    3477, 5, 78, 0, 0, 3477, 3478, 5, 65, 0, 0, 3478, 3479, 5, 76, 0, 0, 
	    3479, 710, 1, 0, 0, 0, 3480, 3481, 5, 85, 0, 0, 3481, 3482, 5, 78, 
	    0, 0, 3482, 3483, 5, 73, 0, 0, 3483, 3484, 5, 79, 0, 0, 3484, 3485, 
	    5, 78, 0, 0, 3485, 712, 1, 0, 0, 0, 3486, 3487, 5, 85, 0, 0, 3487, 
	    3488, 5, 78, 0, 0, 3488, 3489, 5, 75, 0, 0, 3489, 3490, 5, 78, 0, 0, 
	    3490, 3491, 5, 79, 0, 0, 3491, 3492, 5, 87, 0, 0, 3492, 3493, 5, 78, 
	    0, 0, 3493, 714, 1, 0, 0, 0, 3494, 3495, 5, 85, 0, 0, 3495, 3496, 5, 
	    78, 0, 0, 3496, 3497, 5, 76, 0, 0, 3497, 3498, 5, 79, 0, 0, 3498, 3499, 
	    5, 65, 0, 0, 3499, 3500, 5, 68, 0, 0, 3500, 716, 1, 0, 0, 0, 3501, 
	    3502, 5, 85, 0, 0, 3502, 3503, 5, 78, 0, 0, 3503, 3504, 5, 77, 0, 0, 
	    3504, 3505, 5, 65, 0, 0, 3505, 3506, 5, 84, 0, 0, 3506, 3507, 5, 67, 
	    0, 0, 3507, 3508, 5, 72, 0, 0, 3508, 3509, 5, 69, 0, 0, 3509, 3510, 
	    5, 68, 0, 0, 3510, 718, 1, 0, 0, 0, 3511, 3512, 5, 85, 0, 0, 3512, 
	    3513, 5, 78, 0, 0, 3513, 3514, 5, 78, 0, 0, 3514, 3515, 5, 69, 0, 0, 
	    3515, 3516, 5, 83, 0, 0, 3516, 3517, 5, 84, 0, 0, 3517, 720, 1, 0, 
	    0, 0, 3518, 3519, 5, 85, 0, 0, 3519, 3520, 5, 78, 0, 0, 3520, 3521, 
	    5, 80, 0, 0, 3521, 3522, 5, 73, 0, 0, 3522, 3523, 5, 86, 0, 0, 3523, 
	    3524, 5, 79, 0, 0, 3524, 3525, 5, 84, 0, 0, 3525, 722, 1, 0, 0, 0, 
	    3526, 3527, 5, 85, 0, 0, 3527, 3528, 5, 78, 0, 0, 3528, 3529, 5, 83, 
	    0, 0, 3529, 3530, 5, 73, 0, 0, 3530, 3531, 5, 71, 0, 0, 3531, 3532, 
	    5, 78, 0, 0, 3532, 3533, 5, 69, 0, 0, 3533, 3534, 5, 68, 0, 0, 3534, 
	    724, 1, 0, 0, 0, 3535, 3536, 5, 85, 0, 0, 3536, 3537, 5, 78, 0, 0, 
	    3537, 3538, 5, 84, 0, 0, 3538, 3539, 5, 73, 0, 0, 3539, 3540, 5, 76, 
	    0, 0, 3540, 726, 1, 0, 0, 0, 3541, 3542, 5, 85, 0, 0, 3542, 3543, 5, 
	    80, 0, 0, 3543, 3544, 5, 68, 0, 0, 3544, 3545, 5, 65, 0, 0, 3545, 3546, 
	    5, 84, 0, 0, 3546, 3547, 5, 69, 0, 0, 3547, 728, 1, 0, 0, 0, 3548, 
	    3549, 5, 85, 0, 0, 3549, 3550, 5, 83, 0, 0, 3550, 3551, 5, 69, 0, 0, 
	    3551, 730, 1, 0, 0, 0, 3552, 3553, 5, 85, 0, 0, 3553, 3554, 5, 83, 
	    0, 0, 3554, 3555, 5, 69, 0, 0, 3555, 3556, 5, 82, 0, 0, 3556, 732, 
	    1, 0, 0, 0, 3557, 3558, 5, 85, 0, 0, 3558, 3559, 5, 83, 0, 0, 3559, 
	    3560, 5, 73, 0, 0, 3560, 3561, 5, 78, 0, 0, 3561, 3562, 5, 71, 0, 0, 
	    3562, 734, 1, 0, 0, 0, 3563, 3564, 5, 85, 0, 0, 3564, 3565, 5, 84, 
	    0, 0, 3565, 3566, 5, 70, 0, 0, 3566, 3567, 5, 49, 0, 0, 3567, 3568, 
	    5, 54, 0, 0, 3568, 736, 1, 0, 0, 0, 3569, 3570, 5, 85, 0, 0, 3570, 
	    3571, 5, 84, 0, 0, 3571, 3572, 5, 70, 0, 0, 3572, 3573, 5, 51, 0, 0, 
	    3573, 3574, 5, 50, 0, 0, 3574, 738, 1, 0, 0, 0, 3575, 3576, 5, 85, 
	    0, 0, 3576, 3577, 5, 84, 0, 0, 3577, 3578, 5, 70, 0, 0, 3578, 3579, 
	    5, 56, 0, 0, 3579, 740, 1, 0, 0, 0, 3580, 3581, 5, 86, 0, 0, 3581, 
	    3582, 5, 65, 0, 0, 3582, 3583, 5, 67, 0, 0, 3583, 3584, 5, 85, 0, 0, 
	    3584, 3585, 5, 85, 0, 0, 3585, 3586, 5, 77, 0, 0, 3586, 742, 1, 0, 
	    0, 0, 3587, 3588, 5, 86, 0, 0, 3588, 3589, 5, 65, 0, 0, 3589, 3590, 
	    5, 76, 0, 0, 3590, 3591, 5, 73, 0, 0, 3591, 3592, 5, 68, 0, 0, 3592, 
	    3593, 5, 65, 0, 0, 3593, 3594, 5, 84, 0, 0, 3594, 3595, 5, 69, 0, 0, 
	    3595, 744, 1, 0, 0, 0, 3596, 3597, 5, 86, 0, 0, 3597, 3598, 5, 65, 
	    0, 0, 3598, 3599, 5, 76, 0, 0, 3599, 3600, 5, 85, 0, 0, 3600, 3601, 
	    5, 69, 0, 0, 3601, 746, 1, 0, 0, 0, 3602, 3603, 5, 86, 0, 0, 3603, 
	    3604, 5, 65, 0, 0, 3604, 3605, 5, 76, 0, 0, 3605, 3606, 5, 85, 0, 0, 
	    3606, 3607, 5, 69, 0, 0, 3607, 3608, 5, 83, 0, 0, 3608, 748, 1, 0, 
	    0, 0, 3609, 3610, 5, 86, 0, 0, 3610, 3611, 5, 65, 0, 0, 3611, 3612, 
	    5, 82, 0, 0, 3612, 3613, 5, 89, 0, 0, 3613, 3614, 5, 73, 0, 0, 3614, 
	    3615, 5, 78, 0, 0, 3615, 3616, 5, 71, 0, 0, 3616, 750, 1, 0, 0, 0, 
	    3617, 3618, 5, 86, 0, 0, 3618, 3619, 5, 69, 0, 0, 3619, 3620, 5, 82, 
	    0, 0, 3620, 3621, 5, 66, 0, 0, 3621, 3622, 5, 79, 0, 0, 3622, 3623, 
	    5, 83, 0, 0, 3623, 3624, 5, 69, 0, 0, 3624, 752, 1, 0, 0, 0, 3625, 
	    3626, 5, 86, 0, 0, 3626, 3627, 5, 69, 0, 0, 3627, 3628, 5, 82, 0, 0, 
	    3628, 3629, 5, 83, 0, 0, 3629, 3630, 5, 73, 0, 0, 3630, 3631, 5, 79, 
	    0, 0, 3631, 3632, 5, 78, 0, 0, 3632, 754, 1, 0, 0, 0, 3633, 3634, 5, 
	    86, 0, 0, 3634, 3635, 5, 73, 0, 0, 3635, 3636, 5, 69, 0, 0, 3636, 3637, 
	    5, 87, 0, 0, 3637, 756, 1, 0, 0, 0, 3638, 3639, 5, 87, 0, 0, 3639, 
	    3640, 5, 69, 0, 0, 3640, 3641, 5, 68, 0, 0, 3641, 3642, 5, 78, 0, 0, 
	    3642, 3643, 5, 69, 0, 0, 3643, 3644, 5, 83, 0, 0, 3644, 3645, 5, 68, 
	    0, 0, 3645, 3646, 5, 65, 0, 0, 3646, 3647, 5, 89, 0, 0, 3647, 758, 
	    1, 0, 0, 0, 3648, 3649, 5, 87, 0, 0, 3649, 3650, 5, 69, 0, 0, 3650, 
	    3651, 5, 69, 0, 0, 3651, 3652, 5, 75, 0, 0, 3652, 760, 1, 0, 0, 0, 
	    3653, 3654, 5, 87, 0, 0, 3654, 3655, 5, 72, 0, 0, 3655, 3656, 5, 69, 
	    0, 0, 3656, 3657, 5, 78, 0, 0, 3657, 762, 1, 0, 0, 0, 3658, 3659, 5, 
	    87, 0, 0, 3659, 3660, 5, 72, 0, 0, 3660, 3661, 5, 69, 0, 0, 3661, 3662, 
	    5, 82, 0, 0, 3662, 3663, 5, 69, 0, 0, 3663, 764, 1, 0, 0, 0, 3664, 
	    3665, 5, 87, 0, 0, 3665, 3666, 5, 72, 0, 0, 3666, 3667, 5, 73, 0, 0, 
	    3667, 3668, 5, 76, 0, 0, 3668, 3669, 5, 69, 0, 0, 3669, 766, 1, 0, 
	    0, 0, 3670, 3671, 5, 87, 0, 0, 3671, 3672, 5, 73, 0, 0, 3672, 3673, 
	    5, 78, 0, 0, 3673, 3674, 5, 68, 0, 0, 3674, 3675, 5, 79, 0, 0, 3675, 
	    3676, 5, 87, 0, 0, 3676, 768, 1, 0, 0, 0, 3677, 3678, 5, 87, 0, 0, 
	    3678, 3679, 5, 73, 0, 0, 3679, 3680, 5, 84, 0, 0, 3680, 3681, 5, 72, 
	    0, 0, 3681, 770, 1, 0, 0, 0, 3682, 3683, 5, 87, 0, 0, 3683, 3684, 5, 
	    73, 0, 0, 3684, 3685, 5, 84, 0, 0, 3685, 3686, 5, 72, 0, 0, 3686, 3687, 
	    5, 79, 0, 0, 3687, 3688, 5, 85, 0, 0, 3688, 3689, 5, 84, 0, 0, 3689, 
	    772, 1, 0, 0, 0, 3690, 3691, 5, 87, 0, 0, 3691, 3692, 5, 79, 0, 0, 
	    3692, 3693, 5, 82, 0, 0, 3693, 3694, 5, 75, 0, 0, 3694, 774, 1, 0, 
	    0, 0, 3695, 3696, 5, 87, 0, 0, 3696, 3697, 5, 82, 0, 0, 3697, 3698, 
	    5, 65, 0, 0, 3698, 3699, 5, 80, 0, 0, 3699, 3700, 5, 80, 0, 0, 3700, 
	    3701, 5, 69, 0, 0, 3701, 3702, 5, 82, 0, 0, 3702, 776, 1, 0, 0, 0, 
	    3703, 3704, 5, 87, 0, 0, 3704, 3705, 5, 82, 0, 0, 3705, 3706, 5, 73, 
	    0, 0, 3706, 3707, 5, 84, 0, 0, 3707, 3708, 5, 69, 0, 0, 3708, 778, 
	    1, 0, 0, 0, 3709, 3710, 5, 88, 0, 0, 3710, 3711, 5, 90, 0, 0, 3711, 
	    780, 1, 0, 0, 0, 3712, 3713, 5, 89, 0, 0, 3713, 3714, 5, 69, 0, 0, 
	    3714, 3715, 5, 65, 0, 0, 3715, 3716, 5, 82, 0, 0, 3716, 782, 1, 0, 
	    0, 0, 3717, 3718, 5, 89, 0, 0, 3718, 3719, 5, 69, 0, 0, 3719, 3720, 
	    5, 83, 0, 0, 3720, 784, 1, 0, 0, 0, 3721, 3722, 5, 90, 0, 0, 3722, 
	    3723, 5, 79, 0, 0, 3723, 3724, 5, 78, 0, 0, 3724, 3725, 5, 69, 0, 0, 
	    3725, 786, 1, 0, 0, 0, 3726, 3727, 5, 90, 0, 0, 3727, 3728, 5, 83, 
	    0, 0, 3728, 3729, 5, 84, 0, 0, 3729, 3730, 5, 68, 0, 0, 3730, 788, 
	    1, 0, 0, 0, 3731, 3732, 5, 40, 0, 0, 3732, 790, 1, 0, 0, 0, 3733, 3734, 
	    5, 41, 0, 0, 3734, 792, 1, 0, 0, 0, 3735, 3736, 5, 91, 0, 0, 3736, 
	    794, 1, 0, 0, 0, 3737, 3738, 5, 93, 0, 0, 3738, 796, 1, 0, 0, 0, 3739, 
	    3740, 5, 46, 0, 0, 3740, 798, 1, 0, 0, 0, 3741, 3742, 5, 61, 0, 0, 
	    3742, 800, 1, 0, 0, 0, 3743, 3744, 5, 60, 0, 0, 3744, 3748, 5, 62, 
	    0, 0, 3745, 3746, 5, 33, 0, 0, 3746, 3748, 5, 61, 0, 0, 3747, 3743, 
	    1, 0, 0, 0, 3747, 3745, 1, 0, 0, 0, 3748, 802, 1, 0, 0, 0, 3749, 3750, 
	    5, 60, 0, 0, 3750, 804, 1, 0, 0, 0, 3751, 3752, 5, 60, 0, 0, 3752, 
	    3753, 5, 61, 0, 0, 3753, 806, 1, 0, 0, 0, 3754, 3755, 5, 62, 0, 0, 
	    3755, 808, 1, 0, 0, 0, 3756, 3757, 5, 62, 0, 0, 3757, 3758, 5, 61, 
	    0, 0, 3758, 810, 1, 0, 0, 0, 3759, 3760, 5, 43, 0, 0, 3760, 812, 1, 
	    0, 0, 0, 3761, 3762, 5, 45, 0, 0, 3762, 814, 1, 0, 0, 0, 3763, 3764, 
	    5, 42, 0, 0, 3764, 816, 1, 0, 0, 0, 3765, 3766, 5, 47, 0, 0, 3766, 
	    818, 1, 0, 0, 0, 3767, 3768, 5, 37, 0, 0, 3768, 820, 1, 0, 0, 0, 3769, 
	    3770, 5, 124, 0, 0, 3770, 3771, 5, 124, 0, 0, 3771, 822, 1, 0, 0, 0, 
	    3772, 3773, 5, 63, 0, 0, 3773, 824, 1, 0, 0, 0, 3774, 3775, 5, 59, 
	    0, 0, 3775, 826, 1, 0, 0, 0, 3776, 3777, 5, 58, 0, 0, 3777, 828, 1, 
	    0, 0, 0, 3778, 3779, 5, 36, 0, 0, 3779, 830, 1, 0, 0, 0, 3780, 3781, 
	    5, 38, 0, 0, 3781, 832, 1, 0, 0, 0, 3782, 3783, 5, 124, 0, 0, 3783, 
	    834, 1, 0, 0, 0, 3784, 3785, 5, 94, 0, 0, 3785, 836, 1, 0, 0, 0, 3786, 
	    3787, 5, 60, 0, 0, 3787, 3788, 5, 60, 0, 0, 3788, 838, 1, 0, 0, 0, 
	    3789, 3790, 5, 126, 0, 0, 3790, 840, 1, 0, 0, 0, 3791, 3792, 5, 92, 
	    0, 0, 3792, 3793, 9, 0, 0, 0, 3793, 842, 1, 0, 0, 0, 3794, 3799, 5, 
	    34, 0, 0, 3795, 3798, 8, 0, 0, 0, 3796, 3798, 3, 841, 420, 0, 3797, 
	    3795, 1, 0, 0, 0, 3797, 3796, 1, 0, 0, 0, 3798, 3801, 1, 0, 0, 0, 3799, 
	    3797, 1, 0, 0, 0, 3799, 3800, 1, 0, 0, 0, 3800, 3802, 1, 0, 0, 0, 3801, 
	    3799, 1, 0, 0, 0, 3802, 3813, 5, 34, 0, 0, 3803, 3808, 5, 39, 0, 0, 
	    3804, 3807, 8, 1, 0, 0, 3805, 3807, 3, 841, 420, 0, 3806, 3804, 1, 
	    0, 0, 0, 3806, 3805, 1, 0, 0, 0, 3807, 3810, 1, 0, 0, 0, 3808, 3806, 
	    1, 0, 0, 0, 3808, 3809, 1, 0, 0, 0, 3809, 3811, 1, 0, 0, 0, 3810, 3808, 
	    1, 0, 0, 0, 3811, 3813, 5, 39, 0, 0, 3812, 3794, 1, 0, 0, 0, 3812, 
	    3803, 1, 0, 0, 0, 3813, 844, 1, 0, 0, 0, 3814, 3815, 5, 34, 0, 0, 3815, 
	    3816, 5, 34, 0, 0, 3816, 3817, 5, 34, 0, 0, 3817, 3821, 1, 0, 0, 0, 
	    3818, 3820, 9, 0, 0, 0, 3819, 3818, 1, 0, 0, 0, 3820, 3823, 1, 0, 0, 
	    0, 3821, 3822, 1, 0, 0, 0, 3821, 3819, 1, 0, 0, 0, 3822, 3824, 1, 0, 
	    0, 0, 3823, 3821, 1, 0, 0, 0, 3824, 3825, 5, 34, 0, 0, 3825, 3826, 
	    5, 34, 0, 0, 3826, 3841, 5, 34, 0, 0, 3827, 3828, 5, 39, 0, 0, 3828, 
	    3829, 5, 39, 0, 0, 3829, 3830, 5, 39, 0, 0, 3830, 3834, 1, 0, 0, 0, 
	    3831, 3833, 9, 0, 0, 0, 3832, 3831, 1, 0, 0, 0, 3833, 3836, 1, 0, 0, 
	    0, 3834, 3835, 1, 0, 0, 0, 3834, 3832, 1, 0, 0, 0, 3835, 3837, 1, 0, 
	    0, 0, 3836, 3834, 1, 0, 0, 0, 3837, 3838, 5, 39, 0, 0, 3838, 3839, 
	    5, 39, 0, 0, 3839, 3841, 5, 39, 0, 0, 3840, 3814, 1, 0, 0, 0, 3840, 
	    3827, 1, 0, 0, 0, 3841, 846, 1, 0, 0, 0, 3842, 3843, 5, 82, 0, 0, 3843, 
	    3844, 5, 34, 0, 0, 3844, 3850, 1, 0, 0, 0, 3845, 3849, 8, 0, 0, 0, 
	    3846, 3847, 5, 92, 0, 0, 3847, 3849, 9, 0, 0, 0, 3848, 3845, 1, 0, 
	    0, 0, 3848, 3846, 1, 0, 0, 0, 3849, 3852, 1, 0, 0, 0, 3850, 3848, 1, 
	    0, 0, 0, 3850, 3851, 1, 0, 0, 0, 3851, 3853, 1, 0, 0, 0, 3852, 3850, 
	    1, 0, 0, 0, 3853, 3867, 5, 34, 0, 0, 3854, 3855, 5, 82, 0, 0, 3855, 
	    3856, 5, 39, 0, 0, 3856, 3862, 1, 0, 0, 0, 3857, 3861, 8, 1, 0, 0, 
	    3858, 3859, 5, 92, 0, 0, 3859, 3861, 9, 0, 0, 0, 3860, 3857, 1, 0, 
	    0, 0, 3860, 3858, 1, 0, 0, 0, 3861, 3864, 1, 0, 0, 0, 3862, 3860, 1, 
	    0, 0, 0, 3862, 3863, 1, 0, 0, 0, 3863, 3865, 1, 0, 0, 0, 3864, 3862, 
	    1, 0, 0, 0, 3865, 3867, 5, 39, 0, 0, 3866, 3842, 1, 0, 0, 0, 3866, 
	    3854, 1, 0, 0, 0, 3867, 848, 1, 0, 0, 0, 3868, 3869, 5, 82, 0, 0, 3869, 
	    3870, 5, 34, 0, 0, 3870, 3871, 5, 34, 0, 0, 3871, 3872, 5, 34, 0, 0, 
	    3872, 3876, 1, 0, 0, 0, 3873, 3875, 9, 0, 0, 0, 3874, 3873, 1, 0, 0, 
	    0, 3875, 3878, 1, 0, 0, 0, 3876, 3877, 1, 0, 0, 0, 3876, 3874, 1, 0, 
	    0, 0, 3877, 3879, 1, 0, 0, 0, 3878, 3876, 1, 0, 0, 0, 3879, 3880, 5, 
	    34, 0, 0, 3880, 3881, 5, 34, 0, 0, 3881, 3897, 5, 34, 0, 0, 3882, 3883, 
	    5, 82, 0, 0, 3883, 3884, 5, 39, 0, 0, 3884, 3885, 5, 39, 0, 0, 3885, 
	    3886, 5, 39, 0, 0, 3886, 3890, 1, 0, 0, 0, 3887, 3889, 9, 0, 0, 0, 
	    3888, 3887, 1, 0, 0, 0, 3889, 3892, 1, 0, 0, 0, 3890, 3891, 1, 0, 0, 
	    0, 3890, 3888, 1, 0, 0, 0, 3891, 3893, 1, 0, 0, 0, 3892, 3890, 1, 0, 
	    0, 0, 3893, 3894, 5, 39, 0, 0, 3894, 3895, 5, 39, 0, 0, 3895, 3897, 
	    5, 39, 0, 0, 3896, 3868, 1, 0, 0, 0, 3896, 3882, 1, 0, 0, 0, 3897, 
	    850, 1, 0, 0, 0, 3898, 3899, 5, 88, 0, 0, 3899, 3900, 5, 39, 0, 0, 
	    3900, 3904, 1, 0, 0, 0, 3901, 3903, 8, 2, 0, 0, 3902, 3901, 1, 0, 0, 
	    0, 3903, 3906, 1, 0, 0, 0, 3904, 3902, 1, 0, 0, 0, 3904, 3905, 1, 0, 
	    0, 0, 3905, 3907, 1, 0, 0, 0, 3906, 3904, 1, 0, 0, 0, 3907, 4061, 5, 
	    39, 0, 0, 3908, 3909, 5, 66, 0, 0, 3909, 3910, 5, 34, 0, 0, 3910, 3914, 
	    1, 0, 0, 0, 3911, 3913, 8, 3, 0, 0, 3912, 3911, 1, 0, 0, 0, 3913, 3916, 
	    1, 0, 0, 0, 3914, 3912, 1, 0, 0, 0, 3914, 3915, 1, 0, 0, 0, 3915, 3917, 
	    1, 0, 0, 0, 3916, 3914, 1, 0, 0, 0, 3917, 4061, 5, 34, 0, 0, 3918, 
	    3919, 5, 66, 0, 0, 3919, 3920, 5, 39, 0, 0, 3920, 3924, 1, 0, 0, 0, 
	    3921, 3923, 8, 2, 0, 0, 3922, 3921, 1, 0, 0, 0, 3923, 3926, 1, 0, 0, 
	    0, 3924, 3922, 1, 0, 0, 0, 3924, 3925, 1, 0, 0, 0, 3925, 3927, 1, 0, 
	    0, 0, 3926, 3924, 1, 0, 0, 0, 3927, 4061, 5, 39, 0, 0, 3928, 3929, 
	    5, 66, 0, 0, 3929, 3930, 5, 34, 0, 0, 3930, 3931, 5, 34, 0, 0, 3931, 
	    3932, 5, 34, 0, 0, 3932, 3936, 1, 0, 0, 0, 3933, 3935, 9, 0, 0, 0, 
	    3934, 3933, 1, 0, 0, 0, 3935, 3938, 1, 0, 0, 0, 3936, 3937, 1, 0, 0, 
	    0, 3936, 3934, 1, 0, 0, 0, 3937, 3939, 1, 0, 0, 0, 3938, 3936, 1, 0, 
	    0, 0, 3939, 3940, 5, 34, 0, 0, 3940, 3941, 5, 34, 0, 0, 3941, 4061, 
	    5, 34, 0, 0, 3942, 3943, 5, 66, 0, 0, 3943, 3944, 5, 39, 0, 0, 3944, 
	    3945, 5, 39, 0, 0, 3945, 3946, 5, 39, 0, 0, 3946, 3950, 1, 0, 0, 0, 
	    3947, 3949, 9, 0, 0, 0, 3948, 3947, 1, 0, 0, 0, 3949, 3952, 1, 0, 0, 
	    0, 3950, 3951, 1, 0, 0, 0, 3950, 3948, 1, 0, 0, 0, 3951, 3953, 1, 0, 
	    0, 0, 3952, 3950, 1, 0, 0, 0, 3953, 3954, 5, 39, 0, 0, 3954, 3955, 
	    5, 39, 0, 0, 3955, 4061, 5, 39, 0, 0, 3956, 3957, 5, 82, 0, 0, 3957, 
	    3958, 5, 66, 0, 0, 3958, 3959, 5, 34, 0, 0, 3959, 3963, 1, 0, 0, 0, 
	    3960, 3962, 8, 3, 0, 0, 3961, 3960, 1, 0, 0, 0, 3962, 3965, 1, 0, 0, 
	    0, 3963, 3961, 1, 0, 0, 0, 3963, 3964, 1, 0, 0, 0, 3964, 3966, 1, 0, 
	    0, 0, 3965, 3963, 1, 0, 0, 0, 3966, 4061, 5, 34, 0, 0, 3967, 3968, 
	    5, 82, 0, 0, 3968, 3969, 5, 66, 0, 0, 3969, 3970, 5, 39, 0, 0, 3970, 
	    3974, 1, 0, 0, 0, 3971, 3973, 8, 2, 0, 0, 3972, 3971, 1, 0, 0, 0, 3973, 
	    3976, 1, 0, 0, 0, 3974, 3972, 1, 0, 0, 0, 3974, 3975, 1, 0, 0, 0, 3975, 
	    3977, 1, 0, 0, 0, 3976, 3974, 1, 0, 0, 0, 3977, 4061, 5, 39, 0, 0, 
	    3978, 3979, 5, 82, 0, 0, 3979, 3980, 5, 66, 0, 0, 3980, 3981, 5, 34, 
	    0, 0, 3981, 3982, 5, 34, 0, 0, 3982, 3983, 5, 34, 0, 0, 3983, 3987, 
	    1, 0, 0, 0, 3984, 3986, 9, 0, 0, 0, 3985, 3984, 1, 0, 0, 0, 3986, 3989, 
	    1, 0, 0, 0, 3987, 3988, 1, 0, 0, 0, 3987, 3985, 1, 0, 0, 0, 3988, 3990, 
	    1, 0, 0, 0, 3989, 3987, 1, 0, 0, 0, 3990, 3991, 5, 34, 0, 0, 3991, 
	    3992, 5, 34, 0, 0, 3992, 4061, 5, 34, 0, 0, 3993, 3994, 5, 82, 0, 0, 
	    3994, 3995, 5, 66, 0, 0, 3995, 3996, 5, 39, 0, 0, 3996, 3997, 5, 39, 
	    0, 0, 3997, 3998, 5, 39, 0, 0, 3998, 4002, 1, 0, 0, 0, 3999, 4001, 
	    9, 0, 0, 0, 4000, 3999, 1, 0, 0, 0, 4001, 4004, 1, 0, 0, 0, 4002, 4003, 
	    1, 0, 0, 0, 4002, 4000, 1, 0, 0, 0, 4003, 4005, 1, 0, 0, 0, 4004, 4002, 
	    1, 0, 0, 0, 4005, 4006, 5, 39, 0, 0, 4006, 4007, 5, 39, 0, 0, 4007, 
	    4061, 5, 39, 0, 0, 4008, 4009, 5, 66, 0, 0, 4009, 4010, 5, 82, 0, 0, 
	    4010, 4011, 5, 34, 0, 0, 4011, 4015, 1, 0, 0, 0, 4012, 4014, 8, 3, 
	    0, 0, 4013, 4012, 1, 0, 0, 0, 4014, 4017, 1, 0, 0, 0, 4015, 4013, 1, 
	    0, 0, 0, 4015, 4016, 1, 0, 0, 0, 4016, 4018, 1, 0, 0, 0, 4017, 4015, 
	    1, 0, 0, 0, 4018, 4061, 5, 34, 0, 0, 4019, 4020, 5, 66, 0, 0, 4020, 
	    4021, 5, 82, 0, 0, 4021, 4022, 5, 39, 0, 0, 4022, 4026, 1, 0, 0, 0, 
	    4023, 4025, 8, 2, 0, 0, 4024, 4023, 1, 0, 0, 0, 4025, 4028, 1, 0, 0, 
	    0, 4026, 4024, 1, 0, 0, 0, 4026, 4027, 1, 0, 0, 0, 4027, 4029, 1, 0, 
	    0, 0, 4028, 4026, 1, 0, 0, 0, 4029, 4061, 5, 39, 0, 0, 4030, 4031, 
	    5, 66, 0, 0, 4031, 4032, 5, 82, 0, 0, 4032, 4033, 5, 34, 0, 0, 4033, 
	    4034, 5, 34, 0, 0, 4034, 4035, 5, 34, 0, 0, 4035, 4039, 1, 0, 0, 0, 
	    4036, 4038, 9, 0, 0, 0, 4037, 4036, 1, 0, 0, 0, 4038, 4041, 1, 0, 0, 
	    0, 4039, 4040, 1, 0, 0, 0, 4039, 4037, 1, 0, 0, 0, 4040, 4042, 1, 0, 
	    0, 0, 4041, 4039, 1, 0, 0, 0, 4042, 4043, 5, 34, 0, 0, 4043, 4044, 
	    5, 34, 0, 0, 4044, 4061, 5, 34, 0, 0, 4045, 4046, 5, 66, 0, 0, 4046, 
	    4047, 5, 82, 0, 0, 4047, 4048, 5, 39, 0, 0, 4048, 4049, 5, 39, 0, 0, 
	    4049, 4050, 5, 39, 0, 0, 4050, 4054, 1, 0, 0, 0, 4051, 4053, 9, 0, 
	    0, 0, 4052, 4051, 1, 0, 0, 0, 4053, 4056, 1, 0, 0, 0, 4054, 4055, 1, 
	    0, 0, 0, 4054, 4052, 1, 0, 0, 0, 4055, 4057, 1, 0, 0, 0, 4056, 4054, 
	    1, 0, 0, 0, 4057, 4058, 5, 39, 0, 0, 4058, 4059, 5, 39, 0, 0, 4059, 
	    4061, 5, 39, 0, 0, 4060, 3898, 1, 0, 0, 0, 4060, 3908, 1, 0, 0, 0, 
	    4060, 3918, 1, 0, 0, 0, 4060, 3928, 1, 0, 0, 0, 4060, 3942, 1, 0, 0, 
	    0, 4060, 3956, 1, 0, 0, 0, 4060, 3967, 1, 0, 0, 0, 4060, 3978, 1, 0, 
	    0, 0, 4060, 3993, 1, 0, 0, 0, 4060, 4008, 1, 0, 0, 0, 4060, 4019, 1, 
	    0, 0, 0, 4060, 4030, 1, 0, 0, 0, 4060, 4045, 1, 0, 0, 0, 4061, 852, 
	    1, 0, 0, 0, 4062, 4064, 3, 871, 435, 0, 4063, 4062, 1, 0, 0, 0, 4064, 
	    4065, 1, 0, 0, 0, 4065, 4063, 1, 0, 0, 0, 4065, 4066, 1, 0, 0, 0, 4066, 
	    854, 1, 0, 0, 0, 4067, 4068, 5, 48, 0, 0, 4068, 4069, 5, 88, 0, 0, 
	    4069, 4071, 1, 0, 0, 0, 4070, 4072, 7, 4, 0, 0, 4071, 4070, 1, 0, 0, 
	    0, 4072, 4073, 1, 0, 0, 0, 4073, 4071, 1, 0, 0, 0, 4073, 4074, 1, 0, 
	    0, 0, 4074, 856, 1, 0, 0, 0, 4075, 4077, 3, 871, 435, 0, 4076, 4075, 
	    1, 0, 0, 0, 4077, 4078, 1, 0, 0, 0, 4078, 4076, 1, 0, 0, 0, 4078, 4079, 
	    1, 0, 0, 0, 4079, 4080, 1, 0, 0, 0, 4080, 4084, 5, 46, 0, 0, 4081, 
	    4083, 3, 871, 435, 0, 4082, 4081, 1, 0, 0, 0, 4083, 4086, 1, 0, 0, 
	    0, 4084, 4082, 1, 0, 0, 0, 4084, 4085, 1, 0, 0, 0, 4085, 4094, 1, 0, 
	    0, 0, 4086, 4084, 1, 0, 0, 0, 4087, 4089, 5, 46, 0, 0, 4088, 4090, 
	    3, 871, 435, 0, 4089, 4088, 1, 0, 0, 0, 4090, 4091, 1, 0, 0, 0, 4091, 
	    4089, 1, 0, 0, 0, 4091, 4092, 1, 0, 0, 0, 4092, 4094, 1, 0, 0, 0, 4093, 
	    4076, 1, 0, 0, 0, 4093, 4087, 1, 0, 0, 0, 4094, 858, 1, 0, 0, 0, 4095, 
	    4097, 3, 871, 435, 0, 4096, 4095, 1, 0, 0, 0, 4097, 4098, 1, 0, 0, 
	    0, 4098, 4096, 1, 0, 0, 0, 4098, 4099, 1, 0, 0, 0, 4099, 4107, 1, 0, 
	    0, 0, 4100, 4104, 5, 46, 0, 0, 4101, 4103, 3, 871, 435, 0, 4102, 4101, 
	    1, 0, 0, 0, 4103, 4106, 1, 0, 0, 0, 4104, 4102, 1, 0, 0, 0, 4104, 4105, 
	    1, 0, 0, 0, 4105, 4108, 1, 0, 0, 0, 4106, 4104, 1, 0, 0, 0, 4107, 4100, 
	    1, 0, 0, 0, 4107, 4108, 1, 0, 0, 0, 4108, 4109, 1, 0, 0, 0, 4109, 4110, 
	    3, 869, 434, 0, 4110, 4120, 1, 0, 0, 0, 4111, 4113, 5, 46, 0, 0, 4112, 
	    4114, 3, 871, 435, 0, 4113, 4112, 1, 0, 0, 0, 4114, 4115, 1, 0, 0, 
	    0, 4115, 4113, 1, 0, 0, 0, 4115, 4116, 1, 0, 0, 0, 4116, 4117, 1, 0, 
	    0, 0, 4117, 4118, 3, 869, 434, 0, 4118, 4120, 1, 0, 0, 0, 4119, 4096, 
	    1, 0, 0, 0, 4119, 4111, 1, 0, 0, 0, 4120, 860, 1, 0, 0, 0, 4121, 4124, 
	    3, 873, 436, 0, 4122, 4124, 5, 95, 0, 0, 4123, 4121, 1, 0, 0, 0, 4123, 
	    4122, 1, 0, 0, 0, 4124, 4130, 1, 0, 0, 0, 4125, 4129, 3, 873, 436, 
	    0, 4126, 4129, 3, 871, 435, 0, 4127, 4129, 5, 95, 0, 0, 4128, 4125, 
	    1, 0, 0, 0, 4128, 4126, 1, 0, 0, 0, 4128, 4127, 1, 0, 0, 0, 4129, 4132, 
	    1, 0, 0, 0, 4130, 4128, 1, 0, 0, 0, 4130, 4131, 1, 0, 0, 0, 4131, 862, 
	    1, 0, 0, 0, 4132, 4130, 1, 0, 0, 0, 4133, 4139, 5, 96, 0, 0, 4134, 
	    4135, 5, 92, 0, 0, 4135, 4138, 7, 5, 0, 0, 4136, 4138, 8, 6, 0, 0, 
	    4137, 4134, 1, 0, 0, 0, 4137, 4136, 1, 0, 0, 0, 4138, 4141, 1, 0, 0, 
	    0, 4139, 4137, 1, 0, 0, 0, 4139, 4140, 1, 0, 0, 0, 4140, 4142, 1, 0, 
	    0, 0, 4141, 4139, 1, 0, 0, 0, 4142, 4143, 5, 96, 0, 0, 4143, 864, 1, 
	    0, 0, 0, 4144, 4145, 5, 64, 0, 0, 4145, 4146, 5, 64, 0, 0, 4146, 4147, 
	    1, 0, 0, 0, 4147, 4156, 3, 861, 430, 0, 4148, 4149, 5, 64, 0, 0, 4149, 
	    4150, 5, 64, 0, 0, 4150, 4151, 1, 0, 0, 0, 4151, 4152, 3, 861, 430, 
	    0, 4152, 4153, 5, 46, 0, 0, 4153, 4154, 3, 861, 430, 0, 4154, 4156, 
	    1, 0, 0, 0, 4155, 4144, 1, 0, 0, 0, 4155, 4148, 1, 0, 0, 0, 4156, 866, 
	    1, 0, 0, 0, 4157, 4158, 5, 64, 0, 0, 4158, 4159, 3, 861, 430, 0, 4159, 
	    868, 1, 0, 0, 0, 4160, 4162, 5, 69, 0, 0, 4161, 4163, 7, 7, 0, 0, 4162, 
	    4161, 1, 0, 0, 0, 4162, 4163, 1, 0, 0, 0, 4163, 4165, 1, 0, 0, 0, 4164, 
	    4166, 3, 871, 435, 0, 4165, 4164, 1, 0, 0, 0, 4166, 4167, 1, 0, 0, 
	    0, 4167, 4165, 1, 0, 0, 0, 4167, 4168, 1, 0, 0, 0, 4168, 870, 1, 0, 
	    0, 0, 4169, 4170, 7, 8, 0, 0, 4170, 872, 1, 0, 0, 0, 4171, 4172, 7, 
	    9, 0, 0, 4172, 874, 1, 0, 0, 0, 4173, 4174, 5, 45, 0, 0, 4174, 4175, 
	    5, 45, 0, 0, 4175, 4179, 1, 0, 0, 0, 4176, 4178, 8, 10, 0, 0, 4177, 
	    4176, 1, 0, 0, 0, 4178, 4181, 1, 0, 0, 0, 4179, 4177, 1, 0, 0, 0, 4179, 
	    4180, 1, 0, 0, 0, 4180, 4183, 1, 0, 0, 0, 4181, 4179, 1, 0, 0, 0, 4182, 
	    4184, 5, 13, 0, 0, 4183, 4182, 1, 0, 0, 0, 4183, 4184, 1, 0, 0, 0, 
	    4184, 4186, 1, 0, 0, 0, 4185, 4187, 5, 10, 0, 0, 4186, 4185, 1, 0, 
	    0, 0, 4186, 4187, 1, 0, 0, 0, 4187, 4188, 1, 0, 0, 0, 4188, 4189, 6, 
	    437, 0, 0, 4189, 876, 1, 0, 0, 0, 4190, 4194, 5, 35, 0, 0, 4191, 4193, 
	    8, 10, 0, 0, 4192, 4191, 1, 0, 0, 0, 4193, 4196, 1, 0, 0, 0, 4194, 
	    4192, 1, 0, 0, 0, 4194, 4195, 1, 0, 0, 0, 4195, 4198, 1, 0, 0, 0, 4196, 
	    4194, 1, 0, 0, 0, 4197, 4199, 5, 13, 0, 0, 4198, 4197, 1, 0, 0, 0, 
	    4198, 4199, 1, 0, 0, 0, 4199, 4201, 1, 0, 0, 0, 4200, 4202, 5, 10, 
	    0, 0, 4201, 4200, 1, 0, 0, 0, 4201, 4202, 1, 0, 0, 0, 4202, 4203, 1, 
	    0, 0, 0, 4203, 4204, 6, 438, 0, 0, 4204, 878, 1, 0, 0, 0, 4205, 4206, 
	    5, 47, 0, 0, 4206, 4207, 5, 42, 0, 0, 4207, 4212, 1, 0, 0, 0, 4208, 
	    4211, 3, 879, 439, 0, 4209, 4211, 9, 0, 0, 0, 4210, 4208, 1, 0, 0, 
	    0, 4210, 4209, 1, 0, 0, 0, 4211, 4214, 1, 0, 0, 0, 4212, 4213, 1, 0, 
	    0, 0, 4212, 4210, 1, 0, 0, 0, 4213, 4215, 1, 0, 0, 0, 4214, 4212, 1, 
	    0, 0, 0, 4215, 4216, 5, 42, 0, 0, 4216, 4217, 5, 47, 0, 0, 4217, 4218, 
	    1, 0, 0, 0, 4218, 4219, 6, 439, 0, 0, 4219, 880, 1, 0, 0, 0, 4220, 
	    4222, 7, 11, 0, 0, 4221, 4220, 1, 0, 0, 0, 4222, 4223, 1, 0, 0, 0, 
	    4223, 4221, 1, 0, 0, 0, 4223, 4224, 1, 0, 0, 0, 4224, 4225, 1, 0, 0, 
	    0, 4225, 4226, 6, 440, 0, 0, 4226, 882, 1, 0, 0, 0, 4227, 4228, 7, 
	    12, 0, 0, 4228, 4229, 1, 0, 0, 0, 4229, 4230, 6, 441, 0, 0, 4230, 884, 
	    1, 0, 0, 0, 4231, 4232, 5, 47, 0, 0, 4232, 4235, 5, 42, 0, 0, 4233, 
	    4235, 7, 13, 0, 0, 4234, 4231, 1, 0, 0, 0, 4234, 4233, 1, 0, 0, 0, 
	    4235, 886, 1, 0, 0, 0, 4236, 4237, 9, 0, 0, 0, 4237, 888, 1, 0, 0, 
	    0, 61, 0, 3747, 3797, 3799, 3806, 3808, 3812, 3821, 3834, 3840, 3848, 
	    3850, 3860, 3862, 3866, 3876, 3890, 3896, 3904, 3914, 3924, 3936, 3950, 
	    3963, 3974, 3987, 4002, 4015, 4026, 4039, 4054, 4060, 4065, 4073, 4078, 
	    4084, 4091, 4093, 4098, 4104, 4107, 4115, 4119, 4123, 4128, 4130, 4137, 
	    4139, 4155, 4162, 4167, 4179, 4183, 4186, 4194, 4198, 4201, 4210, 4212, 
	    4223, 4234, 1, 0, 1, 0
	]);