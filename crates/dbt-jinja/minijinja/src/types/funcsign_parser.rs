use std::{collections::BTreeMap, sync::Arc};

use crate::types::{
    adapter::AdapterType,
    agate_table::AgateTableType,
    api::{ApiColumnType, ApiType},
    builtin::Type,
    class::DynClassType,
    column_schema::ColumnSchemaType,
    config::ConfigType,
    dict::DictType,
    function::{DynFunctionType, LambdaType},
    hook::HookType,
    information_schema::InformationSchemaType,
    list::ListType,
    model::ModelType,
    node::NodeType,
    relation::RelationType,
    struct_::StructType,
    tuple::TupleType,
    union::UnionType,
};

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub location: CodeLocation,
}

impl ParseError {
    pub fn new(message: String, location: CodeLocation) -> Self {
        Self { message, location }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Parse error at line {}, column {}: {}",
            self.location.line, self.location.col, self.message
        )
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
pub struct CodeLocation {
    pub line: u32,
    pub col: u32,
    #[allow(dead_code)]
    pub index: u32,
}

impl CodeLocation {
    pub fn default() -> Self {
        Self {
            line: 1,
            col: 1,
            index: 0,
        }
    }
}

fn get_token_location(tokens: &[Token], index: usize) -> CodeLocation {
    tokens
        .get(index)
        .and_then(|t| t.location())
        .unwrap_or_else(CodeLocation::default)
}

#[derive(Debug)]
enum Token {
    OpenParen(CodeLocation),
    CloseParen(CodeLocation),
    OpenBracket(CodeLocation),
    CloseBracket(CodeLocation),
    OpenBrace(CodeLocation),
    CloseBrace(CodeLocation),
    Comma(CodeLocation),
    Arrow(CodeLocation),
    Colon(CodeLocation),
    Identifier(String, CodeLocation),
}

impl Token {
    fn location(&self) -> Option<CodeLocation> {
        match self {
            Token::OpenParen(loc) => Some(loc.clone()),
            Token::CloseParen(loc) => Some(loc.clone()),
            Token::OpenBracket(loc) => Some(loc.clone()),
            Token::CloseBracket(loc) => Some(loc.clone()),
            Token::OpenBrace(loc) => Some(loc.clone()),
            Token::CloseBrace(loc) => Some(loc.clone()),
            Token::Comma(loc) => Some(loc.clone()),
            Token::Arrow(loc) => Some(loc.clone()),
            Token::Colon(loc) => Some(loc.clone()),
            Token::Identifier(_, loc) => Some(loc.clone()),
        }
    }
}

fn tokenize(s: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut line = 1u32;
    let mut col = 1u32;
    let mut index = 0u32;

    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if c.is_whitespace() {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        } else if c == '(' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::OpenParen(CodeLocation { line, col, index }));
            col += 1;
        } else if c == ')' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::CloseParen(CodeLocation { line, col, index }));
            col += 1;
        } else if c == '[' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::OpenBracket(CodeLocation { line, col, index }));
            col += 1;
        } else if c == ']' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::CloseBracket(CodeLocation { line, col, index }));
            col += 1;
        } else if c == '{' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::OpenBrace(CodeLocation { line, col, index }));
            col += 1;
        } else if c == '}' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::CloseBrace(CodeLocation { line, col, index }));
            col += 1;
        } else if c == ',' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::Comma(CodeLocation { line, col, index }));
            col += 1;
        } else if c == '-' && i + 1 < chars.len() && chars[i + 1] == '>' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::Arrow(CodeLocation { line, col, index }));
            i += 1; // Skip the '>' character
            col += 2;
        } else if c == ':' {
            if !current_token.is_empty() {
                tokens.push(Token::Identifier(
                    current_token.clone(),
                    CodeLocation { line, col, index },
                ));
                current_token = String::new();
            }
            tokens.push(Token::Colon(CodeLocation { line, col, index }));
            col += 1;
        } else {
            current_token.push(c);
            col += 1;
        }

        i += 1;
        index += 1;
    }

    if !current_token.is_empty() {
        tokens.push(Token::Identifier(
            current_token.clone(),
            CodeLocation { line, col, index },
        ));
    }
    tokens
}

fn parse_type(tokens: &[Token], index: usize) -> Result<(Type, usize), ParseError> {
    let next_token = tokens.get(index);
    match next_token {
        Some(Token::OpenParen(_)) => {
            let (args, ret_type, consumed) = parse_lambda(tokens, index)?;
            Ok((
                Type::Function(DynFunctionType::new(Arc::new(LambdaType::new(
                    args, ret_type,
                )))),
                consumed,
            ))
        }
        Some(Token::Identifier(id, location)) => match id.as_str() {
            "string" => Ok((Type::String(None), 1)),
            "integer" => Ok((Type::Integer(None), 1)),
            "float" => Ok((Type::Float, 1)),
            "bool" => Ok((Type::Bool, 1)),
            "bytes" => Ok((Type::Bytes, 1)),
            "seq" | "list" => {
                let (parameters, consumed) = parse_list(tokens, index + 1)?;
                if parameters.len() != 1 {
                    let location = get_token_location(tokens, index);
                    Err(ParseError::new(
                        "Expected 1 parameter for list type".to_string(),
                        location,
                    ))
                } else {
                    Ok((
                        Type::List(ListType::new(parameters[0].clone())),
                        1 + consumed,
                    ))
                }
            }
            "dict" => {
                let (parameters, consumed) = parse_list(tokens, index + 1)?;
                if parameters.len() != 2 {
                    let location = get_token_location(tokens, index);
                    Err(ParseError::new(
                        "Expected 2 parameters for dict type".to_string(),
                        location,
                    ))
                } else {
                    Ok((
                        Type::Dict(DictType::new(parameters[0].clone(), parameters[1].clone())),
                        1 + consumed,
                    ))
                }
            }
            "tuple" => {
                let (elements, consumed) = parse_list(tokens, index + 1)?;
                Ok((Type::Tuple(TupleType::new(elements)), 1 + consumed))
            }
            "struct" => {
                let (fields, consumed) = parse_fields(tokens, index + 1)?;
                Ok((Type::Struct(StructType::new(fields)), 1 + consumed))
            }
            "optional" => {
                let (parameters, consumed) = parse_list(tokens, index + 1)?;
                if parameters.len() != 1 {
                    let location = get_token_location(tokens, index);
                    Err(ParseError::new(
                        "Expected 1 parameter for optional type".to_string(),
                        location,
                    ))
                } else {
                    Ok((
                        Type::Union(UnionType::new(vec![parameters[0].clone(), Type::None])),
                        1 + consumed,
                    ))
                }
            }
            "relation" => Ok((
                Type::Class(DynClassType::new(Arc::new(RelationType::default()))),
                1,
            )),
            "adapter" => Ok((
                Type::Class(DynClassType::new(Arc::new(AdapterType::default()))),
                1,
            )),
            "api" => Ok((
                Type::Class(DynClassType::new(Arc::new(ApiType::default()))),
                1,
            )),
            "api_column" => Ok((
                Type::Class(DynClassType::new(Arc::new(ApiColumnType::default()))),
                1,
            )),
            "column_schema" => Ok((
                Type::Class(DynClassType::new(Arc::new(ColumnSchemaType::default()))),
                1,
            )),
            "agate_table" => Ok((
                Type::Class(DynClassType::new(Arc::new(AgateTableType::default()))),
                1,
            )),
            "model" => Ok((
                Type::Class(DynClassType::new(Arc::new(ModelType::default()))),
                1,
            )),
            "none" => Ok((Type::None, 1)),
            "any" => Ok((Type::Any { hard: false }, 1)),
            "information_schema" => Ok((
                Type::Class(DynClassType::new(
                    Arc::new(InformationSchemaType::default()),
                )),
                1,
            )),
            "timestamp" => Ok((Type::TimeStamp, 1)),
            "config" => Ok((
                Type::Class(DynClassType::new(Arc::new(ConfigType::default()))),
                1,
            )),
            "hook" => Ok((
                Type::Class(DynClassType::new(Arc::new(HookType::default()))),
                1,
            )),
            "node" => Ok((
                Type::Class(DynClassType::new(Arc::new(NodeType::default()))),
                1,
            )),

            _ => Err(ParseError::new(
                format!("Unknown type: {id}"),
                location.clone(),
            )),
        },
        None => Err(ParseError::new(
            "Unexpected end of input".to_string(),
            get_token_location(tokens, index),
        )),
        _ => Err(ParseError::new(
            "Unexpected token".to_string(),
            get_token_location(tokens, index),
        )),
    }
}

fn parse_lambda(
    tokens: &[Token],
    mut index: usize,
) -> Result<(Vec<Type>, Type, usize), ParseError> {
    let start_index = index;
    let mut args = Vec::new();

    // Expect opening parenthesis
    match tokens.get(index) {
        Some(Token::OpenParen(_)) => {
            index += 1; // Skip opening paren
        }
        _ => {
            return Err(ParseError::new(
                "Expected opening parenthesis".to_string(),
                get_token_location(tokens, index),
            ))
        }
    }

    // Parse arguments
    if let Some(Token::CloseParen(_)) = tokens.get(index) {
        // Empty parameter list
        index += 1; // Skip closing paren
    } else {
        // Parse first argument
        let (arg_type, consumed) = parse_type(tokens, index)?;
        args.push(arg_type);
        index += consumed;

        // Parse remaining arguments
        while let Some(token) = tokens.get(index) {
            match token {
                Token::CloseParen(_) => {
                    index += 1; // Skip closing paren
                    break;
                }
                Token::Comma(_) => {
                    index += 1; // Skip comma
                    let (arg_type, consumed) = parse_type(tokens, index)?;
                    args.push(arg_type);
                    index += consumed;
                }
                _ => {
                    return Err(ParseError::new(
                        "Expected ',' or ')' in parameter list".to_string(),
                        get_token_location(tokens, index),
                    ))
                }
            }
        }
    }

    // Parse arrow and return type - NOW REQUIRED for function signatures
    let ret_type = match tokens.get(index) {
        Some(Token::Arrow(_)) => {
            index += 1; // Skip arrow
            let (ret_type, consumed) = parse_type(tokens, index)?;
            index += consumed;
            ret_type
        }
        Some(Token::Identifier(_, _)) => {
            // If we see an identifier after the closing paren without an arrow, it's an error
            return Err(ParseError::new(
                "Expected '->' before return type".to_string(),
                get_token_location(tokens, index),
            ));
        }
        _ => Type::None,
    };

    Ok((args, ret_type, index - start_index))
}

fn parse_list(tokens: &[Token], mut index: usize) -> Result<(Vec<Type>, usize), ParseError> {
    let start_index = index;
    match tokens.get(index) {
        Some(Token::OpenBracket(_)) => {
            index += 1; // Skip opening bracket
            let mut parameters = Vec::new();

            if let Some(Token::CloseBracket(_)) = tokens.get(index) {
                // Empty parameter list
                index += 1; // Skip closing bracket
                return Ok((parameters, index - start_index));
            }

            // Parse first parameter
            let (parameter, consumed) = parse_type(tokens, index)?;
            parameters.push(parameter);
            index += consumed;

            // Parse remaining parameters
            loop {
                match tokens.get(index) {
                    Some(Token::CloseBracket(_)) => {
                        index += 1; // Skip closing bracket
                        break;
                    }
                    Some(Token::Comma(_)) => {
                        index += 1; // Skip comma
                        let (parameter, consumed) = parse_type(tokens, index)?;
                        parameters.push(parameter);
                        index += consumed;
                    }
                    Some(_) => {
                        return Err(ParseError::new(
                            "Expected ',' or ']' in parameter list".to_string(),
                            get_token_location(tokens, index),
                        ))
                    }
                    None => {
                        return Err(ParseError::new(
                            "Unclosed bracket - expected ']'".to_string(),
                            CodeLocation::default(),
                        ))
                    }
                }
            }
            Ok((parameters, index - start_index))
        }
        _ => {
            let location = get_token_location(tokens, index);
            Err(ParseError::new(
                "Expected open bracket for list type".to_string(),
                location,
            ))
        }
    }
}

fn parse_fields(
    tokens: &[Token],
    mut index: usize,
) -> Result<(BTreeMap<String, Type>, usize), ParseError> {
    let start_index = index;
    let mut fields = BTreeMap::new();

    match tokens.get(index) {
        Some(Token::OpenBrace(_)) => {
            index += 1; // Skip opening brace

            if let Some(Token::CloseBrace(_)) = tokens.get(index) {
                // Empty field list
                index += 1; // Skip closing brace
                return Ok((fields, index - start_index));
            }

            // Parse first field
            let field_name = match tokens.get(index) {
                Some(Token::Identifier(name, _)) => {
                    index += 1; // Skip field name
                    name.clone()
                }
                _ => {
                    return Err(ParseError::new(
                        "Expected field name".to_string(),
                        get_token_location(tokens, index),
                    ))
                }
            };

            // Expect colon
            match tokens.get(index) {
                Some(Token::Colon(_)) => {
                    index += 1; // Skip colon
                }
                _ => {
                    return Err(ParseError::new(
                        "Expected ':' after field name".to_string(),
                        get_token_location(tokens, index),
                    ))
                }
            }

            let (field_type, consumed) = parse_type(tokens, index)?;
            fields.insert(field_name, field_type);
            index += consumed;

            // Parse remaining fields
            loop {
                match tokens.get(index) {
                    Some(Token::CloseBrace(_)) => {
                        index += 1; // Skip closing brace
                        break;
                    }
                    Some(Token::Comma(_)) => {
                        index += 1; // Skip comma

                        let field_name = match tokens.get(index) {
                            Some(Token::Identifier(name, _)) => {
                                index += 1; // Skip field name
                                name.clone()
                            }
                            _ => {
                                return Err(ParseError::new(
                                    "Expected field name".to_string(),
                                    get_token_location(tokens, index),
                                ))
                            }
                        };

                        // Expect colon
                        match tokens.get(index) {
                            Some(Token::Colon(_)) => {
                                index += 1; // Skip colon
                            }
                            _ => {
                                return Err(ParseError::new(
                                    "Expected ':' after field name".to_string(),
                                    get_token_location(tokens, index),
                                ))
                            }
                        }

                        let (field_type, consumed) = parse_type(tokens, index)?;
                        fields.insert(field_name, field_type);
                        index += consumed;
                    }
                    Some(_) => {
                        return Err(ParseError::new(
                            "Expected ',' or '}' in field list".to_string(),
                            get_token_location(tokens, index),
                        ))
                    }
                    None => {
                        return Err(ParseError::new(
                            "Unclosed brace - expected '}'".to_string(),
                            CodeLocation::default(),
                        ))
                    }
                }
            }
            Ok((fields, index - start_index))
        }
        _ => {
            let location = get_token_location(tokens, index);
            Err(ParseError::new(
                "Expected open brace for struct type".to_string(),
                location,
            ))
        }
    }
}

pub fn parse(s: &str) -> Result<(Vec<Type>, Type), ParseError> {
    let tokens = tokenize(s);
    let (args, ret_type, _) = parse_lambda(&tokens, 0)?;
    Ok((args, ret_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_function() {
        let (args, ret_type) = parse("() -> string").unwrap();
        assert_eq!(args.len(), 0);
        matches!(ret_type, Type::String(None));
    }

    #[test]
    fn test_parse_function_with_single_arg() {
        let (args, ret_type) = parse("(string) -> integer").unwrap();
        assert_eq!(args.len(), 1);
        matches!(args[0], Type::String(None));
        matches!(ret_type, Type::Integer(None));
    }

    #[test]
    fn test_parse_function_with_multiple_args() {
        let (args, ret_type) = parse("(string, integer, bool) -> float").unwrap();
        assert_eq!(args.len(), 3);
        matches!(args[0], Type::String(None));
        matches!(args[1], Type::Integer(None));
        matches!(args[2], Type::Bool);
        matches!(ret_type, Type::Float);
    }

    #[test]
    fn test_parse_function_returning_list() {
        let (args, ret_type) = parse("(string) -> seq[integer]").unwrap();
        assert_eq!(args.len(), 1);
        matches!(args[0], Type::String(None));
        matches!(ret_type, Type::List { .. });
    }

    #[test]
    fn test_parse_function_returning_dict() {
        let (args, ret_type) = parse("(string) -> dict[string, integer]").unwrap();
        assert_eq!(args.len(), 1);
        matches!(args[0], Type::String(None));
        matches!(ret_type, Type::Dict { .. });
    }

    #[test]
    fn test_parse_function_with_list_args() {
        let (args, ret_type) = parse("(seq[string], list[integer]) -> bool").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::List { .. });
        matches!(args[1], Type::List { .. });
        matches!(ret_type, Type::Bool);
    }

    #[test]
    fn test_parse_function_with_dict_args() {
        let (args, ret_type) =
            parse("(dict[string, integer], dict[integer, bool]) -> string").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::Dict { .. });
        matches!(args[1], Type::Dict { .. });
        matches!(ret_type, Type::String(None));
    }

    #[test]
    fn test_parse_function_with_struct_args() {
        let (args, ret_type) = parse("(struct{name: string, age: integer}) -> bool").unwrap();
        assert_eq!(args.len(), 1);
        matches!(args[0], Type::Struct(_));
        matches!(ret_type, Type::Bool);
    }

    #[test]
    fn test_parse_function_returning_struct() {
        let (args, ret_type) =
            parse("(string, integer) -> struct{id: integer, name: string, active: bool}").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::String(None));
        matches!(args[1], Type::Integer(None));
        matches!(ret_type, Type::Struct(_));
    }

    #[test]
    fn test_parse_function_with_class_types() {
        let (args, ret_type) = parse("(relation, adapter) -> api").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::Class(_));
        matches!(args[1], Type::Class(_));
        matches!(ret_type, Type::Class(_));
    }

    #[test]
    fn test_parse_function_with_nested_function_type() {
        let (args, ret_type) = parse("((string) -> integer, string) -> bool").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::Function(_));
        matches!(args[1], Type::String(None));
        matches!(ret_type, Type::Bool);
    }

    #[test]
    fn test_parse_function_with_complex_nested_types() {
        let (args, ret_type) =
            parse("(seq[dict[string, integer]], struct{users: seq[string]}) -> dict[string, bool]")
                .unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::List { .. });
        matches!(args[1], Type::Struct(_));
        matches!(ret_type, Type::Dict { .. });
    }

    #[test]
    fn test_parse_function_with_all_primitive_types() {
        let (args, ret_type) = parse("(string, integer, float, bool, bytes) -> string").unwrap();
        assert_eq!(args.len(), 5);
        matches!(args[0], Type::String(None));
        matches!(args[1], Type::Integer(None));
        matches!(args[2], Type::Float);
        matches!(args[3], Type::Bool);
        matches!(args[4], Type::Bytes);
        matches!(ret_type, Type::String(None));
    }

    #[test]
    fn test_parse_function_with_whitespace() {
        let (args, ret_type) = parse("  ( string , integer )  ->  bool  ").unwrap();
        assert_eq!(args.len(), 2);
        matches!(args[0], Type::String(None));
        matches!(args[1], Type::Integer(None));
        matches!(ret_type, Type::Bool);
    }

    #[test]
    fn test_parse_function_returning_apicolumn() {
        let (args, ret_type) = parse("(string) -> api_column").unwrap();
        assert_eq!(args.len(), 1);
        matches!(args[0], Type::String(None));
        matches!(ret_type, Type::Class(_));
    }

    #[test]
    fn test_parse_complex_function_signature() {
        let (args, ret_type) = parse("(relation, dict[string, list[string]], (relation, string, list[string]) -> string) -> list[string]").unwrap();

        // Should have 3 arguments
        assert_eq!(args.len(), 3);

        // First arg should be relation (class type)
        matches!(args[0], Type::Class(_));

        // Second arg should be dict[string, list[string]]
        matches!(args[1], Type::Dict { .. });
        if let Type::Dict(dict) = &args[1] {
            matches!(*dict.key, Type::String(None));
            if let Type::List(list) = &*dict.value {
                matches!(*list.element, Type::String(None));
            } else {
                panic!("Expected seq type for dict value");
            }
        } else {
            panic!("Expected dict type for second argument");
        }

        // Third arg should be a function type: (relation, string, list[string]) -> string
        matches!(args[2], Type::Function(_));
        if let Type::Function(func) = &args[2] {
            let func = func.downcast_ref::<LambdaType>().unwrap();

            // Function should have 3 args: relation, string, list[string]
            assert_eq!(func.args.len(), 3);
            matches!(func.args[0], Type::Class(_)); // relation
            matches!(func.args[1], Type::String(None)); // string
            matches!(func.args[2], Type::List { .. }); // list[string]

            // Function return type should be string
            matches!(func.ret_type, Type::String(None));
        } else {
            panic!("Expected function type for third argument");
        }

        // Return type should be list[string]
        matches!(ret_type, Type::List { .. });
        if let Type::List(list) = &ret_type {
            matches!(*list.element, Type::String(None));
        } else {
            panic!("Expected seq type for return value");
        }
    }

    // Tests for error conditions
    #[test]
    fn test_parse_invalid_syntax() {
        let result = parse("invalid -> -> string");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("Unexpected token") || error.message.contains("Expected"));
    }

    #[test]
    fn test_parse_missing_return_type() {
        let result = parse("(string) ->");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.message.contains("Unexpected end of input") || error.message.contains("Expected")
        );
    }

    #[test]
    fn test_parse_missing_arrow() {
        let result = parse("(string) string");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("Unexpected token") || error.message.contains("Expected"));
    }

    #[test]
    fn test_parse_unclosed_parenthesis() {
        let result = parse("(string -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.message.contains("Unexpected end of input") || error.message.contains("Expected")
        );
    }

    #[test]
    fn test_parse_unclosed_bracket() {
        let result = parse("(seq[string) -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        println!("Actual error message: '{}'", error.message);
        assert!(error
            .message
            .contains("Expected ',' or ']' in parameter list"));
    }

    #[test]
    fn test_parse_unclosed_brace() {
        let result = parse("(struct{name: string) -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        println!("Actual error message: '{}'", error.message);
        assert!(error.message.contains("Expected ',' or '}' in field list"));
    }

    // New error handling tests
    #[test]
    fn test_parse_error_with_location() {
        let result = parse("(unknown_bracket_type[) -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.location.line > 0);
        assert!(error.location.col > 0);
    }

    #[test]
    fn test_parse_error_message_display() {
        let result = parse("(invalid -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        let error_str = format!("{error}");
        assert!(error_str.contains("Parse error"));
    }

    #[test]
    fn test_parse_empty_input() {
        let result = parse("");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.message.contains("Expected opening parenthesis"));
    }

    #[test]
    fn test_parse_malformed_list_type() {
        let result = parse("(seq[string, integer, bool) -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        println!("Actual error message: '{}'", error.message);
        assert!(error
            .message
            .contains("Expected ',' or ']' in parameter list"));
    }

    #[test]
    fn test_parse_malformed_dict_type() {
        let result = parse("(dict[string) -> bool");
        assert!(result.is_err());
        let error = result.unwrap_err();
        println!("Actual error message: '{}'", error.message);
        assert!(error
            .message
            .contains("Expected ',' or ']' in parameter list"));
    }
}
