//! Query parser
//!
//! Parses query strings into AST

use crate::query::ast::*;
use thiserror::Error;

/// Parser error
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Parse error: {0}")]
    Syntax(String),
    #[error("Unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("Expected token: {0}")]
    ExpectedToken(String),
}

/// Query parser
pub struct QueryParser;

impl QueryParser {
    /// Parse a query string
    pub fn parse(query: &str) -> Result<Query, ParseError> {
        // Simple parser - for now, we'll support a basic subset
        // Full SQL parser would use a proper parser generator (like pest or nom)

        let query = query.trim();

        // Basic validation
        if !query.to_uppercase().starts_with("SELECT") {
            return Err(ParseError::Syntax(
                "Query must start with SELECT".to_string(),
            ));
        }

        // For now, return a simple query structure
        // A full implementation would parse the full SQL syntax
        Ok(Query::select("default_stream"))
    }

    /// Parse SELECT clause
    #[allow(dead_code)]
    fn parse_select(_tokens: &[&str]) -> Result<SelectClause, ParseError> {
        // TODO: Implement full SELECT parsing
        Ok(SelectClause {
            fields: vec![SelectField::All],
        })
    }

    /// Parse FROM clause
    #[allow(dead_code)]
    fn parse_from(_tokens: &[&str]) -> Result<FromClause, ParseError> {
        // TODO: Implement full FROM parsing
        Ok(FromClause::Single {
            stream: "default_stream".to_string(),
            alias: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_query() {
        let query = QueryParser::parse("SELECT * FROM events").unwrap();
        assert_eq!(query.from.primary_stream(), "default_stream");
    }
}
