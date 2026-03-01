//! SQL parser for stream processing queries
//!
//! Parses SQL-like queries into AST for stream processing

use crate::query::ast::{
    JoinCondition, JoinTable, JoinType, OrderByClause, OrderByField, SortDirection, WhereClause, *,
};
use thiserror::Error;

/// Parser error
#[derive(Debug, Error)]
pub enum SqlParseError {
    #[error("Parse error: {0}")]
    Syntax(String),
    #[error("Unexpected token: {0}")]
    UnexpectedToken(String),
    #[error("Expected token: {0}")]
    ExpectedToken(String),
}

/// SQL parser for stream processing
pub struct SqlParser;

impl SqlParser {
    /// Parse a SQL query string
    pub fn parse(query: &str) -> Result<Query, SqlParseError> {
        let query = query.trim();

        if !query.to_uppercase().starts_with("SELECT") {
            return Err(SqlParseError::Syntax(
                "Query must start with SELECT".to_string(),
            ));
        }

        // Tokenize the query
        let tokens = Self::tokenize(query);

        // Parse SELECT clause
        let (select, mut pos) = Self::parse_select(&tokens, 0)?;

        // Parse FROM clause
        let (from, pos_after_from) = Self::parse_from(&tokens, pos)?;
        pos = pos_after_from;

        // Parse WHERE clause (optional)
        let (where_clause, pos_after_where) = Self::parse_where(&tokens, pos)?;
        if where_clause.is_some() {
            pos = pos_after_where;
        }

        // Parse GROUP BY clause (optional)
        let (group_by, pos_after_group) = Self::parse_group_by(&tokens, pos)?;
        if group_by.is_some() {
            pos = pos_after_group;
        }

        // Parse HAVING clause (optional)
        let (having, pos_after_having) = Self::parse_having(&tokens, pos)?;
        pos = pos_after_having;

        // Parse window specification (optional) - e.g., "WINDOW TUMBLING 60 SECONDS"
        let (window, pos_after_window) = Self::parse_window(&tokens, pos)?;
        pos = pos_after_window;

        // Parse ORDER BY clause (optional)
        let (order_by, pos_after_order) = Self::parse_order_by(&tokens, pos)?;
        pos = pos_after_order;

        // Parse LIMIT clause (optional)
        let (limit, pos_after_limit) = Self::parse_limit(&tokens, pos)?;
        pos = pos_after_limit;

        // Parse OFFSET clause (optional)
        let (offset, _) = Self::parse_offset(&tokens, pos)?;

        // Extract aggregations from SELECT clause
        let aggregations = Self::extract_aggregations(&select);

        Ok(Query {
            select,
            from,
            where_clause,
            group_by,
            having,
            aggregations: if aggregations.is_empty() {
                None
            } else {
                Some(aggregations)
            },
            window,
            order_by,
            limit,
            offset,
        })
    }

    /// Tokenize SQL query
    fn tokenize(query: &str) -> Vec<String> {
        // Strip -- line comments before tokenizing
        let stripped: String = query
            .lines()
            .map(|line| {
                if let Some(idx) = line.find("--") {
                    &line[..idx]
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        // Simple tokenizer - split on whitespace and handle quoted strings
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut quote_char = '\0';

        for ch in stripped.chars() {
            match ch {
                '\'' | '"' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                    current.push(ch);
                }
                c if c == quote_char && in_quotes => {
                    in_quotes = false;
                    quote_char = '\0';
                    current.push(c);
                }
                ' ' | '\t' | '\n' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                }
                ',' | '(' | ')' | '=' | '!' | '<' | '>' | '+' | '-' | '*' | '/' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push(ch.to_string());
                }
                c => {
                    current.push(c);
                }
            }
        }

        if !current.is_empty() {
            tokens.push(current);
        }

        tokens
    }

    /// Parse SELECT clause
    fn parse_select(
        tokens: &[String],
        start: usize,
    ) -> Result<(SelectClause, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "SELECT" {
            return Err(SqlParseError::Syntax("Expected SELECT".to_string()));
        }

        let mut pos = start + 1;
        let mut fields = Vec::new();

        // Parse field list
        while pos < tokens.len() && tokens[pos].to_uppercase() != "FROM" {
            if tokens[pos] == "*" {
                fields.push(SelectField::All);
                pos += 1;
                break;
            }

            // Check for aggregation function
            let upper = tokens[pos].to_uppercase();
            if matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "MEDIAN"
            ) {
                let (agg, new_pos) = Self::parse_aggregation(tokens, pos)?;
                fields.push(SelectField::Aggregation(agg));
                pos = new_pos;
            } else {
                // Regular field
                let field_name = tokens[pos].clone();
                pos += 1;

                // Check for alias (AS keyword)
                if pos < tokens.len() && tokens[pos].to_uppercase() == "AS" {
                    pos += 1;
                    if pos >= tokens.len() {
                        return Err(SqlParseError::Syntax("Expected alias after AS".to_string()));
                    }
                    let alias = tokens[pos].clone();
                    pos += 1;
                    fields.push(SelectField::Aliased {
                        field: field_name,
                        alias,
                    });
                } else {
                    fields.push(SelectField::Field(field_name));
                }
            }

            // Skip comma
            if pos < tokens.len() && tokens[pos] == "," {
                pos += 1;
            }
        }

        if fields.is_empty() {
            fields.push(SelectField::All);
        }

        Ok((SelectClause { fields }, pos))
    }

    /// Parse aggregation function
    fn parse_aggregation(
        tokens: &[String],
        start: usize,
    ) -> Result<(Aggregation, usize), SqlParseError> {
        let func_name = tokens[start].to_uppercase();
        let mut pos = start + 1;

        // Skip opening parenthesis
        if pos >= tokens.len() || tokens[pos] != "(" {
            return Err(SqlParseError::Syntax(
                "Expected ( after aggregation function".to_string(),
            ));
        }
        pos += 1;

        // Parse field or *
        let field = if pos < tokens.len() && tokens[pos] == "*" {
            pos += 1;
            None
        } else if pos < tokens.len() {
            let field_name = tokens[pos].clone();
            pos += 1;
            Some(field_name)
        } else {
            None
        };

        // Skip closing parenthesis
        if pos >= tokens.len() || tokens[pos] != ")" {
            return Err(SqlParseError::Syntax(
                "Expected ) after aggregation".to_string(),
            ));
        }
        pos += 1;

        // Check for alias
        let alias = if pos < tokens.len() && tokens[pos].to_uppercase() == "AS" {
            pos += 1;
            if pos >= tokens.len() {
                return Err(SqlParseError::Syntax("Expected alias after AS".to_string()));
            }
            let alias = tokens[pos].clone();
            pos += 1;
            Some(alias)
        } else {
            None
        };

        Ok((
            Aggregation {
                function: func_name,
                field,
                alias,
            },
            pos,
        ))
    }

    /// Parse FROM clause (supports JOINs)
    fn parse_from(tokens: &[String], start: usize) -> Result<(FromClause, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "FROM" {
            return Err(SqlParseError::Syntax("Expected FROM".to_string()));
        }

        let mut pos = start + 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax(
                "Expected stream name after FROM".to_string(),
            ));
        }

        // Parse first table
        let (mut from_clause, pos_after_first) = Self::parse_table_reference(tokens, pos)?;
        pos = pos_after_first;

        // Parse JOINs (can be chained)
        while pos < tokens.len() {
            let upper = tokens[pos].to_uppercase();

            // Check if this is a JOIN keyword
            if upper == "JOIN"
                || upper == "INNER"
                || upper == "LEFT"
                || upper == "RIGHT"
                || (upper == "FULL"
                    && pos + 1 < tokens.len()
                    && tokens[pos + 1].to_uppercase() == "OUTER")
            {
                // Parse join type
                let join_type = if upper == "INNER" {
                    pos += 1;
                    if pos >= tokens.len() || tokens[pos].to_uppercase() != "JOIN" {
                        return Err(SqlParseError::Syntax(
                            "Expected JOIN after INNER".to_string(),
                        ));
                    }
                    pos += 1;
                    JoinType::Inner
                } else if upper == "LEFT" {
                    pos += 1;
                    if pos >= tokens.len() || tokens[pos].to_uppercase() != "JOIN" {
                        return Err(SqlParseError::Syntax(
                            "Expected JOIN after LEFT".to_string(),
                        ));
                    }
                    pos += 1;
                    JoinType::Left
                } else if upper == "RIGHT" {
                    pos += 1;
                    if pos >= tokens.len() || tokens[pos].to_uppercase() != "JOIN" {
                        return Err(SqlParseError::Syntax(
                            "Expected JOIN after RIGHT".to_string(),
                        ));
                    }
                    pos += 1;
                    JoinType::Right
                } else if upper == "FULL" {
                    pos += 1;
                    if pos >= tokens.len() || tokens[pos].to_uppercase() != "OUTER" {
                        return Err(SqlParseError::Syntax(
                            "Expected OUTER after FULL".to_string(),
                        ));
                    }
                    pos += 1;
                    if pos >= tokens.len() || tokens[pos].to_uppercase() != "JOIN" {
                        return Err(SqlParseError::Syntax(
                            "Expected JOIN after FULL OUTER".to_string(),
                        ));
                    }
                    pos += 1;
                    JoinType::Outer
                } else {
                    // Just "JOIN" means INNER JOIN
                    pos += 1;
                    JoinType::Inner
                };

                // Parse right table
                let (right_table, pos_after_right) = Self::parse_table_reference(tokens, pos)?;
                pos = pos_after_right;

                // Parse ON condition
                if pos >= tokens.len() || tokens[pos].to_uppercase() != "ON" {
                    return Err(SqlParseError::Syntax(
                        "Expected ON clause after JOIN".to_string(),
                    ));
                }
                pos += 1;

                // Parse join condition (left_field = right_field)
                let (left_field, pos_after_left) = Self::parse_qualified_field(tokens, pos)?;
                pos = pos_after_left;

                if pos >= tokens.len() {
                    return Err(SqlParseError::Syntax(
                        "Expected operator in JOIN condition".to_string(),
                    ));
                }

                // Parse operator (usually =, but could be <, >, etc.)
                let op = match tokens[pos].as_str() {
                    "=" => BinaryOperator::Eq,
                    "!=" | "<>" => BinaryOperator::Ne,
                    "<" => BinaryOperator::Lt,
                    "<=" => BinaryOperator::Le,
                    ">" => BinaryOperator::Gt,
                    ">=" => BinaryOperator::Ge,
                    _ => {
                        return Err(SqlParseError::Syntax(format!(
                            "Unsupported JOIN operator: {}",
                            tokens[pos]
                        )))
                    }
                };
                pos += 1;

                let (right_field, pos_after_right_field) =
                    Self::parse_qualified_field(tokens, pos)?;
                pos = pos_after_right_field;

                // Create join condition
                let condition = JoinCondition {
                    left_field,
                    right_field,
                    operator: op,
                };

                // Convert right table to JoinTable
                let join_table = match right_table {
                    FromClause::Single { stream, alias } => JoinTable { stream, alias },
                    _ => {
                        return Err(SqlParseError::Syntax(
                            "Right side of JOIN must be a single table".to_string(),
                        ))
                    }
                };

                // Build join
                from_clause = FromClause::Join {
                    left: Box::new(from_clause),
                    join_type,
                    right: join_table,
                    condition,
                };
            } else {
                // Not a JOIN, we're done
                break;
            }
        }

        Ok((from_clause, pos))
    }

    /// Parse a table reference (table name with optional alias)
    fn parse_table_reference(
        tokens: &[String],
        start: usize,
    ) -> Result<(FromClause, usize), SqlParseError> {
        if start >= tokens.len() {
            return Err(SqlParseError::Syntax("Expected table name".to_string()));
        }

        let stream = tokens[start].clone();
        let mut pos = start + 1;

        // Check for alias
        let alias = if pos < tokens.len()
            && tokens[pos].to_uppercase() != "JOIN"
            && tokens[pos].to_uppercase() != "INNER"
            && tokens[pos].to_uppercase() != "LEFT"
            && tokens[pos].to_uppercase() != "RIGHT"
            && tokens[pos].to_uppercase() != "FULL"
            && tokens[pos].to_uppercase() != "ON"
            && tokens[pos].to_uppercase() != "WHERE"
            && tokens[pos].to_uppercase() != "GROUP"
            && tokens[pos].to_uppercase() != "WINDOW"
            && tokens[pos].to_uppercase() != "ORDER"
            && tokens[pos].to_uppercase() != "LIMIT"
            && tokens[pos].to_uppercase() != "HAVING"
        {
            let alias = tokens[pos].clone();
            pos += 1;
            Some(alias)
        } else {
            None
        };

        Ok((FromClause::Single { stream, alias }, pos))
    }

    /// Parse a qualified field (table.field or just field)
    fn parse_qualified_field(
        tokens: &[String],
        start: usize,
    ) -> Result<(String, usize), SqlParseError> {
        if start >= tokens.len() {
            return Err(SqlParseError::Syntax("Expected field name".to_string()));
        }

        let mut field = tokens[start].clone();
        let mut pos = start + 1;

        // Check for table.field syntax
        if pos < tokens.len() && tokens[pos] == "." {
            pos += 1;
            if pos >= tokens.len() {
                return Err(SqlParseError::Syntax(
                    "Expected field name after .".to_string(),
                ));
            }
            field = format!("{}.{}", field, tokens[pos]);
            pos += 1;
        }

        Ok((field, pos))
    }

    /// Parse WHERE clause
    fn parse_where(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<WhereClause>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "WHERE" {
            return Ok((None, start));
        }

        let pos = start + 1;
        let (condition, new_pos) = Self::parse_expression(tokens, pos)?;

        Ok((Some(WhereClause { condition }), new_pos))
    }

    /// Parse expression (simplified - handles basic comparisons)
    fn parse_expression(
        tokens: &[String],
        start: usize,
    ) -> Result<(Expression, usize), SqlParseError> {
        if start >= tokens.len() {
            return Err(SqlParseError::Syntax("Expected expression".to_string()));
        }

        // Simple expression parser - handles: field op value
        let left = Self::parse_expression_term(tokens, start)?;
        let (left_expr, mut pos) = left;

        if pos >= tokens.len() {
            return Ok((left_expr, pos));
        }

        // Check for binary operator
        let op_str = tokens[pos].to_uppercase();
        let op = match op_str.as_str() {
            "=" | "==" => BinaryOperator::Eq,
            "!=" | "<>" => BinaryOperator::Ne,
            "<" => BinaryOperator::Lt,
            "<=" => BinaryOperator::Le,
            ">" => BinaryOperator::Gt,
            ">=" => BinaryOperator::Ge,
            "AND" => BinaryOperator::And,
            "OR" => BinaryOperator::Or,
            _ => return Ok((left_expr, pos)), // No operator, return left side
        };

        pos += 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax("Expected right operand".to_string()));
        }

        let (right_expr, new_pos) = Self::parse_expression_term(tokens, pos)?;

        Ok((Expression::binary_op(left_expr, op, right_expr), new_pos))
    }

    /// Parse expression term (field or literal)
    fn parse_expression_term(
        tokens: &[String],
        start: usize,
    ) -> Result<(Expression, usize), SqlParseError> {
        if start >= tokens.len() {
            return Err(SqlParseError::Syntax(
                "Expected expression term".to_string(),
            ));
        }

        let token = &tokens[start];

        // Check if it's a string literal
        if (token.starts_with('\'') && token.ends_with('\''))
            || (token.starts_with('"') && token.ends_with('"'))
        {
            let value = token[1..token.len() - 1].to_string();
            return Ok((Expression::literal(Value::String(value)), start + 1));
        }

        // Check if it's a number
        if let Ok(i) = token.parse::<i64>() {
            return Ok((Expression::literal(Value::Integer(i)), start + 1));
        }
        if let Ok(f) = token.parse::<f64>() {
            return Ok((Expression::literal(Value::Float(f)), start + 1));
        }

        // Check if it's a boolean
        if token.to_uppercase() == "TRUE" {
            return Ok((Expression::literal(Value::Boolean(true)), start + 1));
        }
        if token.to_uppercase() == "FALSE" {
            return Ok((Expression::literal(Value::Boolean(false)), start + 1));
        }

        // Otherwise, it's a field reference
        Ok((Expression::field(token), start + 1))
    }

    /// Parse GROUP BY clause
    fn parse_group_by(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<Vec<String>>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "GROUP" {
            return Ok((None, start));
        }

        let mut pos = start + 1;
        if pos >= tokens.len() || tokens[pos].to_uppercase() != "BY" {
            return Ok((None, start));
        }

        pos += 1;
        let mut fields = Vec::new();

        while pos < tokens.len() {
            let token_upper = tokens[pos].to_uppercase();
            if token_upper == "WINDOW"
                || token_upper == "HAVING"
                || token_upper == "ORDER"
                || token_upper == "LIMIT"
            {
                break;
            }
            fields.push(tokens[pos].clone());
            pos += 1;

            if pos < tokens.len() && tokens[pos] == "," {
                pos += 1;
            } else {
                break;
            }
        }

        Ok((Some(fields), pos))
    }

    /// Parse HAVING clause
    fn parse_having(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<WhereClause>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "HAVING" {
            return Ok((None, start));
        }

        let pos = start + 1;
        let (condition, new_pos) = Self::parse_expression(tokens, pos)?;

        Ok((Some(WhereClause { condition }), new_pos))
    }

    /// Parse ORDER BY clause
    fn parse_order_by(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<OrderByClause>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "ORDER" {
            return Ok((None, start));
        }

        let mut pos = start + 1;
        if pos >= tokens.len() || tokens[pos].to_uppercase() != "BY" {
            return Ok((None, start));
        }

        pos += 1;
        let mut fields = Vec::new();

        loop {
            if pos >= tokens.len() {
                break;
            }

            let token_upper = tokens[pos].to_uppercase();
            if token_upper == "LIMIT" || token_upper == "OFFSET" {
                break;
            }

            let field = tokens[pos].clone();
            pos += 1;

            // Check for ASC/DESC
            let direction = if pos < tokens.len() {
                match tokens[pos].to_uppercase().as_str() {
                    "ASC" => {
                        pos += 1;
                        SortDirection::Asc
                    }
                    "DESC" => {
                        pos += 1;
                        SortDirection::Desc
                    }
                    _ => SortDirection::Asc,
                }
            } else {
                SortDirection::Asc
            };

            fields.push(OrderByField { field, direction });

            // Check for comma (more fields)
            if pos < tokens.len() && tokens[pos] == "," {
                pos += 1;
            } else {
                break;
            }
        }

        Ok((Some(OrderByClause { fields }), pos))
    }

    /// Parse LIMIT clause
    fn parse_limit(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<usize>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "LIMIT" {
            return Ok((None, start));
        }

        let mut pos = start + 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax(
                "Expected number after LIMIT".to_string(),
            ));
        }

        let limit = tokens[pos]
            .parse::<usize>()
            .map_err(|_| SqlParseError::Syntax("Invalid LIMIT value".to_string()))?;
        pos += 1;

        Ok((Some(limit), pos))
    }

    /// Parse OFFSET clause
    fn parse_offset(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<usize>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "OFFSET" {
            return Ok((None, start));
        }

        let pos = start + 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax(
                "Expected number after OFFSET".to_string(),
            ));
        }

        let offset = tokens[pos]
            .parse::<usize>()
            .map_err(|_| SqlParseError::Syntax("Invalid OFFSET value".to_string()))?;
        let new_pos = pos + 1;

        Ok((Some(offset), new_pos))
    }

    /// Parse window specification
    fn parse_window(
        tokens: &[String],
        start: usize,
    ) -> Result<(Option<WindowSpec>, usize), SqlParseError> {
        if start >= tokens.len() || tokens[start].to_uppercase() != "WINDOW" {
            return Ok((None, start));
        }

        let mut pos = start + 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax(
                "Expected window type after WINDOW".to_string(),
            ));
        }

        let window_type_str = tokens[pos].to_uppercase();
        let window_type = match window_type_str.as_str() {
            "TUMBLING" => WindowType::Tumbling,
            "SLIDING" => WindowType::Sliding,
            "SESSION" => WindowType::Session,
            _ => {
                return Err(SqlParseError::Syntax(format!(
                    "Unknown window type: {}",
                    window_type_str
                )))
            }
        };

        pos += 1;
        if pos >= tokens.len() {
            return Err(SqlParseError::Syntax("Expected window size".to_string()));
        }

        // Parse size
        let size_str = tokens[pos].clone();
        pos += 1;

        // Check for time unit
        let size = if pos < tokens.len() {
            let unit = tokens[pos].to_uppercase();
            match unit.as_str() {
                "SECONDS" | "SECOND" | "S" => {
                    pos += 1;
                    size_str
                        .parse::<u64>()
                        .map(WindowSize::Time)
                        .map_err(|_| SqlParseError::Syntax("Invalid window size".to_string()))?
                }
                "MINUTES" | "MINUTE" | "M" => {
                    pos += 1;
                    size_str
                        .parse::<u64>()
                        .map(|s| WindowSize::Time(s * 60))
                        .map_err(|_| SqlParseError::Syntax("Invalid window size".to_string()))?
                }
                "HOURS" | "HOUR" | "H" => {
                    pos += 1;
                    size_str
                        .parse::<u64>()
                        .map(|s| WindowSize::Time(s * 3600))
                        .map_err(|_| SqlParseError::Syntax("Invalid window size".to_string()))?
                }
                _ => {
                    // Try to parse as count (no unit consumed)
                    size_str
                        .parse::<usize>()
                        .map(WindowSize::Count)
                        .map_err(|_| SqlParseError::Syntax("Invalid window size".to_string()))?
                }
            }
        } else {
            // No unit, assume count
            size_str
                .parse::<usize>()
                .map(WindowSize::Count)
                .map_err(|_| SqlParseError::Syntax("Invalid window size".to_string()))?
        };

        Ok((Some(WindowSpec { window_type, size }), pos))
    }

    /// Extract aggregations from SELECT clause
    fn extract_aggregations(select: &SelectClause) -> Vec<Aggregation> {
        select
            .fields
            .iter()
            .filter_map(|f| {
                if let SelectField::Aggregation(agg) = f {
                    Some(agg.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = SqlParser::parse("SELECT * FROM events").unwrap();
        assert_eq!(query.from.primary_stream(), "events");
        assert!(matches!(query.select.fields[0], SelectField::All));
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = SqlParser::parse("SELECT * FROM events WHERE temperature > 20").unwrap();
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_parse_select_with_aggregation() {
        let query = SqlParser::parse("SELECT COUNT(*) FROM events").unwrap();
        assert!(query.aggregations.is_some());
        assert_eq!(query.aggregations.unwrap()[0].function, "COUNT");
    }

    #[test]
    fn test_parse_select_with_group_by() {
        let query =
            SqlParser::parse("SELECT sensor_id, AVG(temperature) FROM events GROUP BY sensor_id")
                .unwrap();
        assert!(query.group_by.is_some());
        assert_eq!(query.group_by.unwrap()[0], "sensor_id");
    }

    #[test]
    fn test_parse_select_with_window() {
        let query = SqlParser::parse("SELECT * FROM events WINDOW TUMBLING 60 SECONDS").unwrap();
        assert!(query.window.is_some());
        assert!(matches!(
            query.window.unwrap().window_type,
            WindowType::Tumbling
        ));
    }

    #[test]
    fn test_parse_select_with_median() {
        let query = SqlParser::parse("SELECT MEDIAN(temperature) FROM events").unwrap();
        assert!(query.aggregations.is_some());
        assert_eq!(query.aggregations.unwrap()[0].function, "MEDIAN");
    }

    #[test]
    fn test_parse_select_with_having() {
        let query = SqlParser::parse("SELECT sensor_id, AVG(temperature) FROM events GROUP BY sensor_id HAVING AVG(temperature) > 25").unwrap();
        assert!(query.having.is_some());
        assert!(query.group_by.is_some());
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let query = SqlParser::parse("SELECT * FROM events ORDER BY temperature DESC").unwrap();
        assert!(query.order_by.is_some());
        assert_eq!(query.order_by.as_ref().unwrap().fields.len(), 1);
        assert_eq!(
            query.order_by.as_ref().unwrap().fields[0].direction,
            SortDirection::Desc
        );
    }

    #[test]
    fn test_parse_select_with_limit() {
        let query = SqlParser::parse("SELECT * FROM events LIMIT 10").unwrap();
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_parse_join_inner() {
        let query =
            SqlParser::parse("SELECT * FROM orders o INNER JOIN payments p ON o.id = p.order_id")
                .unwrap();
        match &query.from {
            FromClause::Join {
                join_type, right, ..
            } => {
                assert_eq!(*join_type, JoinType::Inner);
                assert_eq!(right.stream, "payments");
                assert_eq!(right.alias, Some("p".to_string()));
            }
            _ => panic!("Expected JOIN clause"),
        }
    }

    #[test]
    fn test_parse_join_left() {
        let query = SqlParser::parse(
            "SELECT * FROM orders LEFT JOIN payments ON orders.id = payments.order_id",
        )
        .unwrap();
        match &query.from {
            FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, JoinType::Left);
            }
            _ => panic!("Expected JOIN clause"),
        }
    }

    #[test]
    fn test_parse_select_with_limit_offset() {
        let query = SqlParser::parse("SELECT * FROM events LIMIT 10 OFFSET 5").unwrap();
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(5));
    }
}
