//! Abstract syntax tree for queries

use crate::core::event::Event;

/// A query statement
#[derive(Debug, Clone)]
pub struct Query {
    /// SELECT clause
    pub select: SelectClause,
    /// FROM clause
    pub from: FromClause,
    /// WHERE clause (optional)
    pub where_clause: Option<WhereClause>,
    /// GROUP BY clause (optional)
    pub group_by: Option<Vec<String>>,
    /// Aggregations (optional)
    pub aggregations: Option<Vec<Aggregation>>,
    /// Window specification (optional)
    pub window: Option<WindowSpec>,
}

/// SELECT clause
#[derive(Debug, Clone)]
pub struct SelectClause {
    /// Selected fields (empty means SELECT *)
    pub fields: Vec<SelectField>,
}

/// Select field
#[derive(Debug, Clone)]
pub enum SelectField {
    /// Select all fields
    All,
    /// Select a specific field
    Field(String),
    /// Select with alias
    Aliased { field: String, alias: String },
    /// Aggregation function
    Aggregation(Aggregation),
}

/// FROM clause
#[derive(Debug, Clone)]
pub struct FromClause {
    /// Stream name
    pub stream: String,
    /// Alias (optional)
    pub alias: Option<String>,
}

/// WHERE clause
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// Filter expression
    pub condition: Expression,
}

/// Expression types
#[derive(Debug, Clone)]
pub enum Expression {
    /// Field reference
    Field(String),
    /// Literal value
    Literal(Value),
    /// Binary operation
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Function call
    FunctionCall { name: String, args: Vec<Expression> },
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Eq,  // =
    Ne,  // !=
    Lt,  // <
    Le,  // <=
    Gt,  // >
    Ge,  // >=
    And, // AND
    Or,  // OR
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
}

/// Value types
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

/// Aggregation function
#[derive(Debug, Clone)]
pub struct Aggregation {
    /// Function name (COUNT, SUM, AVG, MIN, MAX)
    pub function: String,
    /// Field to aggregate (None for COUNT(*))
    pub field: Option<String>,
    /// Alias for result
    pub alias: Option<String>,
}

/// Window specification
#[derive(Debug, Clone)]
pub struct WindowSpec {
    /// Window type
    pub window_type: WindowType,
    /// Window size
    pub size: WindowSize,
}

/// Window type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
    /// Tumbling window (non-overlapping)
    Tumbling,
    /// Sliding window (overlapping)
    Sliding,
    /// Session window (gap-based)
    Session,
}

/// Window size
#[derive(Debug, Clone)]
pub enum WindowSize {
    /// Time-based (seconds)
    Time(u64),
    /// Count-based (number of events)
    Count(usize),
}

impl Query {
    /// Create a simple SELECT query
    pub fn select(stream: &str) -> Self {
        Self {
            select: SelectClause {
                fields: vec![SelectField::All],
            },
            from: FromClause {
                stream: stream.to_string(),
                alias: None,
            },
            where_clause: None,
            group_by: None,
            aggregations: None,
            window: None,
        }
    }

    /// Add WHERE clause
    pub fn where_clause(mut self, condition: Expression) -> Self {
        self.where_clause = Some(WhereClause { condition });
        self
    }

    /// Add GROUP BY clause
    pub fn group_by(mut self, fields: Vec<String>) -> Self {
        self.group_by = Some(fields);
        self
    }

    /// Add window specification
    pub fn window(mut self, window: WindowSpec) -> Self {
        self.window = Some(window);
        self
    }
}

impl Expression {
    /// Create a field reference
    pub fn field(name: &str) -> Self {
        Self::Field(name.to_string())
    }

    /// Create a literal value
    pub fn literal(value: Value) -> Self {
        Self::Literal(value)
    }

    /// Create a binary operation
    pub fn binary_op(left: Expression, op: BinaryOperator, right: Expression) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Evaluate expression against an event
    pub fn evaluate(&self, event: &Event) -> Value {
        match self {
            Expression::Field(name) => {
                // Get field value from event payload
                match &event.value {
                    crate::core::event::EventValue::Json(json) => {
                        json.get(name)
                            .map(|v| {
                                // Try to convert to Value
                                if let Some(s) = v.as_str() {
                                    Value::String(s.to_string())
                                } else if let Some(i) = v.as_i64() {
                                    Value::Integer(i)
                                } else if let Some(f) = v.as_f64() {
                                    Value::Float(f)
                                } else if let Some(b) = v.as_bool() {
                                    Value::Boolean(b)
                                } else {
                                    Value::Null
                                }
                            })
                            .unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                }
            }
            Expression::Literal(v) => v.clone(),
            Expression::BinaryOp { left, op, right } => {
                let left_val = left.evaluate(event);
                let right_val = right.evaluate(event);
                evaluate_binary_op(&left_val, *op, &right_val)
            }
            Expression::FunctionCall { .. } => {
                // Function evaluation not yet implemented
                Value::Null
            }
        }
    }
}

fn evaluate_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Value {
    match op {
        BinaryOperator::Eq => Value::Boolean(left == right),
        BinaryOperator::Ne => Value::Boolean(left != right),
        BinaryOperator::Lt => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
            (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Le => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l <= r),
            (Value::Float(l), Value::Float(r)) => Value::Boolean(l <= r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Gt => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
            (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Ge => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l >= r),
            (Value::Float(l), Value::Float(r)) => Value::Boolean(l >= r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::And => match (left, right) {
            (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(*l && *r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(*l || *r),
            _ => Value::Boolean(false),
        },
        BinaryOperator::Add => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Integer(l + r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l + r),
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Integer(l - r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l - r),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Value::Integer(l * r),
            (Value::Float(l), Value::Float(r)) => Value::Float(l * r),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => {
                if *r == 0 {
                    Value::Null
                } else {
                    Value::Integer(l / r)
                }
            }
            (Value::Float(l), Value::Float(r)) => {
                if *r == 0.0 {
                    Value::Null
                } else {
                    Value::Float(l / r)
                }
            }
            _ => Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::{Event, EventKey, EventValue};
    use serde_json::json;

    #[test]
    fn test_expression_evaluation() {
        let event_data = json!({
            "temperature": 25,
            "humidity": 60.5,
            "active": true
        });
        let event = Event::new(
            EventKey::from_str("sensor"),
            EventValue::Json(event_data),
            0,
        );

        // Field access
        let expr = Expression::field("temperature");
        assert_eq!(expr.evaluate(&event), Value::Integer(25));

        // Comparison
        let expr = Expression::binary_op(
            Expression::field("temperature"),
            BinaryOperator::Gt,
            Expression::literal(Value::Integer(20)),
        );
        assert_eq!(expr.evaluate(&event), Value::Boolean(true));
    }
}
