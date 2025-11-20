//! Query engine for stream processing
//!
//! Provides query language, parser, optimizer, and execution

pub mod ast;
pub mod parser;
pub mod optimizer;
pub mod executor;

pub use ast::{Query, SelectClause, FromClause, WhereClause, Aggregation};
pub use parser::QueryParser;
pub use optimizer::QueryOptimizer;
pub use executor::QueryExecutor;

