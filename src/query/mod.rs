//! Query engine for stream processing
//!
//! Provides query language, parser, optimizer, and execution

pub mod ast;
pub mod executor;
pub mod optimizer;
pub mod parser;

pub use ast::{Aggregation, FromClause, Query, SelectClause, WhereClause};
pub use executor::QueryExecutor;
pub use optimizer::QueryOptimizer;
pub use parser::QueryParser;
