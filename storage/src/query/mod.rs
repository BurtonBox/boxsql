//! Query processing module for SQL parsing, planning, and execution.
//! 
//! This module provides the complete query processing pipeline:
//! - SQL parsing to abstract syntax trees
//! - Query planning and optimization  
//! - Physical execution using storage engine primitives
//!
//! # Architecture
//!
//! ```text
//! SQL String -> Parser -> AST -> Planner -> LogicalPlan -> PhysicalPlan -> Executor -> Results
//! ```
//!
//! The query processor integrates with the storage engine to provide:
//! - Table scans over heap pages
//! - Filtering and projection operations
//! - Basic aggregation and sorting
//! - Simple join algorithms

pub mod ast;
pub mod parser;
pub mod planner;
pub mod executor;
pub mod types;

#[cfg(test)]
mod integration_tests;