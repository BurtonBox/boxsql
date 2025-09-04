//! Query planning and optimization.
//!
//! This module converts AST nodes into logical and physical execution plans.
//! It handles query optimization, cost estimation, and plan generation.

use crate::query::ast::{Expression, SelectStatement, Statement};
use crate::query::types::Schema;

/// Logical query plan node.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Table scan operation
    TableScan { table_name: String, schema: Schema },
    /// Projection (SELECT list) operation  
    Projection {
        exprs: Vec<Expression>,
        input: Box<LogicalPlan>,
    },
    /// Filter (WHERE clause) operation
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    /// Limit operation
    Limit { limit: u32, input: Box<LogicalPlan> },
}

/// Physical execution plan node.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential scan of table pages
    SeqScan { table_name: String, schema: Schema },
    /// Projection operation
    Projection {
        exprs: Vec<Expression>,
        input: Box<PhysicalPlan>,
    },
    /// Filter operation
    Filter {
        predicate: Expression,
        input: Box<PhysicalPlan>,
    },
    /// Limit operation
    Limit {
        limit: u32,
        input: Box<PhysicalPlan>,
    },
}

/// Query planner that converts statements to execution plans.
pub struct QueryPlanner {
    // TODO: Add catalog/schema registry
}

impl QueryPlanner {
    /// Creates a new query planner.
    pub fn new() -> Self {
        Self {}
    }

    /// Plans a SQL statement into a physical execution plan.
    pub fn plan(&self, stmt: &Statement) -> anyhow::Result<PhysicalPlan> {
        match stmt {
            Statement::Select(select) => self.plan_select(select),
        }
    }

    /// Plans a SELECT statement.
    fn plan_select(&self, select: &SelectStatement) -> anyhow::Result<PhysicalPlan> {
        // Start with table scan if FROM clause exists
        let mut plan = if let Some(table_name) = &select.from {
            // TODO: Look up schema from catalog
            let schema = self.get_table_schema(table_name)?;
            PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                schema,
            }
        } else {
            // For expressions without FROM clause, create a dummy scan
            anyhow::bail!("SELECT without FROM not yet supported");
        };

        // Add WHERE clause as filter
        if let Some(where_expr) = &select.where_clause {
            plan = PhysicalPlan::Filter {
                predicate: where_expr.clone(),
                input: Box::new(plan),
            };
        }

        // Add SELECT list as projection
        if !select
            .select_list
            .iter()
            .any(|item| matches!(item, crate::query::ast::SelectItem::Wildcard))
        {
            let exprs: Result<Vec<_>, _> = select
                .select_list
                .iter()
                .map(|item| match item {
                    crate::query::ast::SelectItem::Expression { expr, .. } => Ok(expr.clone()),
                    crate::query::ast::SelectItem::Wildcard => {
                        anyhow::bail!("Wildcard not supported in projection")
                    }
                })
                .collect();

            plan = PhysicalPlan::Projection {
                exprs: exprs?,
                input: Box::new(plan),
            };
        }

        // Add LIMIT clause
        if let Some(limit) = select.limit {
            plan = PhysicalPlan::Limit {
                limit,
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

    /// Gets the schema for a table (placeholder implementation).
    fn get_table_schema(&self, _table_name: &str) -> anyhow::Result<Schema> {
        // TODO: Implement proper catalog lookup
        // For now, return a simple test schema
        use crate::query::types::{Column, DataType};

        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Varchar(255),
                nullable: true,
            },
        ]);

        Ok(schema)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::SelectStatement;

    #[test]
    fn test_simple_select_planning() {
        let planner = QueryPlanner::new();
        let select = SelectStatement::select_all_from("users");
        let stmt = Statement::Select(select);

        let plan = planner.plan(&stmt).unwrap();

        match plan {
            PhysicalPlan::SeqScan { table_name, .. } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected SeqScan plan"),
        }
    }
}
