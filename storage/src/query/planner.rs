use crate::query::ast::{Expression, SelectStatement, Statement};
use crate::query::types::Schema;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
        schema: Schema,
    },
    Projection {
        exprs: Vec<Expression>,
        input: Box<LogicalPlan>,
    },
    Filter {
        predicate: Expression,
        input: Box<LogicalPlan>,
    },
    Limit {
        limit: u32,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table_name: String,
        schema: Schema,
    },
    Projection {
        exprs: Vec<Expression>,
        input: Box<PhysicalPlan>,
    },
    Filter {
        predicate: Expression,
        input: Box<PhysicalPlan>,
    },
    Limit {
        limit: u32,
        input: Box<PhysicalPlan>,
    },
}

pub struct QueryPlanner {
    // TODO: Add catalog/schema registry
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan(&self, stmt: &Statement) -> anyhow::Result<PhysicalPlan> {
        match stmt {
            Statement::Select(select) => self.plan_select(select),
        }
    }

    fn plan_select(&self, select: &SelectStatement) -> anyhow::Result<PhysicalPlan> {
        let mut plan = if let Some(table_name) = &select.from {
            // TODO: Look up schema from catalog
            let schema = self.get_table_schema(table_name)?;
            PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                schema,
            }
        } else {
            anyhow::bail!("SELECT without FROM not yet supported");
        };

        if let Some(where_expr) = &select.where_clause {
            plan = PhysicalPlan::Filter {
                predicate: where_expr.clone(),
                input: Box::new(plan),
            };
        }

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

        if let Some(limit) = select.limit {
            plan = PhysicalPlan::Limit {
                limit,
                input: Box::new(plan),
            };
        }

        Ok(plan)
    }

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
