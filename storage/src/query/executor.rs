//! Query execution engine.
//!
//! This module implements the physical execution of query plans using
//! iterators over the storage engine primitives.

use crate::disk::disk_manager::DiskManager;
use crate::heap::heap_page::HeapPage;
use crate::query::ast::Expression;
use crate::query::planner::PhysicalPlan;
use crate::query::types::{Row, Schema, Value};

/// Query execution result.
pub struct QueryResult {
    /// Result rows
    pub rows: Vec<Row>,
    /// Schema of the result
    pub schema: Schema,
}

/// Query execution engine.
pub struct QueryExecutor;

impl QueryExecutor {
    /// Creates a new query executor.
    pub fn new() -> Self {
        Self
    }

    /// Executes a physical plan and returns result rows.
    pub fn execute<D: DiskManager>(
        &self,
        plan: PhysicalPlan,
        disk_manager: &mut D,
    ) -> anyhow::Result<QueryResult> {
        match plan {
            PhysicalPlan::SeqScan { table_name, schema } => {
                let rows = self.execute_seq_scan(&table_name, &schema, disk_manager)?;
                Ok(QueryResult { rows, schema })
            }
            PhysicalPlan::Filter { predicate, input } => {
                let input_result = self.execute(*input, disk_manager)?;
                let rows = self.execute_filter(&predicate, input_result.rows)?;
                Ok(QueryResult {
                    rows,
                    schema: input_result.schema,
                })
            }
            PhysicalPlan::Projection { exprs, input } => {
                let input_result = self.execute(*input, disk_manager)?;
                let (rows, schema) =
                    self.execute_projection(&exprs, input_result.rows, &input_result.schema)?;
                Ok(QueryResult { rows, schema })
            }
            PhysicalPlan::Limit { limit, input } => {
                let input_result = self.execute(*input, disk_manager)?;
                let rows = self.execute_limit(limit, input_result.rows);
                Ok(QueryResult {
                    rows,
                    schema: input_result.schema,
                })
            }
        }
    }

    /// Executes a sequential scan.
    fn execute_seq_scan<D: DiskManager>(
        &self,
        _table_name: &str,
        schema: &Schema,
        disk_manager: &mut D,
    ) -> anyhow::Result<Vec<Row>> {
        use crate::page::page_id::PageId;

        let mut rows = Vec::new();
        let file_id = 1; // TODO: Look up actual file_id for table
        let mut page_no = 0;

        loop {
            let pid = PageId::new(file_id, page_no);

            match disk_manager.read_page(pid) {
                Ok(page) => {
                    let heap_page = HeapPage { page };

                    // Scan all slots in this page
                    for slot_no in 0..heap_page.slot_count() {
                        if let Some(tuple_data) = heap_page.read_tuple(slot_no) {
                            let row = self.deserialize_row(&tuple_data, schema)?;
                            rows.push(row);
                        }
                    }

                    page_no += 1;
                }
                Err(_) => {
                    break; // No more pages
                }
            }
        }

        Ok(rows)
    }

    /// Executes a filter operation.
    fn execute_filter(
        &self,
        predicate: &Expression,
        input_rows: Vec<Row>,
    ) -> anyhow::Result<Vec<Row>> {
        let mut result_rows = Vec::new();

        for row in input_rows {
            if self.evaluate_predicate(predicate, &row)? {
                result_rows.push(row);
            }
        }

        Ok(result_rows)
    }

    /// Executes a projection operation.
    fn execute_projection(
        &self,
        exprs: &[Expression],
        input_rows: Vec<Row>,
        _input_schema: &Schema,
    ) -> anyhow::Result<(Vec<Row>, Schema)> {
        let mut result_rows = Vec::new();

        for input_row in input_rows {
            let mut output_row = Vec::new();

            for expr in exprs {
                let value = self.evaluate_expression(expr, &input_row)?;
                output_row.push(value);
            }

            result_rows.push(output_row);
        }

        // TODO: Create proper output schema based on expressions
        // For now, create a simple schema
        use crate::query::types::{Column, DataType};
        let schema = Schema::new(
            exprs
                .iter()
                .enumerate()
                .map(|(i, _)| Column {
                    name: format!("col_{}", i),
                    data_type: DataType::Integer, // Placeholder
                    nullable: true,
                })
                .collect(),
        );

        Ok((result_rows, schema))
    }

    /// Executes a limit operation.
    fn execute_limit(&self, limit: u32, input_rows: Vec<Row>) -> Vec<Row> {
        input_rows.into_iter().take(limit as usize).collect()
    }

    /// Deserializes tuple bytes into a row of values.
    fn deserialize_row(&self, data: &[u8], schema: &Schema) -> anyhow::Result<Row> {
        let mut row = Vec::new();
        let mut offset = 0;

        for column in &schema.columns {
            let value = match &column.data_type {
                crate::query::types::DataType::Integer => {
                    if offset + 4 > data.len() {
                        anyhow::bail!("Not enough data for integer column");
                    }
                    let bytes = &data[offset..offset + 4];
                    let val = i32::from_le_bytes(bytes.try_into()?);
                    offset += 4;
                    Value::Integer(val)
                }
                crate::query::types::DataType::Varchar(_) => {
                    if offset + 4 > data.len() {
                        anyhow::bail!("Not enough data for varchar length");
                    }
                    let len_bytes = &data[offset..offset + 4];
                    let len = u32::from_le_bytes(len_bytes.try_into()?) as usize;
                    offset += 4;

                    if offset + len > data.len() {
                        anyhow::bail!("Not enough data for varchar content");
                    }
                    let string_bytes = &data[offset..offset + len];
                    let s = String::from_utf8(string_bytes.to_vec())?;
                    offset += len;
                    Value::Varchar(s)
                }
                crate::query::types::DataType::Boolean => {
                    if offset + 1 > data.len() {
                        anyhow::bail!("Not enough data for boolean column");
                    }
                    let val = data[offset] != 0;
                    offset += 1;
                    Value::Boolean(val)
                }
            };
            row.push(value);
        }

        Ok(row)
    }

    /// Evaluates a predicate expression against a row.
    fn evaluate_predicate(&self, expr: &Expression, _row: &Row) -> anyhow::Result<bool> {
        match expr {
            Expression::Literal { value } => match value {
                Value::Boolean(b) => Ok(*b),
                _ => anyhow::bail!("Non-boolean literal in predicate"),
            },
            Expression::Column { .. } => {
                // TODO: Implement column value lookup
                anyhow::bail!("Column references in predicates not yet implemented")
            }
            Expression::BinaryOp { .. } => {
                // TODO: Implement binary operation evaluation
                anyhow::bail!("Binary operations in predicates not yet implemented")
            }
        }
    }

    /// Evaluates an expression against a row.
    fn evaluate_expression(&self, expr: &Expression, _row: &Row) -> anyhow::Result<Value> {
        match expr {
            Expression::Literal { value } => Ok(value.clone()),
            Expression::Column { .. } => {
                // TODO: Implement column value lookup
                anyhow::bail!("Column references in projections not yet implemented")
            }
            Expression::BinaryOp { .. } => {
                // TODO: Implement binary operation evaluation
                anyhow::bail!("Binary operations in projections not yet implemented")
            }
        }
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::file_system::FsDiskManager;
    use crate::query::planner::PhysicalPlan;
    use crate::query::types::{Column, DataType, Schema};
    use tempfile::TempDir;

    #[test]
    fn test_query_executor_creation() {
        let executor = QueryExecutor::new();
        assert!(std::ptr::addr_of!(executor) as *const _ != std::ptr::null());
    }

    #[test]
    fn test_seq_scan_plan() {
        let temp_dir = TempDir::new().unwrap();
        let mut dm = FsDiskManager::new(temp_dir.path().to_str().unwrap()).unwrap();

        let schema = Schema::new(vec![Column {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
        }]);

        let plan = PhysicalPlan::SeqScan {
            table_name: "test".to_string(),
            schema: schema.clone(),
        };

        let executor = QueryExecutor::new();
        let result = executor.execute(plan, &mut dm);

        // Should succeed even with no data
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 0);
        assert_eq!(query_result.schema, schema);
    }
}
