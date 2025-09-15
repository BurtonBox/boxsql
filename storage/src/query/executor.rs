use crate::disk::disk_manager::DiskManager;
use crate::heap::heap_page::HeapPage;
use crate::query::ast::Expression;
use crate::query::planner::PhysicalPlan;
use crate::query::types::{Row, Schema, Value};

pub struct QueryResult {
    pub rows: Vec<Row>,
    pub schema: Schema,
}

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

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
                let rows = self.execute_filter_with_schema(
                    &predicate,
                    input_result.rows,
                    &input_result.schema,
                )?;
                Ok(QueryResult {
                    rows,
                    schema: input_result.schema,
                })
            }
            PhysicalPlan::Projection { exprs, input } => {
                let input_result = self.execute(*input, disk_manager)?;
                let (rows, schema) = self.execute_projection_with_schema(
                    &exprs,
                    input_result.rows,
                    &input_result.schema,
                )?;
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

    fn execute_projection_with_schema(
        &self,
        exprs: &[Expression],
        input_rows: Vec<Row>,
        input_schema: &Schema,
    ) -> anyhow::Result<(Vec<Row>, Schema)> {
        let mut result_rows = Vec::new();

        for input_row in input_rows {
            let mut output_row = Vec::new();

            for expr in exprs {
                let value = self.evaluate_expression_with_schema(expr, &input_row, input_schema)?;
                output_row.push(value);
            }

            result_rows.push(output_row);
        }

        // Create output schema based on expressions
        let output_schema = self.create_projection_schema(exprs, input_schema)?;

        Ok((result_rows, output_schema))
    }

    fn create_projection_schema(
        &self,
        exprs: &[Expression],
        input_schema: &Schema,
    ) -> anyhow::Result<Schema> {
        use crate::query::types::{Column, DataType};

        let mut columns = Vec::new();

        for expr in exprs {
            let (name, data_type) = match expr {
                Expression::Column { name } => {
                    // Find the column in input schema
                    let input_col = input_schema
                        .columns
                        .iter()
                        .find(|col| &col.name == name)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Column '{}' not found in input schema", name)
                        })?;
                    (name.clone(), input_col.data_type.clone())
                }
                Expression::Literal { value } => {
                    let data_type = match value {
                        crate::query::types::Value::Integer(_) => DataType::Integer,
                        crate::query::types::Value::Varchar(_) => DataType::Varchar(255),
                        crate::query::types::Value::Boolean(_) => DataType::Boolean,
                        crate::query::types::Value::Null => DataType::Varchar(255), // Default for nulls
                    };
                    ("literal".to_string(), data_type)
                }
                Expression::BinaryOp { .. } => {
                    // For now, assume binary ops produce integers (simplification)
                    ("expr".to_string(), DataType::Integer)
                }
            };

            columns.push(Column {
                name,
                data_type,
                nullable: true,
            });
        }

        Ok(Schema::new(columns))
    }

    fn execute_limit(&self, limit: u32, input_rows: Vec<Row>) -> Vec<Row> {
        input_rows.into_iter().take(limit as usize).collect()
    }

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

    fn execute_filter_with_schema(
        &self,
        predicate: &Expression,
        input_rows: Vec<Row>,
        schema: &Schema,
    ) -> anyhow::Result<Vec<Row>> {
        let mut result_rows = Vec::new();

        for row in input_rows {
            if self.evaluate_predicate_with_schema(predicate, &row, schema)? {
                result_rows.push(row);
            }
        }

        Ok(result_rows)
    }

    fn evaluate_predicate_with_schema(
        &self,
        expr: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> anyhow::Result<bool> {
        match expr {
            Expression::Literal { value } => match value {
                Value::Boolean(b) => Ok(*b),
                _ => anyhow::bail!("Non-boolean literal in predicate"),
            },
            Expression::Column { name } => {
                let value = self.lookup_column_value(name, row, schema)?;
                match value {
                    Value::Boolean(b) => Ok(b),
                    _ => anyhow::bail!("Column reference in predicate must evaluate to boolean"),
                }
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_with_schema(left, row, schema)?;
                let right_val = self.evaluate_expression_with_schema(right, row, schema)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }
        }
    }

    fn evaluate_expression_with_schema(
        &self,
        expr: &Expression,
        row: &Row,
        schema: &Schema,
    ) -> anyhow::Result<Value> {
        match expr {
            Expression::Literal { value } => Ok(value.clone()),
            Expression::Column { name } => self.lookup_column_value(name, row, schema),
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression_with_schema(left, row, schema)?;
                let right_val = self.evaluate_expression_with_schema(right, row, schema)?;
                self.evaluate_binary_op_value(&left_val, op, &right_val)
            }
        }
    }

    fn lookup_column_value(
        &self,
        column_name: &str,
        row: &Row,
        schema: &Schema,
    ) -> anyhow::Result<Value> {
        // Find the column index in the schema
        let column_index = schema
            .columns
            .iter()
            .position(|col| col.name == column_name)
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in schema", column_name))?;

        // Get the value from the row
        row.get(column_index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Row has fewer columns than schema indicates"))
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &crate::query::ast::BinaryOperator,
        right: &Value,
    ) -> anyhow::Result<bool> {
        use crate::query::ast::BinaryOperator;

        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(match op {
                BinaryOperator::Eq => l == r,
                BinaryOperator::Ne => l != r,
                BinaryOperator::Lt => l < r,
                BinaryOperator::Le => l <= r,
                BinaryOperator::Gt => l > r,
                BinaryOperator::Ge => l >= r,
                _ => anyhow::bail!(
                    "Operator {:?} not supported for integers in boolean context",
                    op
                ),
            }),
            (Value::Varchar(l), Value::Varchar(r)) => Ok(match op {
                BinaryOperator::Eq => l == r,
                BinaryOperator::Ne => l != r,
                BinaryOperator::Lt => l < r,
                BinaryOperator::Le => l <= r,
                BinaryOperator::Gt => l > r,
                BinaryOperator::Ge => l >= r,
                _ => anyhow::bail!(
                    "Operator {:?} not supported for strings in boolean context",
                    op
                ),
            }),
            (Value::Boolean(l), Value::Boolean(r)) => Ok(match op {
                BinaryOperator::Eq => l == r,
                BinaryOperator::Ne => l != r,
                BinaryOperator::And => *l && *r,
                BinaryOperator::Or => *l || *r,
                _ => anyhow::bail!("Operator {:?} not supported for booleans", op),
            }),
            _ => anyhow::bail!(
                "Cannot compare {:?} and {:?} with operator {:?}",
                left,
                right,
                op
            ),
        }
    }

    fn evaluate_binary_op_value(
        &self,
        left: &Value,
        op: &crate::query::ast::BinaryOperator,
        right: &Value,
    ) -> anyhow::Result<Value> {
        use crate::query::ast::BinaryOperator;

        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => Ok(match op {
                BinaryOperator::Add => Value::Integer(l + r),
                BinaryOperator::Sub => Value::Integer(l - r),
                BinaryOperator::Mul => Value::Integer(l * r),
                BinaryOperator::Div => {
                    if *r == 0 {
                        anyhow::bail!("Division by zero");
                    }
                    Value::Integer(l / r)
                }
                BinaryOperator::Eq => Value::Boolean(l == r),
                BinaryOperator::Ne => Value::Boolean(l != r),
                BinaryOperator::Lt => Value::Boolean(l < r),
                BinaryOperator::Le => Value::Boolean(l <= r),
                BinaryOperator::Gt => Value::Boolean(l > r),
                BinaryOperator::Ge => Value::Boolean(l >= r),
                _ => anyhow::bail!("Operator {:?} not supported for integers", op),
            }),
            (Value::Varchar(l), Value::Varchar(r)) => Ok(match op {
                BinaryOperator::Add => Value::Varchar(format!("{}{}", l, r)),
                BinaryOperator::Eq => Value::Boolean(l == r),
                BinaryOperator::Ne => Value::Boolean(l != r),
                BinaryOperator::Lt => Value::Boolean(l < r),
                BinaryOperator::Le => Value::Boolean(l <= r),
                BinaryOperator::Gt => Value::Boolean(l > r),
                BinaryOperator::Ge => Value::Boolean(l >= r),
                _ => anyhow::bail!("Operator {:?} not supported for strings", op),
            }),
            (Value::Boolean(l), Value::Boolean(r)) => Ok(match op {
                BinaryOperator::And => Value::Boolean(*l && *r),
                BinaryOperator::Or => Value::Boolean(*l || *r),
                BinaryOperator::Eq => Value::Boolean(l == r),
                BinaryOperator::Ne => Value::Boolean(l != r),
                _ => anyhow::bail!("Operator {:?} not supported for booleans", op),
            }),
            _ => anyhow::bail!(
                "Cannot apply operator {:?} to {:?} and {:?}",
                op,
                left,
                right
            ),
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

        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 0);
        assert_eq!(query_result.schema, schema);
    }
}
