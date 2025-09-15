#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::disk::disk_manager::DiskManager;
    use crate::disk::file_system::FsDiskManager;
    use crate::heap::heap_page::HeapPage;
    use crate::query::executor::QueryExecutor;
    use crate::query::parser::parse_sql;
    use crate::query::planner::QueryPlanner;
    use crate::query::types::Value;

    fn create_test_data(dm: &mut FsDiskManager) -> anyhow::Result<()> {
        let pid = dm.allocate_page(1)?;
        let mut hp = HeapPage::new_empty(pid);

        for i in 0i32..5 {
            let mut tuple_data = Vec::new();

            tuple_data.extend_from_slice(&i.to_le_bytes());

            let name = format!("user_{}", i);
            let name_bytes = name.as_bytes();
            tuple_data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            tuple_data.extend_from_slice(name_bytes);

            hp.insert_tuple(&tuple_data)?;
        }

        dm.write_page(&hp.page)?;
        Ok(())
    }

    #[test]
    fn test_end_to_end_query_processing() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path().to_str().unwrap())?;

        create_test_data(&mut dm)?;

        let sql = "SELECT * FROM users";
        let stmt = parse_sql(sql)?;
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt)?;
        let executor = QueryExecutor::new();
        let result = executor.execute(plan, &mut dm)?;

        assert_eq!(result.rows.len(), 5);

        for (i, row) in result.rows.iter().enumerate() {
            assert_eq!(row.len(), 2);
            if let Value::Integer(val) = &row[0] {
                assert_eq!(*val, i as i32);
            } else {
                panic!("Expected integer value");
            }
        }

        Ok(())
    }

    #[test]
    fn test_query_with_limit() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path().to_str().unwrap())?;

        create_test_data(&mut dm)?;

        let sql = "SELECT * FROM users LIMIT 3";
        let stmt = parse_sql(sql)?;
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt)?;
        let executor = QueryExecutor::new();
        let result = executor.execute(plan, &mut dm)?;

        assert_eq!(result.rows.len(), 3);

        Ok(())
    }

    #[test]
    fn test_parser_error_handling() {
        let invalid_sql = "INVALID GARBAGE";
        let result = parse_sql(invalid_sql);
        assert!(result.is_err());

        let missing_select = "FROM users";
        let result = parse_sql(missing_select);
        assert!(result.is_err());

        let unclosed_string = "SELECT 'hello FROM users";
        let result = parse_sql(unclosed_string);
        assert!(result.is_err());

        let empty_sql = "";
        let result = parse_sql(empty_sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_expression_only_query() {
        let sql = "SELECT 42";
        let stmt = parse_sql(sql);
        assert!(stmt.is_ok());

        let planner = QueryPlanner::new();
        let result = planner.plan(&stmt.unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_expression_parsing() -> anyhow::Result<()> {
        let sql = "SELECT 1 + 2 * 3";
        let stmt = parse_sql(sql)?;

        match stmt {
            crate::query::ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                assert!(select.from.is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn test_boolean_and_string_literals() -> anyhow::Result<()> {
        let sql = "SELECT true, false, 'hello', 123";
        let stmt = parse_sql(sql)?;

        match stmt {
            crate::query::ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 4);
            }
        }

        Ok(())
    }

    #[test]
    fn test_where_clause_parsing() -> anyhow::Result<()> {
        let sql = "SELECT * FROM users WHERE id = 42";
        let stmt = parse_sql(sql)?;

        match stmt {
            crate::query::ast::Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                assert_eq!(select.from, Some("users".to_string()));
            }
        }

        Ok(())
    }
}
