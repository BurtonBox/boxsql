//! Integration tests for end-to-end query processing.
//!
//! These tests demonstrate the complete pipeline from SQL parsing
//! through execution using the storage engine.

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    
    use crate::query::parser::parse_sql;
    use crate::query::planner::QueryPlanner;
    use crate::query::executor::QueryExecutor;
    use crate::query::types::Value;
    use crate::disk::file_system::FsDiskManager;
    use crate::disk::disk_manager::DiskManager;
    use crate::heap::heap_page::HeapPage;

    /// Creates test data in the storage engine.
    fn create_test_data(dm: &mut FsDiskManager) -> anyhow::Result<()> {
        // Create a page with some test tuples
        let pid = dm.allocate_page(1)?;
        let mut hp = HeapPage::new_empty(pid);
        
        // Insert test data that matches our default schema (id: Integer, name: Varchar)
        for i in 0i32..5 {
            let mut tuple_data = Vec::new();
            
            // Add integer id field
            tuple_data.extend_from_slice(&i.to_le_bytes());
            
            // Add varchar name field (length + data)
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
        // Set up temporary database
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path().to_str().unwrap())?;
        
        // Create test data
        create_test_data(&mut dm)?;
        
        // Parse SQL statement
        let sql = "SELECT * FROM users";
        let stmt = parse_sql(sql)?;
        
        // Plan the query
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt)?;
        
        // Execute the query
        let executor = QueryExecutor::new();
        let result = executor.execute(plan, &mut dm)?;
        
        // Verify results
        assert_eq!(result.rows.len(), 5);
        
        // Check that we got the expected integer values
        for (i, row) in result.rows.iter().enumerate() {
            assert_eq!(row.len(), 2); // id and name columns from default schema
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
        // Set up temporary database
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path().to_str().unwrap())?;
        
        // Create test data
        create_test_data(&mut dm)?;
        
        // Parse SQL statement with LIMIT
        let sql = "SELECT * FROM users LIMIT 3";
        let stmt = parse_sql(sql)?;
        
        // Plan and execute the query
        let planner = QueryPlanner::new();
        let plan = planner.plan(&stmt)?;
        
        let executor = QueryExecutor::new();
        let result = executor.execute(plan, &mut dm)?;
        
        // Verify we only got 3 rows
        assert_eq!(result.rows.len(), 3);
        
        Ok(())
    }

    #[test]
    fn test_parser_error_handling() {
        // Test completely invalid SQL (not starting with SELECT)
        let invalid_sql = "INVALID GARBAGE";
        let result = parse_sql(invalid_sql);
        assert!(result.is_err());
        
        // Test missing SELECT keyword
        let missing_select = "FROM users";
        let result = parse_sql(missing_select);
        assert!(result.is_err());
        
        // Test unclosed string literal - this should definitely fail
        let unclosed_string = "SELECT 'hello FROM users";
        let result = parse_sql(unclosed_string);
        assert!(result.is_err());
        
        // Test empty input
        let empty_sql = "";
        let result = parse_sql(empty_sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_expression_only_query() {
        // Test SQL with no FROM clause
        let sql = "SELECT 42";
        let stmt = parse_sql(sql);
        assert!(stmt.is_ok());
        
        // This should fail in planning since we don't support expression-only queries yet
        let planner = QueryPlanner::new();
        let result = planner.plan(&stmt.unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_expression_parsing() -> anyhow::Result<()> {
        // Test parsing of complex expressions
        let sql = "SELECT 1 + 2 * 3";
        let stmt = parse_sql(sql)?;
        
        // Verify the statement structure
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
        // Test parsing various literal types
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
        // Test WHERE clause parsing
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