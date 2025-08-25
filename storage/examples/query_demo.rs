//! Demonstration of the BoxSQL query processing system.
//!
//! This example shows how to use the complete query pipeline:
//! SQL parsing -> Query planning -> Execution -> Results

use storage::disk::disk_manager::DiskManager;
use storage::disk::file_system::FsDiskManager;
use storage::heap::heap_page::HeapPage;
use storage::query::parser::parse_sql;
use storage::query::planner::QueryPlanner;
use storage::query::executor::QueryExecutor;

fn main() -> anyhow::Result<()> {
    println!("BoxSQL Query Processing Demo");
    println!("============================");

    // Set up temporary database
    let mut dm = FsDiskManager::new("./query_demo_data")?;
    
    // Create some test data
    create_sample_data(&mut dm)?;
    
    // Parse SQL query
    let sql = "SELECT * FROM users";
    println!("\n1. Parsing SQL: {}", sql);
    let stmt = parse_sql(sql)?;
    println!("   ✓ Parsed successfully: {:?}", stmt);
    
    // Plan the query
    println!("\n2. Creating execution plan...");
    let planner = QueryPlanner::new();
    let plan = planner.plan(&stmt)?;
    println!("   ✓ Plan created: {:?}", plan);
    
    // Execute the query
    println!("\n3. Executing query...");
    let executor = QueryExecutor::new();
    let result = executor.execute(plan, &mut dm)?;
    
    // Display results
    println!("\n4. Results:");
    println!("   Schema: {:?}", result.schema);
    println!("   Rows found: {}", result.rows.len());
    for (i, row) in result.rows.iter().enumerate() {
        println!("   Row {}: {:?}", i, row);
    }
    
    // Test with LIMIT
    println!("{}", "\n".to_owned() + "=".repeat(50).as_str());
    let sql_with_limit = "SELECT * FROM users LIMIT 3";
    println!("\n5. Testing with LIMIT: {}", sql_with_limit);
    let stmt2 = parse_sql(sql_with_limit)?;
    let plan2 = planner.plan(&stmt2)?;
    let result2 = executor.execute(plan2, &mut dm)?;
    
    println!("   Limited results: {} rows", result2.rows.len());
    for (i, row) in result2.rows.iter().enumerate() {
        println!("   Row {}: {:?}", i, row);
    }
    
    println!("\n✓ Query processing demo completed successfully!");
    
    Ok(())
}

/// Creates sample data in the database for demonstration.
fn create_sample_data(dm: &mut FsDiskManager) -> anyhow::Result<()> {
    println!("Setting up sample data...");
    
    // Create a page with test tuples matching the default schema (id: Integer, name: Varchar)
    let pid = dm.allocate_page(1)?;
    let mut hp = HeapPage::new_empty(pid);
    
    let sample_users = [
        (1i32, "Alice"),
        (2i32, "Bob"),
        (3i32, "Charlie"),
        (4i32, "Diana"),
        (5i32, "Eve"),
    ];
    
    for (id, name) in sample_users {
        let mut tuple_data = Vec::new();
        
        // Serialize integer ID
        tuple_data.extend_from_slice(&id.to_le_bytes());
        
        // Serialize varchar name (length + data)
        let name_bytes = name.as_bytes();
        tuple_data.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        tuple_data.extend_from_slice(name_bytes);
        
        hp.insert_tuple(&tuple_data)?;
    }
    
    dm.write_page(&hp.page)?;
    println!("   ✓ Created {} user records", sample_users.len());
    
    Ok(())
}