use clap::{Parser, Subcommand};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::io::Write;
use storage::disk::disk_manager::DiskManager;
use storage::disk::file_system::FsDiskManager;
use storage::heap::heap_page::HeapPage;
use storage::query::parser::parse_sql;
use storage::query::planner::QueryPlanner;
use storage::query::executor::{QueryExecutor, QueryResult};

#[derive(Parser)]
#[command(name = "boxsqld")]
#[command(about = "BoxSQL Database Server and CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    
    /// Database data directory
    #[arg(short, long, default_value = "./data")]
    data_dir: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a single SQL statement
    Exec {
        /// SQL statement to execute
        sql: String,
    },
    /// Start interactive SQL shell
    Shell,
    /// Initialize sample data for testing
    InitData,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    let data_dir = std::env::var("BOXSQLD_DATA").unwrap_or(cli.data_dir);
    
    match cli.command {
        Some(Commands::Exec { sql }) => {
            execute_sql(&sql, &data_dir)?;
        }
        Some(Commands::Shell) => {
            start_interactive_shell(&data_dir)?;
        }
        Some(Commands::InitData) => {
            initialize_sample_data(&data_dir)?;
        }
        None => {
            // Default to interactive shell
            start_interactive_shell(&data_dir)?;
        }
    }
    
    Ok(())
}

fn execute_sql(sql: &str, data_dir: &str) -> anyhow::Result<()> {
    let mut dm = FsDiskManager::new(data_dir)?;
    
    // Parse the SQL
    let stmt = parse_sql(sql)?;
    
    // Plan the query
    let planner = QueryPlanner::new();
    let plan = planner.plan(&stmt)?;
    
    // Execute the query
    let executor = QueryExecutor::new();
    let result = executor.execute(plan, &mut dm)?;
    
    // Display results
    display_result(&result);
    
    Ok(())
}

fn start_interactive_shell(data_dir: &str) -> anyhow::Result<()> {
    println!("BoxSQL Interactive Shell");
    println!("Type 'help' for help, 'exit' or 'quit' to quit");
    println!("Data directory: {}\n", data_dir);
    
    let mut rl = DefaultEditor::new()?;
    
    loop {
        let readline = rl.readline("boxsql> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                
                if line.is_empty() {
                    continue;
                }
                
                rl.add_history_entry(line)?;
                
                match line.to_lowercase().as_str() {
                    "exit" | "quit" => {
                        println!("Goodbye!");
                        break;
                    }
                    "help" => {
                        print_help();
                    }
                    "clear" | "cls" => {
                        clear_terminal();
                    }
                    _ => {
                        if let Err(e) = execute_sql(line, data_dir) {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}

fn initialize_sample_data(data_dir: &str) -> anyhow::Result<()> {
    println!("Initializing sample data in {}...", data_dir);
    
    // Remove existing database file to start fresh
    let db_file_path = std::path::Path::new(data_dir).join("base_1.db");
    if db_file_path.exists() {
        std::fs::remove_file(&db_file_path)?;
        println!("Removed existing database file");
    }
    
    let mut dm = FsDiskManager::new(data_dir)?;
    
    // Create a page with test users (matching the query demo)
    let pid = dm.allocate_page(1)?;
    let mut hp = HeapPage::new_empty(pid);
    
    let sample_users = [
        (1i32, "Alice"),
        (2i32, "Bob"),
        (3i32, "Charlie"),
        (4i32, "Diana"),
        (5i32, "Eve"),
        (6i32, "Frank"),
        (7i32, "Grace"),
        (8i32, "Henry"),
        (9i32, "Iris"),
        (10i32, "Jack"),
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
    println!("✓ Created {} user records in page {:?}", sample_users.len(), pid);
    println!("Sample data initialized successfully!");
    
    println!("\nTry these queries:");
    println!("  SELECT * FROM users");
    println!("  SELECT * FROM users LIMIT 3");
    println!("  SELECT * FROM users WHERE id = 5");
    
    Ok(())
}

fn display_result(result: &QueryResult) -> () {
    if result.rows.is_empty() {
        println!("(no rows)");
        return;
    }
    
    // Get column headers
    let headers: Vec<String> = result.schema.columns.iter()
        .map(|col| col.name.clone())
        .collect();
    
    // Convert all data to strings
    let data_rows: Vec<Vec<String>> = result.rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    storage::query::types::Value::Integer(i) => i.to_string(),
                    storage::query::types::Value::Varchar(s) => s.clone(),
                    storage::query::types::Value::Boolean(b) => b.to_string(),
                    storage::query::types::Value::Null => "NULL".to_string(),
                })
                .collect()
        })
        .collect();
    
    // Calculate maximum width for each column
    let mut column_widths: Vec<usize> = headers.iter()
        .map(|h| h.len())
        .collect();
    
    for row in &data_rows {
        for (i, cell) in row.iter().enumerate() {
            column_widths[i] = column_widths[i].max(cell.len());
        }
    }
    
    // Print headers with proper padding
    let padded_headers: Vec<String> = headers.iter()
        .zip(&column_widths)
        .map(|(header, &width)| format!("{:<width$}", header, width = width))
        .collect();
    println!("{}", padded_headers.join(" | "));
    
    // Print separator line
    let separator: Vec<String> = column_widths.iter()
        .map(|&width| "-".repeat(width))
        .collect();
    println!("{}", separator.join("-|-"));
    
    // Print data rows with proper padding
    for row in &data_rows {
        let padded_row: Vec<String> = row.iter()
            .zip(&column_widths)
            .map(|(cell, &width)| format!("{:<width$}", cell, width = width))
            .collect();
        println!("{}", padded_row.join(" | "));
    }
    
    println!("\n({} rows)", result.rows.len());
}

fn clear_terminal() {
    // Use ANSI escape sequence to clear screen and move cursor to top
    print!("\x1B[2J\x1B[1;1H");
    // Flush stdout to make the clear happen immediately
    Write::flush(&mut std::io::stdout()).unwrap();
}

fn print_help() {
    println!("BoxSQL Help:");
    println!("  SQL Commands:");
    println!("    SELECT * FROM users          - Select all users");
    println!("    SELECT * FROM users LIMIT 5  - Select first 5 users");
    println!("    SELECT * FROM users WHERE id = 3  - Select user with id 3");
    println!();
    println!("  Shell Commands:");
    println!("    help    - Show this help");
    println!("    clear   - Clear the terminal screen");
    println!("    cls     - Clear the terminal screen");
    println!("    exit    - Exit the shell");
    println!("    quit    - Exit the shell");
}
