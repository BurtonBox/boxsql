# BoxSQL

A training database storage engine written in Rust, implementing page-based storage with heap pages and robust disk management.

## 🏗️ Architecture

BoxSQL is organized into three main layers:

### 📄 Page Layer (`storage/src/page/`)
- **8KB Pages**: Standard page size for optimal I/O and memory efficiency
- **Page Headers**: 32-byte headers with metadata, checksums, and space management
- **Page IDs**: 64-bit identifiers supporting massive scale (4B files × 4B pages)

### 🗃️ Heap Layer (`storage/src/heap/`)
- **Variable-Length Tuples**: Efficient storage of records of any size
- **Slot Directory**: Indirection layer enabling tuple deletion without data movement
- **Compaction**: Reclaim space from deleted tuples while preserving slot references

### 💾 Disk Layer (`storage/src/disk/`)
- **Trait-Based Design**: Pluggable storage backends (filesystem, memory, network)
- **File Management**: Automatic file creation and organization
- **Error Handling**: Robust error propagation and recovery

## 📦 Project Structure

```
boxsql/
├── storage/           # Core storage engine library
│   ├── src/
│   │   ├── page/     # Page management and headers
│   │   ├── heap/     # Heap pages and tuple storage
│   │   └── disk/     # Disk I/O and file management
│   └── tests/        # Integration tests
├── boxsqld/          # Database daemon
└── README.md         # This file
```

## 🛠️ Installation

### Prerequisites
- Rust 2024 edition or later
- Cargo package manager

### Building from Source

```bash
# Clone the repository
git clone https://github.com/burtonbox/boxsql.git
cd boxsql

# Build the entire workspace
cargo build --release

# Run tests
cargo test

# Run the database daemon
cargo run -p boxsqld
```

## 🚦 Quick Start

### Using the Storage Engine Library

```rust
use storage::disk::disk_manager::DiskManager;
use storage::disk::file_system::FsDiskManager;
use storage::heap::heap_page::HeapPage;

fn main() -> anyhow::Result<()> {
    // Create disk manager and allocate a page
    let mut dm = FsDiskManager::new("./data")?;
    let pid = dm.allocate_page(1)?;

    // Create heap page and insert tuples
    let mut hp = HeapPage::new_empty(pid);
    let slot_no = hp.insert_tuple(b"hello world")?;

    // Write to disk and read back
    dm.write_page(&hp.page)?;
    let pg = dm.read_page(pid)?;
    let hp2 = HeapPage { page: pg };
    let data = hp2.read_tuple(slot_no).unwrap();
    
    println!("Retrieved: {:?}", std::str::from_utf8(data)?);
    Ok(())
}
```

### Running the Database Daemon

```bash
# Use default data directory (./data)
cargo run -p boxsqld

# Use custom data directory
BOXSQLD_DATA=/path/to/data cargo run -p boxsqld
```

## 🧪 Testing

BoxSQL includes comprehensive testing at multiple levels:

```bash
# Run all tests
cargo test

# Run only storage engine tests
cargo test -p storage

# Run integration tests
cargo test --test integration_tests

# Run with output
cargo test -- --nocapture
```

### Test Coverage
- **41 Unit Tests**: Individual component verification
- **8 Integration Tests**: End-to-end workflow validation
- **3 Doc Tests**: Example code verification
- **Edge Cases**: Boundary conditions, error handling, corruption detection

## 📊 Performance Characteristics

- **Page Size**: 8KB (optimal for modern storage systems)
- **Scalability**: Supports up to 4 billion files with 4 billion pages each
- **Storage Efficiency**: Minimal overhead with 32-byte page headers
- **Integrity**: CRC32 checksums on every page with corruption detection

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOXSQLD_DATA` | `./data` | Directory for database files |

### File Layout

```
data/
├── base_1.db         # File ID 1 containing pages
├── base_2.db         # File ID 2 containing pages
└── ...
```

## 📋 Roadmap

- [ ] **Query Engine**: SQL parsing and execution
- [ ] **Indexing**: B+ tree implementation for efficient queries
- [ ] **Transactions**: ACID compliance with concurrency control
- [ ] **WAL**: Write-ahead logging for durability and recovery
- [ ] **Networking**: Client-server protocol and connection management
- [ ] **Optimization**: Query optimization and execution planning

## 📖 Documentation

- **API Documentation**: Run `cargo doc --open`

## ⚖️ License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with ❤️ in Rust
- Inspired by PostgreSQL's storage architecture
- Developed with comprehensive testing and documentation practices

---

**BoxSQL** - Building to understand database storage, one page at a time.