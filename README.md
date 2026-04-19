# BoxSQL Database Management System

### Rust-DB: Learning Rust by Building a Database

This repository is a learning project to dive deep into **Rust** by constructing a database system from scratch, inspired by the design of **PostgreSQL**.  
The goal is not just to recreate Postgres, but to gain practical experience with:

- ⚡ **Rust fundamentals** (ownership, lifetimes, traits, error handling, concurrency)
- 🗄️ **Database internals** (storage engines, indexes, WAL, transactions)
- 📊 **Query parsing & execution** (SQL subset, execution plans)
- 🔒 **Concurrency control** (MVCC, locks, isolation levels)
- 🏗️ **Systems-level engineering** (files, memory management, performance tuning)

## Why This Project?
Rust is a language well-suited for **high-performance, safe systems programming**. Databases are among the most challenging systems to build — they require careful handling of memory, disk, concurrency, and correctness. By combining the two, this project provides a deep, real-world way to master Rust.

## Roadmap
1. Minimal key-value store
2. Page-based storage layer
3. Write-ahead logging (WAL) for crash recovery
4. B-Tree indexes
5. SQL parser & execution engine
6. Transactions & concurrency (MVCC)
7. Optimizations & Postgres-like features

## 🛠️ Installation

### Prerequisites
- Rust 2024 edition or later
- Cargo package manager

### Build

```bash
# Build all workspace members
cargo build

# Build a specific crate
cargo build -p storage
cargo build -p boxsqld
```

## 🚀 Running

```bash
# Run the database daemon (data stored in ./data by default)
cargo run -p boxsqld

# Run with a custom data directory
BOXSQLD_DATA=/path/to/data cargo run -p boxsqld
```

The daemon creates heap pages, inserts test tuples, and writes them to disk in the configured data directory.

## 🧪 Testing

```bash
# Run all tests
cargo test

# Test a specific crate
cargo test -p storage
```

## 🔍 Code Quality

```bash
cargo clippy    # Lint
cargo fmt       # Format
cargo check     # Fast compilation check
```

## ⚖️ License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by PostgreSQL's storage architecture
---

✨ Whether you’re learning Rust or curious about how databases work under the hood, this repo is for you.
