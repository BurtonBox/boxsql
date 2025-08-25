//! BoxSQL Storage Engine
//! 
//! A database storage engine implementing page-based storage with heap pages
//! and disk management. This crate provides the fundamental building blocks
//! for database storage operations.
//! 
//! # Architecture
//! 
//! The storage engine is organized into three main layers:
//! 
//! - **Page Layer** (`page`): Core page abstraction with 8KB pages, headers, and file management
//! - **Heap Layer** (`heap`): Heap page implementation with tuple storage using slot directories
//! - **Disk Layer** (`disk`): Disk management with trait-based design and filesystem implementation
//! 
//! # Key Features
//! 
//! - 8KB page size optimized for performance and memory usage
//! - CRC32 checksums for data integrity verification
//! - Variable-length tuple storage with slot directories
//! - Tombstone-based deletion with compaction support
//! - Trait-based disk management for extensibility
//! - Cross-platform compatibility with little-endian storage
//! 
//! # Usage
//! 
//! ```rust
//! use storage::disk::disk_manager::DiskManager;
//! use storage::disk::file_system::FsDiskManager;
//! use storage::heap::heap_page::HeapPage;
//!
//! // Create disk manager and allocate a page
//! let mut dm = FsDiskManager::new("./data")?;
//! let pid = dm.allocate_page(1)?;
//!
//! // Create heap page and insert tuples
//! let mut hp = HeapPage::new_empty(pid);
//! let slot_no = hp.insert_tuple(b"hello world")?;
//!
//! // Write to disk and read back
//! dm.write_page(&hp.page)?;
//! let pg = dm.read_page(pid)?;
//! let hp2 = HeapPage { page: pg };
//! let data = hp2.read_tuple(slot_no).unwrap();
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod page;
pub mod heap;
pub mod disk;
pub mod query;