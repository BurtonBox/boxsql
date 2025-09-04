//! Disk management layer providing persistent storage abstractions.
//!
//! This module defines the interface for disk-based page storage and
//! provides implementations for different storage backends. The trait-based
//! design allows for pluggable storage systems.
//!
//! # Components
//!
//! - `disk_manager`: Trait defining the disk storage interface
//! - `file_system`: Filesystem-based implementation using regular files
//!
//! # Storage Model
//!
//! Pages are organized into files identified by file_id. Each file contains
//! pages stored sequentially at fixed offsets (page_no * PAGE_SIZE).
//!
//! ```text
//! File Layout (base_1.db):
//! +----------+----------+----------+-----+
//! | Page 0   | Page 1   | Page 2   | ... |
//! | 8KB      | 8KB      | 8KB      |     |
//! +----------+----------+----------+-----+
//! ```
//!
//! The filesystem implementation provides:
//! - Automatic file and directory creation
//! - Checksum verification on reads
//! - Proper error handling and recovery
//! - Cross-platform compatibility

pub mod disk_manager;
pub mod file_system;
