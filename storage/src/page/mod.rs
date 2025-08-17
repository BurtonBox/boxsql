//! Page management layer providing core page abstractions.
//! 
//! This module implements the fundamental page structure used throughout
//! the database storage system. Pages are fixed-size (8KB) units that
//! contain a header with metadata and a data area for content.
//! 
//! # Components
//! 
//! - `constants`: Core constants like PAGE_SIZE
//! - `page_id`: Unique page identifiers and type flags
//! - `page_header`: Page metadata structure (32 bytes)
//! - `page_file`: Main page implementation with buffer and operations
//! 
//! # Page Layout
//! 
//! ```text
//! +------------------+
//! | Header (32 bytes)|
//! +------------------+
//! |                  |
//! |   Data Area      |
//! |   (8160 bytes)   |
//! |                  |
//! +------------------+
//! ```
//! 
//! The header contains essential metadata including checksums, page IDs,
//! LSN for recovery, and space management pointers (lower/upper).

pub mod constants;
pub mod page_id;
pub mod page_header;
pub mod page_file;