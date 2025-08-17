//! Heap page implementation for variable-length tuple storage.
//! 
//! This module provides heap pages that store tuples using a slot directory
//! approach. This design allows for efficient variable-length tuple storage
//! with support for deletion and compaction.
//! 
//! # Components
//! 
//! - `heap_page`: Main heap page implementation with tuple operations
//! - `slot`: Slot directory entries pointing to tuple locations
//! 
//! # Heap Page Layout
//! 
//! ```text
//! +------------------+
//! | Page Header      | <- 32 bytes
//! +------------------+
//! | Tuple Data       | <- Grows upward from 'lower'
//! | (variable)       |
//! +------------------+
//! | Free Space       | <- Between 'lower' and 'upper'
//! +------------------+
//! | Slot Directory   | <- Grows downward from 'upper'
//! | (4 bytes/slot)   |
//! +------------------+
//! ```
//! 
//! Each slot contains an offset and length pointing to tuple data.
//! Deleted tuples are marked as tombstones (length = 0) and can be
//! reclaimed through compaction.

pub mod heap_page;
pub mod slot;