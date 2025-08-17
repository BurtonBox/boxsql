use std::fmt;

/// Page type identifiers used to distinguish different kinds of pages.
/// 
/// The values are powers of 2 to allow bitwise combinations in the future
/// if needed (e.g., a page that's both heap and has special properties).
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageFlags {
	/// Standard heap page containing tuple data
	Heap = 1,
	/// Index page (e.g., B+ tree node) - for future implementation
	Index = 2,
	/// Metadata page containing system information
	Meta = 4,
}

/// Unique identifier for a database page.
/// 
/// A PageId is a 64-bit value that encodes both the file ID and page number:
/// - Upper 32 bits: file_id (supports up to 4 billion files)
/// - Lower 32 bits: page_no (supports up to 4 billion pages per file)
/// 
/// This design allows for:
/// - Efficient storage and comparison (single u64)
/// - Massive scale (32TB per file with 8KB pages)
/// - Easy serialization/deserialization
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct PageId(pub u64);

impl PageId {
	/// Creates a new PageId from file ID and page number.
	/// 
	/// # Arguments
	/// * `file_id` - The file containing this page (0 to 2^32-1)
	/// * `page_no` - The page number within the file (0 to 2^32-1)
	/// 
	/// # Example
	/// ```
	/// use storage::page::page_id::PageId;
	/// let pg_id = PageId::new(1, 42);
	/// assert_eq!(pg_id.file_id(), 1);
	/// assert_eq!(pg_id.page_no(), 42);
	/// ```
	pub fn new(file_id: u32, page_no: u32) -> Self {
		Self(((file_id as u64) << 32) | (page_no as u64))
	}

	/// Extracts the file ID from this PageId.
	/// 
	/// # Returns
	/// The file ID (upper 32 bits)
	pub fn file_id(self) -> u32 {
		(self.0 >> 32) as u32
	}

	/// Extracts the page number from this PageId.
	/// 
	/// # Returns
	/// The page number within the file (lower 32 bits)
	pub fn page_no(self) -> u32 {
		(self.0 & 0xFFFF_FFFF) as u32
	}
}

impl fmt::Debug for PageId {
	/// Custom debug formatting showing file_id:page_no for readability.
	/// 
	/// # Example
	/// ```
	/// use storage::page::page_id::PageId;
	/// let pg_id = PageId::new(1, 42);
	/// assert_eq!(format!("{:?}", pg_id), "PageId(1:42)");
	/// ```
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "PageId({}:{})", self.file_id(), self.page_no())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Tests basic PageId creation and field extraction.
	/// 
	/// This test verifies:
	/// - PageId correctly encodes file_id and page_no into a single u64
	/// - file_id() extracts the correct file identifier
	/// - page_no() extracts the correct page number
	/// - The encoding supports the full 32-bit range for both fields
	#[test]
	fn page_id_creation_and_extraction() {
		let pid = PageId::new(42, 1337);
		assert_eq!(pid.file_id(), 42);
		assert_eq!(pid.page_no(), 1337);

		// Test boundary values
		let max_pid = PageId::new(u32::MAX, u32::MAX);
		assert_eq!(max_pid.file_id(), u32::MAX);
		assert_eq!(max_pid.page_no(), u32::MAX);

		let min_pid = PageId::new(0, 0);
		assert_eq!(min_pid.file_id(), 0);
		assert_eq!(min_pid.page_no(), 0);
	}

	/// Tests PageId ordering and comparison operations.
	/// 
	/// This test verifies:
	/// - PageIds are ordered first by file_id, then by page_no
	/// - Equality works correctly for identical PageIds
	/// - Ordering is consistent for use in collections like BTreeMap
	#[test]
	fn page_id_ordering() {
		let pid1 = PageId::new(1, 100);
		let pid2 = PageId::new(1, 200);
		let pid3 = PageId::new(2, 50);

		assert!(pid1 < pid2);
		assert!(pid2 < pid3);
		assert!(pid1 < pid3);
		assert_eq!(pid1, PageId::new(1, 100));
	}

	/// Tests PageId debug formatting.
	/// 
	/// This test verifies:
	/// - Debug output uses the readable "PageId(file:page)" format
	/// - Format is consistent and parseable by humans
	#[test]
	fn page_id_debug_format() {
		let pid = PageId::new(5, 123);
		assert_eq!(format!("{:?}", pid), "PageId(5:123)");
	}

	/// Tests PageId serialization and hashing properties.
	/// 
	/// This test verifies:
	/// - PageIds can be used as hash map keys
	/// - Hash values are consistent for equal PageIds
	/// - Different PageIds produce different hash values (probabilistically)
	#[test]
	fn page_id_hash_and_collections() {
		use std::collections::HashMap;
		
		let mut map = HashMap::new();
		let pid1 = PageId::new(1, 100);
		let pid2 = PageId::new(2, 200);
		
		map.insert(pid1, "value1");
		map.insert(pid2, "value2");
		
		assert_eq!(map.get(&pid1), Some(&"value1"));
		assert_eq!(map.get(&pid2), Some(&"value2"));
		assert_eq!(map.len(), 2);
	}

	/// Tests PageFlags enum properties.
	/// 
	/// This test verifies:
	/// - PageFlags values are powers of 2 for future bitwise operations
	/// - Each flag has the expected numeric value
	/// - Flags can be compared for equality
	#[test]
	fn page_flags_values() {
		assert_eq!(PageFlags::Heap as u16, 1);
		assert_eq!(PageFlags::Index as u16, 2);
		assert_eq!(PageFlags::Meta as u16, 4);
		
		// Verify they're powers of 2 (useful for bitwise combinations)
		assert_eq!((PageFlags::Heap as u16).count_ones(), 1);
		assert_eq!((PageFlags::Index as u16).count_ones(), 1);
		assert_eq!((PageFlags::Meta as u16).count_ones(), 1);
	}
}