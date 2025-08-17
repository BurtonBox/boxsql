use crate::page::{constants::PAGE_SIZE, page_id::{PageFlags, PageId}};

/// Header structure for database pages (32 bytes).
/// 
/// The PageHeader appears at the beginning of every page and contains
/// essential metadata for the page. The layout is designed to be
/// cross-platform compatible using little-endian byte order.
/// 
/// Layout (32 bytes total):
/// - checksum (4 bytes): CRC32 checksum for corruption detection
/// - page_id (8 bytes): Unique page identifier
/// - page_lsn (8 bytes): Log sequence number for WAL/recovery
/// - page_flags (2 bytes): Page type and status flags
/// - lower (2 bytes): Offset to end of allocated data (grows upward)
/// - upper (2 bytes): Offset to start of free space (grows downward)
/// - reserved (6 bytes): Reserved for future use
#[derive(Clone, Copy, Debug)]
pub struct PageHeader {
	/// CRC32 checksum of the entire page (excluding this field)
	pub checksum: u32,
	/// Unique identifier for this page
	pub page_id: u64,
	/// Log sequence number for write-ahead logging and recovery
	pub page_lsn: u64,
	/// Page type flags (heap, index, meta, etc.)
	pub page_flags: u16,
	/// Offset to end of allocated data (data grows upward from header)
	pub lower: u16,
	/// Offset to start of free space (slots grow downward from page end)
	pub upper: u16,
	/// Reserved space for future extensions
	pub reserved: [u8; 6],
}

impl PageHeader {
	/// Size of the page header in bytes (32 bytes)
	pub const LEN: usize = 32;

	/// Creates a new page header for an empty page.
	/// 
	/// Initializes the header with:
	/// - checksum: 0 (will be computed when page is written)
	/// - page_lsn: 0 (will be set by WAL system)
	/// - lower: Points to end of header (start of data area)
	/// - upper: Points to end of page (start of slot directory)
	/// - reserved: Zeroed for future use
	/// 
	/// # Arguments
	/// * `pid` - The page identifier for this page
	/// * `flags` - The page type (heap, index, meta)
	/// 
	/// # Returns
	/// A new PageHeader ready for use
	pub fn new(pid: PageId, flags: PageFlags) -> Self {
		Self {
			checksum: 0,
			page_id: pid.0,
			page_lsn: 0,
			page_flags: flags as u16,
			lower: Self::LEN as u16,
			upper: PAGE_SIZE as u16,
			reserved: [0u8; 6],
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Tests PageHeader creation and initialization.
	/// 
	/// This test verifies:
	/// - PageHeader::new() creates a properly initialized header
	/// - Header size is exactly 32 bytes as expected
	/// - Initial values are set correctly (checksum=0, lsn=0, etc.)
	/// - lower and upper pointers are positioned correctly for an empty page
	#[test]
	fn page_header_creation() {
		let pid = PageId::new(1, 42);
		let hdr = PageHeader::new(pid, PageFlags::Heap);

		assert_eq!(hdr.checksum, 0);
		assert_eq!(hdr.page_id, pid.0);
		assert_eq!(hdr.page_lsn, 0);
		assert_eq!(hdr.page_flags, PageFlags::Heap as u16);
		assert_eq!(hdr.lower, PageHeader::LEN as u16);
		assert_eq!(hdr.upper, PAGE_SIZE as u16);
		assert_eq!(hdr.reserved, [0u8; 6]);
	}

	/// Tests PageHeader size constraints.
	/// 
	/// This test verifies:
	/// - PageHeader::LEN constant matches the actual struct size
	/// - Header is exactly 32 bytes to maintain page layout expectations
	/// - Size is consistent across platforms
	#[test]
	fn page_header_size() {
		assert_eq!(PageHeader::LEN, 32);
		assert_eq!(std::mem::size_of::<PageHeader>(), 32);
	}

	/// Tests different page flag types in headers.
	/// 
	/// This test verifies:
	/// - All PageFlags variants can be used in headers
	/// - Flag values are preserved correctly in the header
	/// - Headers can be created for different page types
	#[test]
	fn page_header_flags() {
		let pid = PageId::new(1, 0);
		
		let heap_hdr = PageHeader::new(pid, PageFlags::Heap);
		assert_eq!(heap_hdr.page_flags, PageFlags::Heap as u16);
		
		let index_hdr = PageHeader::new(pid, PageFlags::Index);
		assert_eq!(index_hdr.page_flags, PageFlags::Index as u16);
		
		let meta_hdr = PageHeader::new(pid, PageFlags::Meta);
		assert_eq!(meta_hdr.page_flags, PageFlags::Meta as u16);
	}

	/// Tests page space management pointers.
	/// 
	/// This test verifies:
	/// - lower pointer starts after the header
	/// - upper pointer starts at the end of the page
	/// - This creates maximum free space for a new empty page
	/// - Pointers leave room for data growth from both ends
	#[test]
	fn page_header_space_pointers() {
		let pid = PageId::new(1, 0);
		let hdr = PageHeader::new(pid, PageFlags::Heap);

		// lower should point just after the header
		assert_eq!(hdr.lower, 32);
		
		// upper should point to end of page (8192 bytes)
		assert_eq!(hdr.upper, 8192);
		
		// Total free space should be page size minus header
		let free_space = hdr.upper - hdr.lower;
		assert_eq!(free_space, PAGE_SIZE as u16 - PageHeader::LEN as u16);
		assert_eq!(free_space, 8160); // 8192 - 32
	}

	/// Tests PageHeader cloning and copying.
	/// 
	/// This test verifies:
	/// - PageHeader implements Clone and Copy correctly
	/// - Cloned headers have identical values
	/// - Copy semantics work as expected for header manipulation
	#[test]
	fn page_header_clone_copy() {
		let pid = PageId::new(5, 123);
		let original_hdr = PageHeader::new(pid, PageFlags::Index);
		
		let cloned_hdr = original_hdr.clone();
		let copied_hdr = original_hdr;
		
		assert_eq!(original_hdr.page_id, cloned_hdr.page_id);
		assert_eq!(original_hdr.page_flags, cloned_hdr.page_flags);
		assert_eq!(original_hdr.lower, cloned_hdr.lower);
		assert_eq!(original_hdr.upper, cloned_hdr.upper);
		
		assert_eq!(original_hdr.page_id, copied_hdr.page_id);
		assert_eq!(original_hdr.page_flags, copied_hdr.page_flags);
	}
}