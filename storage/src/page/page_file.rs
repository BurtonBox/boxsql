use crate::page::{
    constants::PAGE_SIZE,
    page_header::PageHeader,
    page_id::{PageFlags, PageId},
};
use crc32fast::Hasher;

/// A database page containing an 8KB buffer with header and data.
///
/// The Page struct represents a single page in the database storage system.
/// Each page has a fixed size (PAGE_SIZE = 8192 bytes) and contains:
/// - A 32-byte header with metadata
/// - Variable data area managed by higher-level structures (e.g., HeapPage)
///
/// The page layout uses little-endian byte order for cross-platform compatibility
/// and includes CRC32 checksums for data integrity verification.
#[derive(Clone, Debug)]
pub struct Page {
    /// Raw page buffer containing header and data
    pub buf: [u8; PAGE_SIZE],
}

impl Page {
    /// Creates a new empty page with the specified ID and type.
    ///
    /// The page is initialized with:
    /// - Zero-filled buffer
    /// - Proper header with the given PageId and flags
    /// - Valid checksum
    ///
    /// # Arguments
    /// * `pid` - Unique identifier for this page
    /// * `flags` - Page type (heap, index, meta)
    ///
    /// # Returns
    /// A new Page ready for use
    pub fn new(pid: PageId, flags: PageFlags) -> Self {
        let mut pg = Self {
            buf: [0u8; PAGE_SIZE],
        };
        let header = PageHeader::new(pid, flags);
        pg.write_header(&header);
        pg.recompute_checksum();
        pg
    }

    /// Reads and parses the page header from the buffer.
    ///
    /// Extracts the 32-byte header from the beginning of the page buffer,
    /// converting from little-endian byte representation to native types.
    ///
    /// # Returns
    /// PageHeader struct with all metadata fields
    pub fn header(&self) -> PageHeader {
        let mut hdr = PageHeader {
            checksum: u32::from_le_bytes(self.buf[0..4].try_into().unwrap()),
            page_id: u64::from_le_bytes(self.buf[4..12].try_into().unwrap()),
            page_lsn: u64::from_le_bytes(self.buf[12..20].try_into().unwrap()),
            page_flags: u16::from_le_bytes(self.buf[20..22].try_into().unwrap()),
            lower: u16::from_le_bytes(self.buf[22..24].try_into().unwrap()),
            upper: u16::from_le_bytes(self.buf[24..26].try_into().unwrap()),
            reserved: [0u8; 6],
        };
        hdr.reserved.copy_from_slice(&self.buf[26..32]);
        hdr
    }

    /// Writes the page header to the buffer.
    ///
    /// Serializes the PageHeader struct to the first 32 bytes of the page buffer
    /// using little-endian byte order for cross-platform compatibility.
    ///
    /// # Arguments
    /// * `hdr` - The PageHeader to write
    pub fn write_header(&mut self, hdr: &PageHeader) {
        self.buf[0..4].copy_from_slice(&hdr.checksum.to_le_bytes());
        self.buf[4..12].copy_from_slice(&hdr.page_id.to_le_bytes());
        self.buf[12..20].copy_from_slice(&hdr.page_lsn.to_le_bytes());
        self.buf[20..22].copy_from_slice(&hdr.page_flags.to_le_bytes());
        self.buf[22..24].copy_from_slice(&hdr.lower.to_le_bytes());
        self.buf[24..26].copy_from_slice(&hdr.upper.to_le_bytes());
        self.buf[26..32].copy_from_slice(&hdr.reserved);
    }

    /// Updates the LSN (Log Sequence Number) in the page header.
    ///
    /// This is used by the write-ahead logging system to track
    /// the last log record that modified this page.
    ///
    /// # Arguments
    /// * `lsn` - The new log sequence number
    pub fn set_lsn(&mut self, lsn: u64) {
        self.buf[12..20].copy_from_slice(&lsn.to_le_bytes());
    }

    /// Updates the lower pointer in the page header.
    ///
    /// The lower pointer marks the end of allocated data
    /// (data grows upward from the header).
    ///
    /// # Arguments
    /// * `lower` - New lower boundary offset
    pub fn set_lower(&mut self, lower: u16) {
        self.buf[22..24].copy_from_slice(&lower.to_le_bytes());
    }

    /// Updates the upper pointer in the page header.
    ///
    /// The upper pointer marks the start of free space
    /// (slots grow downward from the page end).
    ///
    /// # Arguments
    /// * `upper` - New upper boundary offset
    pub fn set_upper(&mut self, upper: u16) {
        self.buf[24..26].copy_from_slice(&upper.to_le_bytes());
    }

    /// Calculates the amount of free space available on this page.
    ///
    /// Free space is the gap between allocated data (growing up from header)
    /// and the slot directory (growing down from page end).
    ///
    /// # Returns
    /// Number of free bytes available for new data and slots
    pub fn free_space(&self) -> usize {
        let hdr = self.header();
        (hdr.upper - hdr.lower) as usize
    }

    /// Extracts the PageId from the page header.
    ///
    /// # Returns
    /// The unique identifier for this page
    pub fn page_id(&self) -> PageId {
        PageId(u64::from_le_bytes(self.buf[4..12].try_into().unwrap()))
    }

    /// Verifies the page's CRC32 checksum for corruption detection.
    ///
    /// Computes a CRC32 checksum over the entire page except the checksum field
    /// itself, then compares with the stored checksum.
    ///
    /// # Returns
    /// true if the checksum is valid, false if corruption is detected
    pub fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.buf[4..]); // skip checksum field
        let sum = hasher.finalize();
        sum == u32::from_le_bytes(self.buf[0..4].try_into().unwrap())
    }

    /// Recomputes and updates the page's CRC32 checksum.
    ///
    /// Should be called after any modification to the page content.
    /// The checksum is computed over the entire page except the
    /// checksum field itself.
    pub fn recompute_checksum(&mut self) {
        // zero checksum field then compute over [4..)
        self.buf[0..4].fill(0);
        let mut hasher = Hasher::new();
        hasher.update(&self.buf[4..]);
        let sum = hasher.finalize();
        self.buf[0..4].copy_from_slice(&sum.to_le_bytes());
    }

    /// Reads a 16-bit unsigned integer from the specified offset.
    ///
    /// Uses little-endian byte order for cross-platform compatibility.
    ///
    /// # Arguments
    /// * `offset` - Byte offset within the page buffer
    ///
    /// # Returns
    /// The u16 value at the specified offset
    pub fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.buf[offset..offset + 2].try_into().unwrap())
    }

    /// Writes a 16-bit unsigned integer to the specified offset.
    ///
    /// Uses little-endian byte order for cross-platform compatibility.
    ///
    /// # Arguments
    /// * `offset` - Byte offset within the page buffer
    /// * `value` - The u16 value to write
    pub fn write_u16(&mut self, offset: usize, value: u16) {
        self.buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests Page creation and basic initialization.
    ///
    /// This test verifies:
    /// - Page::new() creates a page with correct size
    /// - PageId and flags are properly stored in the header
    /// - Initial checksum is computed correctly
    /// - Buffer is properly initialized
    #[test]
    fn page_creation() {
        let pid = PageId::new(3, 456);
        let pg = Page::new(pid, PageFlags::Heap);

        assert_eq!(pg.buf.len(), PAGE_SIZE);
        assert_eq!(pg.page_id(), pid);

        let hdr = pg.header();
        assert_eq!(hdr.page_id, pid.0);
        assert_eq!(hdr.page_flags, PageFlags::Heap as u16);
        assert!(pg.verify_checksum());
    }

    /// Tests page header reading and writing operations.
    ///
    /// This test verifies:
    /// - Headers can be written to and read from page buffers
    /// - All header fields are preserved during serialization
    /// - Little-endian byte order is maintained
    /// - Reserved fields are handled correctly
    #[test]
    fn page_header_serialization() {
        let pid = PageId::new(1, 100);
        let mut pg = Page::new(pid, PageFlags::Index);

        // Modify header fields
        let mut hdr = pg.header();
        hdr.page_lsn = 12345;
        hdr.lower = 64;
        hdr.upper = 8000;
        hdr.reserved = [1, 2, 3, 4, 5, 6];

        pg.write_header(&hdr);
        let read_hdr = pg.header();

        assert_eq!(read_hdr.page_id, pid.0);
        assert_eq!(read_hdr.page_lsn, 12345);
        assert_eq!(read_hdr.page_flags, PageFlags::Index as u16);
        assert_eq!(read_hdr.lower, 64);
        assert_eq!(read_hdr.upper, 8000);
        assert_eq!(read_hdr.reserved, [1, 2, 3, 4, 5, 6]);
    }

    /// Tests LSN (Log Sequence Number) updates.
    ///
    /// This test verifies:
    /// - LSN can be updated without affecting other header fields
    /// - LSN values are stored in little-endian format
    /// - Multiple LSN updates work correctly
    #[test]
    fn page_lsn_updates() {
        let pid = PageId::new(2, 200);
        let mut pg = Page::new(pid, PageFlags::Meta);

        pg.set_lsn(999);
        assert_eq!(pg.header().page_lsn, 999);

        pg.set_lsn(u64::MAX);
        assert_eq!(pg.header().page_lsn, u64::MAX);

        // Verify other fields unchanged
        assert_eq!(pg.page_id(), pid);
        assert_eq!(pg.header().page_flags, PageFlags::Meta as u16);
    }

    /// Tests space management pointer updates.
    ///
    /// This test verifies:
    /// - lower and upper pointers can be updated independently
    /// - Free space calculation works correctly after pointer updates
    /// - Pointer values are stored properly in little-endian format
    #[test]
    fn page_space_pointers() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        // Initial state: empty page
        let initial_hdr = pg.header();
        assert_eq!(initial_hdr.lower, 32); // after header
        assert_eq!(initial_hdr.upper, 8192); // end of page
        assert_eq!(pg.free_space(), 8160); // 8192 - 32

        // Simulate data allocation
        pg.set_lower(100); // allocated 68 bytes of data
        pg.set_upper(8000); // allocated 192 bytes of slots

        assert_eq!(pg.header().lower, 100);
        assert_eq!(pg.header().upper, 8000);
        assert_eq!(pg.free_space(), 7900); // 8000 - 100
    }

    /// Tests free space calculation edge cases.
    ///
    /// This test verifies:
    /// - Free space calculation handles boundary conditions
    /// - Zero free space is reported when pointers meet
    /// - Large free space values are handled correctly
    #[test]
    fn page_free_space_edge_cases() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        // Test zero free space (pointers meet)
        pg.set_lower(4000);
        pg.set_upper(4000);
        assert_eq!(pg.free_space(), 0);

        // Test maximum free space (empty page)
        pg.set_lower(32);
        pg.set_upper(8192);
        assert_eq!(pg.free_space(), 8160);
    }

    /// Tests 16-bit integer read/write operations.
    ///
    /// This test verifies:
    /// - u16 values can be written to and read from arbitrary offsets
    /// - Little-endian byte order is maintained
    /// - Boundary values (0, max) are handled correctly
    #[test]
    fn page_u16_operations() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        // Test various offsets and values
        let test_cases = [
            (100, 0u16),
            (200, 65535u16),
            (300, 12345u16),
            (400, 256u16), // tests byte boundary
        ];

        for (offset, value) in test_cases {
            pg.write_u16(offset, value);
            assert_eq!(pg.read_u16(offset), value);
        }
    }

    /// Tests checksum verification and recomputation.
    ///
    /// This test verifies:
    /// - Initial checksum is valid after page creation
    /// - Checksum detection works when data is modified
    /// - Recomputing checksum fixes corrupted checksums
    /// - Checksum verification is reliable for corruption detection
    #[test]
    fn page_checksum_verification() {
        let pid = PageId::new(5, 500);
        let mut pg = Page::new(pid, PageFlags::Heap);

        // Initial page should have valid checksum
        assert!(pg.verify_checksum());

        // Corrupt the page data (not checksum)
        pg.buf[100] = 0xFF;
        assert!(!pg.verify_checksum());

        // Fix checksum
        pg.recompute_checksum();
        assert!(pg.verify_checksum());

        // Corrupt checksum field itself
        pg.buf[0] = 0x00;
        assert!(!pg.verify_checksum());
    }

    /// Tests page cloning functionality.
    ///
    /// This test verifies:
    /// - Pages can be cloned without affecting the original
    /// - Cloned pages have identical content
    /// - Modifications to clones don't affect the original
    #[test]
    fn page_cloning() {
        let pid = PageId::new(7, 700);
        let mut original_pg = Page::new(pid, PageFlags::Index);

        // Modify original page
        original_pg.set_lsn(999);
        original_pg.buf[100] = 42;
        original_pg.recompute_checksum();

        // Clone and verify it's identical
        let cloned_pg = original_pg.clone();
        assert_eq!(cloned_pg.buf, original_pg.buf);
        assert_eq!(cloned_pg.page_id(), original_pg.page_id());
        assert_eq!(cloned_pg.header().page_lsn, 999);
        assert_eq!(cloned_pg.buf[100], 42);

        // Modify clone, verify original unchanged
        let mut modified_clone = cloned_pg;
        modified_clone.buf[200] = 99;
        assert_eq!(original_pg.buf[200], 0);
        assert_eq!(modified_clone.buf[200], 99);
    }
}
