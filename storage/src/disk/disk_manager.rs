use crate::page::page_file::Page;
use crate::page::page_id::PageId;

/// Trait defining the interface for disk-based page storage.
/// 
/// DiskManager provides an abstraction layer for page storage operations,
/// allowing different storage backends (filesystem, memory-mapped files,
/// network storage, etc.) to be used interchangeably.
/// 
/// The trait ensures ACID properties by:
/// - Providing atomic page operations
/// - Supporting explicit synchronization for durability
/// - Enabling implementations to handle consistency and isolation
/// 
/// All implementations must handle:
/// - Page allocation with unique identifiers
/// - Reliable page read/write operations
/// - Data integrity verification (checksums)
/// - Proper error handling and recovery
pub trait DiskManager {
	/// Allocates a new page in the specified file.
	/// 
	/// Creates a new page within the given file and returns its unique identifier.
	/// The implementation should ensure the page is properly initialized and
	/// accessible for subsequent read/write operations.
	/// 
	/// # Arguments
	/// * `file_id` - The file where the page should be allocated
	/// 
	/// # Returns
	/// A unique PageId for the newly allocated page
	/// 
	/// # Errors
	/// Returns an error if page allocation fails (disk full, I/O error, etc.)
	fn allocate_page(&mut self, file_id: u32) -> anyhow::Result<PageId>;
	
	/// Reads a page from disk by its identifier.
	/// 
	/// Retrieves the page data and verifies its integrity using checksums.
	/// The implementation must handle corruption detection and return
	/// appropriate errors for corrupted or missing pages.
	/// 
	/// # Arguments
	/// * `pid` - The unique identifier of the page to read
	/// 
	/// # Returns
	/// The page data if successfully read and verified
	/// 
	/// # Errors
	/// Returns an error if the page cannot be read or fails checksum verification
	fn read_page(&self, pid: PageId) -> anyhow::Result<Page>;
	
	/// Writes a page to disk.
	/// 
	/// Persists the page data to storage at the location specified by the
	/// page's identifier. The implementation should ensure the write is
	/// atomic and handle partial write scenarios appropriately.
	/// 
	/// # Arguments
	/// * `page` - The page to write (contains both data and identifier)
	/// 
	/// # Returns
	/// Ok(()) if the page was successfully written
	/// 
	/// # Errors
	/// Returns an error if the write operation fails
	fn write_page(&mut self, page: &Page) -> anyhow::Result<()>;
	
	/// Synchronizes all pending writes to durable storage.
	/// 
	/// Forces all buffered writes to be committed to persistent storage,
	/// ensuring durability guarantees. This operation may be expensive
	/// and should be used strategically (e.g., at transaction boundaries).
	/// 
	/// # Returns
	/// Ok(()) if synchronization completed successfully
	/// 
	/// # Errors
	/// Returns an error if the sync operation fails
	fn sync(&mut self) -> anyhow::Result<()>;
}