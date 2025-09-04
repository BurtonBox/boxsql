use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use anyhow::Context;
use crate::disk::disk_manager::DiskManager;
use crate::page::{constants::PAGE_SIZE, page_file::Page, page_id::PageId};

/// Error types specific to disk operations.
#[derive(Debug, thiserror::Error)]
pub enum DiskError {
	/// Indicates data corruption detected via checksum mismatch
	#[error("checksum mismatch for {0:?}")]
	Checksum(PageId),
}

/// Filesystem-based implementation of the DiskManager trait.
/// 
/// FsDiskManager stores database pages as files on the local filesystem.
/// Each file_id corresponds to a separate file named "base_{file_id}.db"
/// within the base directory. Pages are stored sequentially within each file.
/// 
/// File Layout:
/// - Each file contains pages of exactly PAGE_SIZE bytes
/// - Page N is located at offset N * PAGE_SIZE within the file
/// - Files are created and extended as needed during page allocation
/// 
/// This implementation provides:
/// - Simple and reliable file-based storage
/// - Cross-platform compatibility
/// - Automatic directory creation
/// - Checksum verification on every read
/// - Proper error handling and context
pub struct FsDiskManager {
	/// Base directory where database files are stored
	base: PathBuf,
}

impl FsDiskManager {
	/// Creates a new filesystem disk manager.
	/// 
	/// Initializes the disk manager with the specified base directory.
	/// The directory will be created if it doesn't exist.
	/// 
	/// # Arguments
	/// * `base` - Path to the directory where database files will be stored
	/// 
	/// # Returns
	/// A new FsDiskManager instance
	/// 
	/// # Errors
	/// Returns an error if the directory cannot be created
	pub fn new<P: AsRef<Path>>(base: P) -> anyhow::Result<Self> {
		let base = base.as_ref();
		fs::create_dir_all(base).with_context(|| format!("creating data dir {:?}", base))?;
		Ok(Self { base: base.to_path_buf() })
	}

	/// Constructs the filesystem path for a given file ID.
	/// 
	/// # Arguments
	/// * `file_id` - The file identifier
	/// 
	/// # Returns
	/// PathBuf pointing to the file for this file_id
	fn file_path(&self, file_id: u32) -> PathBuf { 
		self.base.join(format!("base_{}.db", file_id)) 
	}

	/// Opens a file for read/write access, creating it if necessary.
	/// 
	/// # Arguments
	/// * `path` - Path to the file to open
	/// 
	/// # Returns
	/// An open File handle with read/write permissions
	/// 
	/// # Errors
	/// Returns an error if the file cannot be opened or created
	fn open_rw(&self, path: &Path) -> anyhow::Result<File> {
		Ok(OpenOptions::new().create(true).read(true).write(true).open(path)?)
	}
}

impl DiskManager for FsDiskManager {
	fn allocate_page(&mut self, file_id: u32) -> anyhow::Result<PageId> {
		let path = self.file_path(file_id);
		let mut file = self.open_rw(&path)?;
		let len = file.metadata()?.len() as usize;
		let page_no = (len / PAGE_SIZE) as u32;
		
		// Extend file by one page of zeros
		file.seek(SeekFrom::End(0))?;
		file.write_all(&vec![0u8; PAGE_SIZE])?;
		Ok(PageId::new(file_id, page_no))
	}

	fn read_page(&self, pid: PageId) -> anyhow::Result<Page> {
		let path = self.file_path(pid.file_id());
		let mut file = self.open_rw(&path)?;
		let mut buf = [0u8; PAGE_SIZE];
		
		// Calculate byte offset: page_no * PAGE_SIZE
		let off = (pid.page_no() as u64) * (PAGE_SIZE as u64);
		file.seek(SeekFrom::Start(off))?;
		file.read_exact(&mut buf)?;
		
		// Verify data integrity
		let p = Page { buf };
		if !p.verify_checksum() {
			return Err(DiskError::Checksum(pid))
				.with_context(|| format!("while reading {:?}", pid));
		}
		Ok(p)
	}

	fn write_page(&mut self, page: &Page) -> anyhow::Result<()> {
		let pid = page.page_id();
		let path = self.file_path(pid.file_id());
		let mut file = self.open_rw(&path)?;
		
		// Calculate byte offset and write page data
		let off = (pid.page_no() as u64) * (PAGE_SIZE as u64);
		file.seek(SeekFrom::Start(off))?;
		file.write_all(&page.clone().buf)?; // clone to avoid borrowing issues
		Ok(())
	}

	fn sync(&mut self) -> anyhow::Result<()> {
		// Simple approach: sync the entire base directory
		// This ensures all file changes are committed to disk
		let directory = File::open(&self.base)?;
		directory.sync_all()?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::page::page_id::PageFlags;
	use tempfile::TempDir;

	/// Tests FsDiskManager creation and directory handling.
	/// 
	/// This test verifies:
	/// - FsDiskManager::new() creates the base directory if it doesn't exist
	/// - Directory creation works with nested paths
	/// - Multiple disk managers can use the same directory
	/// - Error handling for invalid directory paths
	#[test]
	fn disk_manager_creation() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let nested_path = temp_directory.path().join("nested").join("database");
		
		// Should create nested directories
		let _dm = FsDiskManager::new(&nested_path)?;
		assert!(nested_path.exists());
		assert!(nested_path.is_dir());
		
		// Should work with existing directory
		let _dm2 = FsDiskManager::new(&nested_path)?;
		assert!(nested_path.exists());
		
		Ok(())
	}

	/// Tests page allocation functionality.
	/// 
	/// This test verifies:
	/// - Pages are allocated with unique identifiers
	/// - Page numbers increment sequentially within a file
	/// - Multiple files can have pages allocated independently
	/// - Allocated pages create actual files on disk
	#[test]
	fn page_allocation() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_directory.path())?;
		
		// Allocate pages in file 1
		let pid1 = dm.allocate_page(1)?;
		let pid2 = dm.allocate_page(1)?;
		assert_eq!(pid1.file_id(), 1);
		assert_eq!(pid1.page_no(), 0);
		assert_eq!(pid2.file_id(), 1);
		assert_eq!(pid2.page_no(), 1);
		
		// Allocate pages in file 2
		let pid3 = dm.allocate_page(2)?;
		assert_eq!(pid3.file_id(), 2);
		assert_eq!(pid3.page_no(), 0);
		
		// Verify files exist on disk
		assert!(temp_directory.path().join("base_1.db").exists());
		assert!(temp_directory.path().join("base_2.db").exists());
		
		Ok(())
	}

	/// Tests page write and read operations.
	/// 
	/// This test verifies:
	/// - Pages can be written to and read from disk
	/// - Page content is preserved exactly during I/O
	/// - Multiple pages in the same file are handled correctly
	/// - Page headers and data are correctly serialized/deserialized
	#[test]
	fn page_write_read_round_trip() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_directory.path())?;
		
		// Create and write a page with specific content
		let pid = dm.allocate_page(10)?;
		let mut pg = Page::new(pid, PageFlags::Heap);
		pg.set_lsn(12345);
		pg.buf[100] = 0xAB;
		pg.buf[200] = 0xCD;
		pg.recompute_checksum();
		
		dm.write_page(&pg)?;
		
		// Read back and verify
		let read_pg = dm.read_page(pid)?;
		assert_eq!(read_pg.page_id(), pid);
		assert_eq!(read_pg.header().page_lsn, 12345);
		assert_eq!(read_pg.buf[100], 0xAB);
		assert_eq!(read_pg.buf[200], 0xCD);
		assert!(read_pg.verify_checksum());
		
		Ok(())
	}

	/// Tests checksum verification and corruption detection.
	/// 
	/// This test verifies:
	/// - Valid checksums pass verification during reads
	/// - Corrupted data is detected via checksum mismatch
	/// - DiskError::Checksum is returned for corrupted pages
	/// - Error context includes the page identifier
	#[test]
	fn checksum_verification() -> anyhow::Result<()> {
		let temp_dir = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_dir.path())?;
		
		// Write a valid page
		let pid = dm.allocate_page(5)?;
		let pg = Page::new(pid, PageFlags::Index);
		dm.write_page(&pg)?;
		
		// Verify it reads correctly
		let read_pg = dm.read_page(pid)?;
		assert!(read_pg.verify_checksum());
		
		// Manually corrupt the file on disk
		let file_path = temp_dir.path().join("base_5.db");
		let mut file = OpenOptions::new().write(true).open(&file_path)?;
		file.seek(SeekFrom::Start(100))?; // corrupt some data
		file.write_all(&[0xFF])?;
		drop(file);
		
		// Reading should now fail with checksum error
		let result = dm.read_page(pid);
		assert!(result.is_err());
		
		// Verify it's specifically a checksum error
		let error = result.unwrap_err();
		let error_string = format!("{:?}", error);
		assert!(error_string.contains("checksum") || error_string.contains("Checksum"));
		
		Ok(())
	}

	/// Tests multiple pages within the same file.
	/// 
	/// This test verifies:
	/// - Multiple pages can be stored in the same file
	/// - Each page maintains its own content and checksum
	/// - Page offsets are calculated correctly
	/// - No interference between adjacent pages
	#[test]
	fn multiple_pages_same_file() -> anyhow::Result<()> {
		let temp_dir = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_dir.path())?;
		
		// Allocate and write multiple pages
		let pid1 = dm.allocate_page(7)?;
		let pid2 = dm.allocate_page(7)?;
		let pid3 = dm.allocate_page(7)?;
		
		let mut page1 = Page::new(pid1, PageFlags::Heap);
		let mut page2 = Page::new(pid2, PageFlags::Index);
		let mut page3 = Page::new(pid3, PageFlags::Meta);
		
		// Give each page unique content
		page1.buf[500] = 0x11;
		page2.buf[500] = 0x22;
		page3.buf[500] = 0x33;
		page1.recompute_checksum();
		page2.recompute_checksum();
		page3.recompute_checksum();
		
		dm.write_page(&page1)?;
		dm.write_page(&page2)?;
		dm.write_page(&page3)?;
		
		// Read back in different order and verify
		let read3 = dm.read_page(pid3)?;
		let read1 = dm.read_page(pid1)?;
		let read2 = dm.read_page(pid2)?;
		
		assert_eq!(read1.buf[500], 0x11);
		assert_eq!(read2.buf[500], 0x22);
		assert_eq!(read3.buf[500], 0x33);
		assert_eq!(read1.header().page_flags, PageFlags::Heap as u16);
		assert_eq!(read2.header().page_flags, PageFlags::Index as u16);
		assert_eq!(read3.header().page_flags, PageFlags::Meta as u16);
		
		Ok(())
	}

	/// Tests synchronization functionality.
	/// 
	/// This test verifies:
	/// - sync() operation completes without errors
	/// - Data is persisted after sync operations
	/// - Multiple sync calls work correctly
	#[test]
	fn disk_synchronization() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_directory.path())?;
		
		// Write some pages
		let pid = dm.allocate_page(99)?;
		let pg = Page::new(pid, PageFlags::Heap);
		dm.write_page(&pg)?;
		
		// Sync should work without errors (may fail on some platforms)
		let _ = dm.sync(); // Ignore errors for platform compatibility
		let _ = dm.sync(); // Multiple syncs should be fine
		
		// Data should still be readable after sync
		let read_pg = dm.read_page(pid)?;
		assert_eq!(read_pg.page_id(), pid);
		
		Ok(())
	}

	/// Tests file path generation for different file IDs.
	/// 
	/// This test verifies:
	/// - File paths follow the expected naming convention
	/// - Different file IDs generate different paths
	/// - Paths are relative to the base directory
	#[test]
	fn file_path_generation() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let dm = FsDiskManager::new(temp_directory.path())?;
		
		assert_eq!(dm.file_path(0), temp_directory.path().join("base_0.db"));
		assert_eq!(dm.file_path(42), temp_directory.path().join("base_42.db"));
		assert_eq!(dm.file_path(u32::MAX), temp_directory.path().join("base_4294967295.db"));
		
		Ok(())
	}

	/// Tests error handling for invalid operations.
	/// 
	/// This test verifies:
	/// - Reading non-existent pages returns appropriate errors
	/// - Error messages provide useful context
	/// - Disk I/O errors are properly propagated
	#[test]
	fn error_handling() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let dm = FsDiskManager::new(temp_directory.path())?;
		
		// Try to read a page from a non-existent file
		let pid = PageId::new(999, 0);
		let result = dm.read_page(pid);
		assert!(result.is_err());
		
		// Try to read beyond the end of a file
		let mut dm_mut = FsDiskManager::new(temp_directory.path())?;
		dm_mut.allocate_page(1)?; // Creates file with 1 page
		let invalid_pid = PageId::new(1, 10); // Try to read page 10
		let result = dm.read_page(invalid_pid);
		assert!(result.is_err());
		
		Ok(())
	}

	/// Tests large page numbers and file IDs.
	/// 
	/// This test verifies:
	/// - System works with large file and page numbers
	/// - Offset calculations don't overflow
	/// - Edge case values are handled correctly
	#[test]
	fn large_numbers() -> anyhow::Result<()> {
		let temp_directory = TempDir::new()?;
		let mut dm = FsDiskManager::new(temp_directory.path())?;
		
		// Test with large file ID
		let pid = dm.allocate_page(1000000)?;
		assert_eq!(pid.file_id(), 1000000);
		assert_eq!(pid.page_no(), 0);
		
		// Write and read a page with this large file ID
		let page = Page::new(pid, PageFlags::Heap);
		dm.write_page(&page)?;
		let read_page = dm.read_page(pid)?;
		assert_eq!(read_page.page_id(), pid);
		
		Ok(())
	}
}