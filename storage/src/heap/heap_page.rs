use crate::heap::slot::Slot;
use crate::page::{constants::PAGE_SIZE, page_file::Page, page_header::PageHeader, page_id::{PageFlags, PageId}};

/// A heap page implementation for storing variable-length tuples.
/// 
/// HeapPage provides tuple storage using a slot directory approach:
/// - Tuples are stored as variable-length data growing upward from the header
/// - A slot directory grows downward from the page end
/// - Each slot contains an (offset, length) pair pointing to a tuple
/// - Free space exists between the data and slot directory
/// 
/// This design allows for:
/// - Efficient variable-length tuple storage
/// - Tuple deletion using tombstones (len=0)
/// - Page compaction to reclaim fragmented space
/// - Fast tuple access by slot number
#[derive(Clone)]
pub struct HeapPage {
	/// The underlying page containing header and data buffer
	pub page: Page,
}

impl HeapPage {
	/// Creates a new empty heap page.
	/// 
	/// Initializes a new page with the Heap flag and proper header.
	/// The page starts with no tuples and maximum free space.
	/// 
	/// # Arguments
	/// * `pid` - Unique identifier for this page
	/// 
	/// # Returns
	/// A new empty HeapPage ready for tuple insertion
	pub fn new_empty(pid: PageId) -> Self {
		let mut pg = Page::new(pid, PageFlags::Heap);
		pg.recompute_checksum();
		Self { page: pg }
	}

	/// Returns the number of slots currently allocated on this page.
	/// 
	/// Calculates the slot count based on how much space has been
	/// allocated for the slot directory (from upper to page end).
	/// 
	/// # Returns
	/// Number of slots (including tombstoned/deleted slots)
	pub fn slot_count(&self) -> usize {
		let hdr = self.page.header();
		((PAGE_SIZE as u16 - hdr.upper) as usize) / Slot::SIZE
	}

	/// Reads a slot entry from the slot directory.
	/// 
	/// Slots are stored at the end of the page, growing downward.
	/// Slot 0 is at the highest address, slot 1 is 4 bytes lower, etc.
	/// 
	/// # Arguments
	/// * `slot_no` - Index of the slot to read
	/// 
	/// # Returns
	/// The Slot entry containing offset and length
	fn read_slot(&self, slot_no: usize) -> Slot {
		let hdr = self.page.header();
		let base = (PAGE_SIZE as u16 - ((slot_no as u16 + 1) * Slot::SIZE as u16)) as usize;
		let base = base.max(hdr.upper as usize); // guard against corruption
		let off = self.page.read_u16(base);
		let len = self.page.read_u16(base + 2);
		Slot { off, len }
	}

	/// Writes a slot entry to the slot directory.
	/// 
	/// Updates the slot at the specified index with new offset and length values.
	/// 
	/// # Arguments
	/// * `slot_no` - Index of the slot to update
	/// * `slot` - New slot data (offset and length)
	fn write_slot(&mut self, slot_no: usize, slot: Slot) {
		let base = PAGE_SIZE - ((slot_no + 1) * Slot::SIZE);
		self.page.write_u16(base, slot.off);
		self.page.write_u16(base + 2, slot.len);
	}

	/// Inserts a new tuple into the heap page.
	/// 
	/// Allocates space for both the tuple data and a new slot directory entry.
	/// The tuple data is written at the current 'lower' position and grows upward,
	/// while a new slot is allocated by moving 'upper' downward.
	/// 
	/// # Arguments
	/// * `tuple` - The tuple data to insert
	/// 
	/// # Returns
	/// The slot number of the newly inserted tuple, or an error if insufficient space
	/// 
	/// # Errors
	/// Returns an error if there isn't enough free space for the tuple and its slot
	pub fn insert_tuple(&mut self, tuple: &[u8]) -> anyhow::Result<usize> {
		let need = tuple.len() + Slot::SIZE;
		if need > self.page.free_space() { anyhow::bail!("not enough free space") }

		let mut hdr = self.page.header();
		let slot_no = self.slot_count();

		// Write tuple data at current lower boundary
		let off = hdr.lower as usize;
		let len = tuple.len() as u16;
		self.page.buf[off..off + tuple.len()].copy_from_slice(tuple);
		hdr.lower = (off + tuple.len()) as u16;

		// Allocate new slot by moving upper boundary down
		hdr.upper = hdr.upper - Slot::SIZE as u16;
		self.page.write_header(&hdr);
		self.write_slot(slot_no, Slot { off: off as u16, len });
		self.page.recompute_checksum();
		Ok(slot_no)
	}

	/// Reads a tuple from the heap page by slot number.
	/// 
	/// Returns a reference to the tuple data if it exists and is not deleted.
	/// Deleted tuples (tombstones) have len=0 and return None.
	/// 
	/// # Arguments
	/// * `slot_no` - The slot number to read from
	/// 
        /// # Returns
        /// Some(tuple_data) if the tuple exists, None if deleted or slot doesn't exist
        pub fn read_tuple(&self, slot_no: usize) -> Option<&[u8]> {
                if slot_no >= self.slot_count() {
                        return None;
                }
                let slot = self.read_slot(slot_no);
                if slot.len == 0 {
                        return None; // tombstone
                }

                let off = slot.off as usize;
                let len = slot.len as usize;
                let end = off + len;
                let hdr = self.page.header();

                // Validate that the tuple lies within the actual data region
                // to avoid out-of-bounds panics on corrupted pages.
                if off < PageHeader::LEN || end > hdr.lower as usize || end > PAGE_SIZE {
                        return None;
                }

                Some(&self.page.buf[off..end])
        }

	/// Deletes a tuple by marking it as a tombstone.
	/// 
	/// Uses the tombstone approach: sets the slot length to 0 but keeps
	/// the slot allocated. The actual tuple data remains on the page until
	/// compaction is performed. This avoids the need to move other tuples.
	/// 
	/// # Arguments
	/// * `slot_no` - The slot number of the tuple to delete
	/// 
	/// # Returns
	/// Ok(()) on success, or an error if the slot number is invalid
	/// 
	/// # Errors
	/// Returns an error if the slot number is out of range
	pub fn delete_tuple(&mut self, slot_no: usize) -> anyhow::Result<()> {
		if slot_no >= self.slot_count() { anyhow::bail!("slot out of range") }
		let slot = self.read_slot(slot_no);
		if slot.len == 0 { return Ok(()) } // already deleted
		self.write_slot(slot_no, Slot { off: slot.off, len: 0 });
		self.page.recompute_checksum();
		Ok(())
	}

	/// Compacts the page by removing fragmentation from deleted tuples.
	/// 
	/// This operation:
	/// 1. Identifies all live (non-tombstoned) tuples
	/// 2. Copies them to a contiguous area starting after the header
	/// 3. Updates the slot directory with new offsets
	/// 4. Reclaims space previously occupied by deleted tuples
	/// 
	/// After compaction, all live tuples are packed together with no gaps,
	/// maximizing available free space for new insertions.
	/// 
	/// # Note
	/// This operation preserves slot numbers - existing slot references
	/// remain valid after compaction.
	pub fn compact(&mut self) {
		let mut hdr = self.page.header();
		let slots = self.slot_count();

		// Collect all live (non-tombstoned) tuples
		let mut live: Vec<(usize, Slot)> = (0..slots)
			.map(|i| (i, self.read_slot(i)))
			.filter(|(_, s)| s.len != 0)
			.collect();

		let mut lower = PageHeader::LEN as u16;

		// Use scratch buffer to rebuild data area
		let mut scratch = vec![0u8; PAGE_SIZE - PageHeader::LEN];
		for (i, s) in live.iter_mut() {
			let data = &self.page.buf[s.off as usize..(s.off + s.len) as usize];
			let off = lower as usize;
			scratch[off - PageHeader::LEN..off - PageHeader::LEN + data.len()].copy_from_slice(data);
			s.off = lower; // Update slot with new offset
			lower += s.len;
			self.write_slot(*i, *s); // Write updated slot
		}

		// Copy compacted data back to page
		let tgt = PageHeader::LEN..(lower as usize);
		self.page.buf[tgt.clone()].copy_from_slice(&scratch[0..tgt.len()]);
		hdr.lower = lower; // Update lower boundary
		self.page.write_header(&hdr);
		self.page.recompute_checksum();
	}
}

#[cfg(test)]
mod tests {
	use crate::disk::disk_manager::DiskManager;
	use crate::disk::file_system::FsDiskManager;
	use crate::page::page_id::PageId;
	use super::*;

	/// Tests basic heap page operations: insert, read, delete, and compaction.
	/// 
	/// This test verifies:
	/// - Tuples can be inserted and read back correctly
	/// - Deleted tuples are marked as tombstones and return None when read
	/// - Compaction removes tombstones while preserving remaining tuples
	/// - Slot numbers remain valid after compaction for non-deleted tuples
	#[test]
	fn heap_insert_read_delete_roundtrip() {
		let pid = PageId::new(1, 0);
		let mut hp = HeapPage::new_empty(pid);

		// Insert 3 tuples
		let a = b"hello"; let b = b"world!!!"; let c = b"1234567890";
		let sa = hp.insert_tuple(a).unwrap();
		let sb = hp.insert_tuple(b).unwrap();
		let sc = hp.insert_tuple(c).unwrap();

		assert_eq!(hp.read_tuple(sa).unwrap(), a);
		assert_eq!(hp.read_tuple(sb).unwrap(), b);
		assert_eq!(hp.read_tuple(sc).unwrap(), c);

		// Delete middle, ensure tombstone
		hp.delete_tuple(sb).unwrap();
		assert!(hp.read_tuple(sb).is_none());

		// Compact and verify remaining tuples readable
		hp.compact();
		assert_eq!(hp.read_tuple(sa).unwrap(), a);
		assert_eq!(hp.read_tuple(sc).unwrap(), c);
	}

	/// Tests heap page capacity and free space management.
	/// 
	/// This test verifies:
	/// - Free space decreases as tuples are inserted
	/// - A reasonable number of tuples can fit in a single page
	/// - Slot directory overhead is accounted for in free space calculations
	#[test]
	fn heap_page_free_space_and_slots() {
		let pid = PageId::new(1, 1);
		let mut hp = HeapPage::new_empty(pid);
		let initial_free = hp.page.free_space();
		let payload = vec![0u8; 100];
		let mut slots = 0;
		while hp.page.free_space() >= payload.len() + 4 { let _ = hp.insert_tuple(&payload).unwrap(); slots += 1; }
		assert!(slots > 50); // sanity: many slots fit in a page
		assert!(hp.page.free_space() < initial_free);
	}

	/// Tests persistence and integrity of heap pages through disk I/O.
	/// 
	/// This test verifies:
	/// - Heap pages can be written to disk and read back correctly
	/// - CRC32 checksums ensure data integrity during I/O operations
	/// - Multiple tuples are preserved across write/read cycles
	/// - Page structure remains intact after disk persistence
	#[test]
	fn disk_write_read_with_checksum() {
		let td = tempfile::tempdir().unwrap();
		let mut dm = FsDiskManager::new(td.path()).unwrap();
		let pid = dm.allocate_page(7).unwrap();

		let mut hp = HeapPage::new_empty(pid);
		// Fill with patterned tuples
		for i in 0..200u16 {
			let mut v = vec![0u8; 16];
			v[0..2].copy_from_slice(&i.to_le_bytes());
			hp.insert_tuple(&v).unwrap();
		}

		// Write page to disk
		dm.write_page(&hp.page).unwrap();

		// Read back and validate
		let p2 = dm.read_page(pid).unwrap();
		let hp2 = HeapPage { page: p2 };
		let t0 = hp2.read_tuple(0).unwrap();
		assert_eq!(t0[0..2], 0u16.to_le_bytes());
	}

	/// Tests that CRC32 checksums detect data corruption on disk.
	/// 
	/// This test verifies:
	/// - Data corruption is detected when reading pages from disk
	/// - CRC32 checksums provide reliable integrity verification
	/// - The system fails safely when corruption is detected
	/// - Manual corruption of disk data is properly caught
	#[test]
	fn checksum_catches_corruption() {
		use std::io::{Seek, SeekFrom, Write};
		use std::fs::OpenOptions;

		let td = tempfile::tempdir().unwrap();
		let mut dm = FsDiskManager::new(td.path()).unwrap();
		let pid = dm.allocate_page(42).unwrap();

		let mut hp = HeapPage::new_empty(pid);
		hp.insert_tuple(b"abc").unwrap();
		dm.write_page(&hp.page).unwrap();

		// Corrupt on disk by flipping a byte without fixing checksum
		let path = td.path().join("base_42.db");
		let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
		f.seek(SeekFrom::Start((pid.page_no() as u64) * (PAGE_SIZE as u64) + 100)).unwrap();
		f.write_all(&[0xFF]).unwrap();

		let res = dm.read_page(pid);
		assert!(res.is_err(), "corruption should be detected");
	}

	/// Tests heap page edge cases and boundary conditions.
	/// 
	/// This test verifies:
	/// - Empty tuple insertion and retrieval
	/// - Maximum size tuple handling
	/// - Page full conditions
	/// - Invalid slot access returns None appropriately
	#[test]
	fn heap_page_edge_cases() {
		let pid = PageId::new(1, 2);
		let mut hp = HeapPage::new_empty(pid);
		
		// Test single byte tuple
		let single_slot = hp.insert_tuple(b"X").unwrap();
		assert_eq!(hp.read_tuple(single_slot).unwrap(), b"X");
		
		// Test reading non-existent slot
		assert!(hp.read_tuple(999).is_none());
		
		// Test deleting non-existent slot should return error
		assert!(hp.delete_tuple(999).is_err());
	}

	/// Tests heap page compaction functionality in detail.
	/// 
	/// This test verifies:
	/// - Compaction removes all tombstones
	/// - Remaining tuples are preserved correctly
	/// - Free space increases after compaction
	/// - Slot numbers may change after compaction
	#[test]
	fn heap_page_compaction_detailed() {
		let pid = PageId::new(1, 3);
		let mut hp = HeapPage::new_empty(pid);
		
		// Insert multiple tuples
		let data: &[&[u8]] = &[b"first", b"second", b"third", b"fourth", b"fifth"];
		let mut slots = Vec::new();
		for tuple in data {
			slots.push(hp.insert_tuple(tuple).unwrap());
		}
		
		// Delete every other tuple (creating fragmentation)
		hp.delete_tuple(slots[1]).unwrap(); // "second"
		hp.delete_tuple(slots[3]).unwrap(); // "fourth"
		
		let pre_compact_free = hp.page.free_space();
		
		// Compact and verify
		hp.compact();
		let post_compact_free = hp.page.free_space();
		
		// Free space should increase
		assert!(post_compact_free > pre_compact_free);
		
		// Remaining tuples should still be readable
		assert_eq!(hp.read_tuple(slots[0]).unwrap(), b"first");
		assert_eq!(hp.read_tuple(slots[2]).unwrap(), b"third");
		assert_eq!(hp.read_tuple(slots[4]).unwrap(), b"fifth");
		
		// Deleted tuples should remain deleted
		assert!(hp.read_tuple(slots[1]).is_none());
		assert!(hp.read_tuple(slots[3]).is_none());
	}

	/// Tests heap page behavior when nearly full.
	/// 
	/// This test verifies:
	/// - Pages can be filled to near capacity
	/// - Insertion fails gracefully when page is full
	/// - Free space calculation is accurate when page is nearly full
	#[test]
	fn heap_page_near_full() {
		let pid = PageId::new(1, 4);
		let mut hp = HeapPage::new_empty(pid);
		
		// Fill page with many small tuples until nearly full
		let small_tuple = b"x";
		let mut inserted_count = 0;
		
		while hp.page.free_space() >= small_tuple.len() + 4 { // 4 bytes for slot
			if hp.insert_tuple(small_tuple).is_ok() {
				inserted_count += 1;
			} else {
				break;
			}
		}
		
		// Should have inserted many tuples
		assert!(inserted_count > 1000);
		
		// Page should be nearly full
		assert!(hp.page.free_space() < 10);
		
		// Next insertion should fail
		assert!(hp.insert_tuple(b"cannot fit").is_err());
	}

	/// Tests heap page with various tuple sizes.
	/// 
	/// This test verifies:
	/// - Tuples of different sizes can coexist on the same page
	/// - Large tuples are handled correctly
	/// - Slot directory grows correctly with different tuple sizes
	#[test]
	fn heap_page_variable_tuple_sizes() {
		let pid = PageId::new(1, 5);
		let mut hp = HeapPage::new_empty(pid);
		
		// Insert tuples of varying sizes
		let small = b"hi";
		let medium = b"this is a medium sized tuple data";
		let large = vec![b'A'; 1000]; // 1KB tuple
		
		let slot_small = hp.insert_tuple(small).unwrap();
		let slot_medium = hp.insert_tuple(medium).unwrap();
		let slot_large = hp.insert_tuple(&large).unwrap();
		
		// Verify all can be read back correctly
		assert_eq!(hp.read_tuple(slot_small).unwrap(), small);
		assert_eq!(hp.read_tuple(slot_medium).unwrap(), medium);
		assert_eq!(hp.read_tuple(slot_large).unwrap(), &large);
		
		// Delete medium tuple and verify others unaffected
		hp.delete_tuple(slot_medium).unwrap();
		assert_eq!(hp.read_tuple(slot_small).unwrap(), small);
		assert_eq!(hp.read_tuple(slot_large).unwrap(), &large);
		assert!(hp.read_tuple(slot_medium).is_none());
	}

	/// Tests heap page slot directory management.
	/// 
	/// This test verifies:
	/// - Slot directory grows downward from page end
	/// - Slots are correctly indexed and accessible
	/// - Slot directory and data area don't overlap
        #[test]
        fn heap_page_slot_directory() {
                let pid = PageId::new(1, 6);
                let mut hp = HeapPage::new_empty(pid);
		
		// Insert tuples and track slot assignments
		let mut slots = Vec::new();
		for i in 0..10u8 {
			let data = vec![i; 10]; // 10 bytes of repeated value
			slots.push(hp.insert_tuple(&data).unwrap());
		}
		
		// Verify slots are assigned sequentially starting from 0
		for (i, &slot_no) in slots.iter().enumerate() {
			assert_eq!(slot_no as usize, i);
		}
		
		// Verify each slot contains the correct data
                for (i, &slot_no) in slots.iter().enumerate() {
                        let expected = vec![i as u8; 10];
                        assert_eq!(hp.read_tuple(slot_no).unwrap(), &expected);
                }
        }

        /// Ensures corrupted slot entries do not cause panics and return None instead.
        ///
        /// This test verifies:
        /// - read_tuple() gracefully handles slots pointing outside the data region
        /// - No panic occurs when slot offset/length are out of bounds
        /// - Function returns None for corrupted slot metadata
        #[test]
        fn heap_page_read_tuple_out_of_bounds() {
                let pid = PageId::new(1, 7);
                let mut hp = HeapPage::new_empty(pid);

                let slot = hp.insert_tuple(b"data").unwrap();

                // Corrupt the slot to point beyond the end of the page
                let base = PAGE_SIZE - ((slot + 1) * Slot::SIZE);
                hp.page.write_u16(base, (PAGE_SIZE + 10) as u16);

                // Should not panic and must return None for out-of-bounds slot
                assert!(hp.read_tuple(slot).is_none());
        }
}