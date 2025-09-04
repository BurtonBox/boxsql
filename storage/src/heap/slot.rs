/// Slot directory entry pointing to a tuple within a heap page.
///
/// Each slot is a 4-byte structure that describes the location and size
/// of a tuple on the page. Slots form a directory that grows downward
/// from the end of the page, while tuple data grows upward from the header.
///
/// This indirection allows for:
/// - Variable-length tuples
/// - Tuple deletion without moving other tuples (tombstone approach)
/// - Future support for tuple updates and page compaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    /// Byte offset of the tuple within the page
    pub off: u16,
    /// Length of the tuple in bytes (0 indicates deleted/tombstone)
    pub len: u16,
}

impl Slot {
    /// Size of a slot entry in bytes (offset + length = 4 bytes)
    pub const SIZE: usize = 4;

    /// Creates a new slot pointing to a tuple.
    ///
    /// # Arguments
    /// * `off` - Byte offset of the tuple within the page
    /// * `len` - Length of the tuple in bytes
    ///
    /// # Returns
    /// A new Slot pointing to the specified tuple location
    pub fn new(off: u16, len: u16) -> Self {
        Self { off, len }
    }

    /// Creates a tombstone slot (deleted tuple marker).
    ///
    /// Tombstone slots have zero length but preserve the offset
    /// for potential future use or debugging.
    ///
    /// # Arguments
    /// * `off` - Original offset of the deleted tuple
    ///
    /// # Returns
    /// A tombstone slot with zero length
    pub fn tombstone(off: u16) -> Self {
        Self { off, len: 0 }
    }

    /// Checks if this slot represents a deleted tuple.
    ///
    /// # Returns
    /// true if the slot is a tombstone (len == 0)
    pub fn is_tombstone(&self) -> bool {
        self.len == 0
    }

    /// Checks if this slot points to a valid tuple.
    ///
    /// # Returns
    /// true if the slot points to a tuple with non-zero length
    pub fn is_valid(&self) -> bool {
        self.len > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests basic slot creation and properties.
    ///
    /// This test verifies:
    /// - Slot::new() creates slots with correct offset and length
    /// - Slot fields are accessible and correct
    /// - Slot size constant matches the actual struct size
    #[test]
    fn slot_creation() {
        let slot = Slot::new(100, 50);
        assert_eq!(slot.off, 100);
        assert_eq!(slot.len, 50);
        assert_eq!(Slot::SIZE, 4);
        assert_eq!(std::mem::size_of::<Slot>(), 4);
    }

    /// Tests tombstone slot functionality.
    ///
    /// This test verifies:
    /// - Tombstone slots have zero length
    /// - Tombstone slots preserve the original offset
    /// - is_tombstone() correctly identifies tombstone slots
    /// - is_valid() correctly identifies non-tombstone slots
    #[test]
    fn slot_tombstones() {
        let tombstone = Slot::tombstone(200);
        assert_eq!(tombstone.off, 200);
        assert_eq!(tombstone.len, 0);
        assert!(tombstone.is_tombstone());
        assert!(!tombstone.is_valid());

        let valid_slot = Slot::new(200, 25);
        assert!(!valid_slot.is_tombstone());
        assert!(valid_slot.is_valid());
    }

    /// Tests slot validation methods.
    ///
    /// This test verifies:
    /// - is_valid() returns true for slots with non-zero length
    /// - is_valid() returns false for slots with zero length
    /// - is_tombstone() behaves as the logical inverse of is_valid()
    #[test]
    fn slot_validation() {
        // Valid slots
        let slot1 = Slot::new(0, 1);
        let slot2 = Slot::new(1000, u16::MAX);
        assert!(slot1.is_valid());
        assert!(slot2.is_valid());
        assert!(!slot1.is_tombstone());
        assert!(!slot2.is_tombstone());

        // Invalid/tombstone slots
        let slot3 = Slot::new(500, 0);
        let slot4 = Slot::tombstone(750);
        assert!(!slot3.is_valid());
        assert!(!slot4.is_valid());
        assert!(slot3.is_tombstone());
        assert!(slot4.is_tombstone());
    }

    /// Tests slot equality and copying.
    ///
    /// This test verifies:
    /// - Slots with identical offset and length are equal
    /// - Slots with different values are not equal
    /// - Slot implements Copy and Clone correctly
    #[test]
    fn slot_equality_and_copy() {
        let slot1 = Slot::new(100, 50);
        let slot2 = Slot::new(100, 50);
        let slot3 = Slot::new(100, 51);
        let slot4 = Slot::new(101, 50);

        assert_eq!(slot1, slot2);
        assert_ne!(slot1, slot3);
        assert_ne!(slot1, slot4);

        // Test copying
        let copied = slot1;
        let cloned = slot1.clone();
        assert_eq!(slot1, copied);
        assert_eq!(slot1, cloned);
    }

    /// Tests slot with boundary values.
    ///
    /// This test verifies:
    /// - Slots work correctly with maximum u16 values
    /// - Slots work correctly with zero offset
    /// - Edge cases are handled properly
    #[test]
    fn slot_boundary_values() {
        let max_slot = Slot::new(u16::MAX, u16::MAX);
        assert_eq!(max_slot.off, 65535);
        assert_eq!(max_slot.len, 65535);
        assert!(max_slot.is_valid());

        let zero_offset = Slot::new(0, 100);
        assert_eq!(zero_offset.off, 0);
        assert_eq!(zero_offset.len, 100);
        assert!(zero_offset.is_valid());

        let zero_length = Slot::new(100, 0);
        assert!(zero_length.is_tombstone());
        assert!(!zero_length.is_valid());
    }
}
