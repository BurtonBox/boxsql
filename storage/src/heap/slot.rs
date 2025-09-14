#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub off: u16,
    pub len: u16,
}

impl Slot {
    pub const SIZE: usize = 4;

    pub fn new(off: u16, len: u16) -> Self {
        Self { off, len }
    }

    pub fn tombstone(off: u16) -> Self {
        Self { off, len: 0 }
    }

    pub fn is_tombstone(&self) -> bool {
        self.len == 0
    }

    pub fn is_valid(&self) -> bool {
        self.len > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_creation() {
        let slot = Slot::new(100, 50);
        assert_eq!(slot.off, 100);
        assert_eq!(slot.len, 50);
        assert_eq!(Slot::SIZE, 4);
        assert_eq!(std::mem::size_of::<Slot>(), 4);
    }

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

    #[test]
    fn slot_validation() {
        let slot1 = Slot::new(0, 1);
        let slot2 = Slot::new(1000, u16::MAX);
        assert!(slot1.is_valid());
        assert!(slot2.is_valid());
        assert!(!slot1.is_tombstone());
        assert!(!slot2.is_tombstone());

        let slot3 = Slot::new(500, 0);
        let slot4 = Slot::tombstone(750);
        assert!(!slot3.is_valid());
        assert!(!slot4.is_valid());
        assert!(slot3.is_tombstone());
        assert!(slot4.is_tombstone());
    }

    #[test]
    fn slot_equality_and_copy() {
        let slot1 = Slot::new(100, 50);
        let slot2 = Slot::new(100, 50);
        let slot3 = Slot::new(100, 51);
        let slot4 = Slot::new(101, 50);

        assert_eq!(slot1, slot2);
        assert_ne!(slot1, slot3);
        assert_ne!(slot1, slot4);

        let copied = slot1;
        let cloned = slot1.clone();
        assert_eq!(slot1, copied);
        assert_eq!(slot1, cloned);
    }

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
