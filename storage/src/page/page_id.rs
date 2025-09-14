use std::fmt;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageFlags {
    Heap = 1,
    Index = 2,
    Meta = 4,
}

#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct PageId(pub u64);

impl PageId {
    pub fn new(file_id: u32, page_no: u32) -> Self {
        Self(((file_id as u64) << 32) | (page_no as u64))
    }

    pub fn file_id(self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn page_no(self) -> u32 {
        (self.0 & 0xFFFF_FFFF) as u32
    }
}

impl fmt::Debug for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({}:{})", self.file_id(), self.page_no())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_creation_and_access() {
        let pid = PageId::new(42, 1337);
        assert_eq!(pid.file_id(), 42);
        assert_eq!(pid.page_no(), 1337);

        let min_pid = PageId::new(0, 0);
        assert_eq!(min_pid.file_id(), 0);
        assert_eq!(min_pid.page_no(), 0);

        let max_pid = PageId::new(u32::MAX, u32::MAX);
        assert_eq!(max_pid.file_id(), u32::MAX);
        assert_eq!(max_pid.page_no(), u32::MAX);
    }

    #[test]
    fn page_id_comparison() {
        let pid1 = PageId::new(1, 100);
        let pid2 = PageId::new(1, 200);
        let pid3 = PageId::new(2, 50);

        assert!(pid1 < pid2);
        assert!(pid2 < pid3);
        assert!(pid1 < pid3);
        assert_eq!(pid1, PageId::new(1, 100));
    }

    #[test]
    fn page_id_format() {
        let pid = PageId::new(5, 123);
        assert_eq!(format!("{:?}", pid), "PageId(5:123)");
    }

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

    #[test]
    fn page_id_page_flags_values() {
        assert_eq!(PageFlags::Heap as u16, 1);
        assert_eq!(PageFlags::Index as u16, 2);
        assert_eq!(PageFlags::Meta as u16, 4);

        assert_eq!((PageFlags::Heap as u16).count_ones(), 1);
        assert_eq!((PageFlags::Index as u16).count_ones(), 1);
        assert_eq!((PageFlags::Meta as u16).count_ones(), 1);
    }
}
