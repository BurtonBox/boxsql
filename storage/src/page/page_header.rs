use crate::page::{
    constants::PAGE_SIZE,
    page_id::{PageFlags, PageId},
};

#[derive(Clone, Copy, Debug)]
pub struct PageHeader {
    pub checksum: u32,
    pub page_id: u64,
    pub page_lsn: u64,
    pub page_flags: u16,
    pub lower: u16,
    pub upper: u16,
    pub reserved: [u8; 6],
}

impl PageHeader {
    pub const LEN: usize = 32;

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

    #[test]
    fn page_header_size() {
        assert_eq!(PageHeader::LEN, 32);
        assert_eq!(std::mem::size_of::<PageHeader>(), 32);
    }

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

    #[test]
    fn page_header_space_pointers() {
        let pid = PageId::new(1, 0);
        let hdr = PageHeader::new(pid, PageFlags::Heap);

        assert_eq!(hdr.lower, 32);
        assert_eq!(hdr.upper, 8192);

        let free_space = hdr.upper - hdr.lower;
        assert_eq!(free_space, PAGE_SIZE as u16 - PageHeader::LEN as u16);
        assert_eq!(free_space, 8160);
    }

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
        assert_eq!(original_hdr.lower, copied_hdr.lower);
        assert_eq!(original_hdr.upper, copied_hdr.upper);
    }
}
