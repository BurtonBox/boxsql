use crate::page::{
    constants::PAGE_SIZE,
    page_header::PageHeader,
    page_id::{PageFlags, PageId},
};
use crc32fast::Hasher;

#[derive(Clone, Debug)]
pub struct Page {
    pub buf: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(pid: PageId, flags: PageFlags) -> Self {
        let mut pg = Self {
            buf: [0u8; PAGE_SIZE],
        };
        let header = PageHeader::new(pid, flags);
        pg.write_header(&header);
        pg.recompute_checksum();
        pg
    }

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
    pub fn write_header(&mut self, hdr: &PageHeader) {
        self.buf[0..4].copy_from_slice(&hdr.checksum.to_le_bytes());
        self.buf[4..12].copy_from_slice(&hdr.page_id.to_le_bytes());
        self.buf[12..20].copy_from_slice(&hdr.page_lsn.to_le_bytes());
        self.buf[20..22].copy_from_slice(&hdr.page_flags.to_le_bytes());
        self.buf[22..24].copy_from_slice(&hdr.lower.to_le_bytes());
        self.buf[24..26].copy_from_slice(&hdr.upper.to_le_bytes());
        self.buf[26..32].copy_from_slice(&hdr.reserved);
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.buf[12..20].copy_from_slice(&lsn.to_le_bytes());
    }

    pub fn set_lower(&mut self, lower: u16) {
        self.buf[22..24].copy_from_slice(&lower.to_le_bytes());
    }

    pub fn set_upper(&mut self, upper: u16) {
        self.buf[24..26].copy_from_slice(&upper.to_le_bytes());
    }

    pub fn free_space(&self) -> usize {
        let hdr = self.header();
        (hdr.upper - hdr.lower) as usize
    }

    pub fn page_id(&self) -> PageId {
        PageId(u64::from_le_bytes(self.buf[4..12].try_into().unwrap()))
    }

    pub fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.buf[4..]);
        let sum = hasher.finalize();
        sum == u32::from_le_bytes(self.buf[0..4].try_into().unwrap())
    }

    pub fn recompute_checksum(&mut self) {
        self.buf[0..4].fill(0);
        let mut hasher = Hasher::new();
        hasher.update(&self.buf[4..]);
        let sum = hasher.finalize();
        self.buf[0..4].copy_from_slice(&sum.to_le_bytes());
    }

    pub fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.buf[offset..offset + 2].try_into().unwrap())
    }

    pub fn write_u16(&mut self, offset: usize, value: u16) {
        self.buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn page_header_serialization() {
        let pid = PageId::new(1, 100);
        let mut pg = Page::new(pid, PageFlags::Index);

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

    #[test]
    fn page_lsn_updates() {
        let pid = PageId::new(2, 200);
        let mut pg = Page::new(pid, PageFlags::Meta);

        pg.set_lsn(999);
        assert_eq!(pg.header().page_lsn, 999);

        pg.set_lsn(u64::MAX);
        assert_eq!(pg.header().page_lsn, u64::MAX);

        assert_eq!(pg.page_id(), pid);
        assert_eq!(pg.header().page_flags, PageFlags::Meta as u16);
    }

    #[test]
    fn page_space_pointers() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        let initial_hdr = pg.header();
        assert_eq!(initial_hdr.lower, 32);
        assert_eq!(initial_hdr.upper, 8192);
        assert_eq!(pg.free_space(), 8160);

        pg.set_lower(100);
        pg.set_upper(8000);

        assert_eq!(pg.header().lower, 100);
        assert_eq!(pg.header().upper, 8000);
        assert_eq!(pg.free_space(), 7900);
    }

    #[test]
    fn page_free_space_edge_cases() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        pg.set_lower(4000);
        pg.set_upper(4000);
        assert_eq!(pg.free_space(), 0);

        pg.set_lower(32);
        pg.set_upper(8192);
        assert_eq!(pg.free_space(), 8160);
    }

    #[test]
    fn page_u16_operations() {
        let pid = PageId::new(1, 1);
        let mut pg = Page::new(pid, PageFlags::Heap);

        let test_cases = [(100, 0u16), (200, 65535u16), (300, 12345u16), (400, 256u16)];

        for (offset, value) in test_cases {
            pg.write_u16(offset, value);
            assert_eq!(pg.read_u16(offset), value);
        }
    }

    #[test]
    fn page_checksum_verification() {
        let pid = PageId::new(5, 500);
        let mut pg = Page::new(pid, PageFlags::Heap);

        assert!(pg.verify_checksum());

        pg.buf[100] = 0xFF;
        assert!(!pg.verify_checksum());

        pg.recompute_checksum();
        assert!(pg.verify_checksum());

        pg.buf[0] = 0x00;
        assert!(!pg.verify_checksum());
    }

    #[test]
    fn page_cloning() {
        let pid = PageId::new(7, 700);
        let mut original_pg = Page::new(pid, PageFlags::Index);

        original_pg.set_lsn(999);
        original_pg.buf[100] = 42;
        original_pg.recompute_checksum();

        let cloned_pg = original_pg.clone();
        assert_eq!(cloned_pg.buf, original_pg.buf);
        assert_eq!(cloned_pg.page_id(), original_pg.page_id());
        assert_eq!(cloned_pg.header().page_lsn, 999);
        assert_eq!(cloned_pg.buf[100], 42);

        let mut modified_clone = cloned_pg;
        modified_clone.buf[200] = 99;
        assert_eq!(original_pg.buf[200], 0);
        assert_eq!(modified_clone.buf[200], 99);
    }
}
