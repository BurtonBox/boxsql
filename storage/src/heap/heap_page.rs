use crate::heap::slot::Slot;
use crate::page::{
    constants::PAGE_SIZE,
    page_file::Page,
    page_header::PageHeader,
    page_id::{PageFlags, PageId},
};

#[derive(Clone)]
pub struct HeapPage {
    pub page: Page,
}

impl HeapPage {
    pub fn new_empty(pid: PageId) -> Self {
        let mut pg = Page::new(pid, PageFlags::Heap);
        pg.recompute_checksum();
        Self { page: pg }
    }

    pub fn slot_count(&self) -> usize {
        let hdr = self.page.header();
        ((PAGE_SIZE as u16 - hdr.upper) as usize) / Slot::SIZE
    }

    fn read_slot(&self, slot_no: usize) -> Slot {
        let hdr = self.page.header();
        let base = (PAGE_SIZE as u16 - ((slot_no as u16 + 1) * Slot::SIZE as u16)) as usize;
        let base = base.max(hdr.upper as usize);
        let off = self.page.read_u16(base);
        let len = self.page.read_u16(base + 2);
        Slot { off, len }
    }

    fn write_slot(&mut self, slot_no: usize, slot: Slot) {
        let base = PAGE_SIZE - ((slot_no + 1) * Slot::SIZE);
        self.page.write_u16(base, slot.off);
        self.page.write_u16(base + 2, slot.len);
    }

    pub fn insert_tuple(&mut self, tuple: &[u8]) -> anyhow::Result<usize> {
        let need = tuple.len() + Slot::SIZE;
        if need > self.page.free_space() {
            anyhow::bail!("not enough free space")
        }

        let mut hdr = self.page.header();
        let slot_no = self.slot_count();

        let off = hdr.lower as usize;
        let len = tuple.len() as u16;
        self.page.buf[off..off + tuple.len()].copy_from_slice(tuple);
        hdr.lower = (off + tuple.len()) as u16;

        hdr.upper = hdr.upper - Slot::SIZE as u16;
        self.page.write_header(&hdr);
        self.write_slot(
            slot_no,
            Slot {
                off: off as u16,
                len,
            },
        );
        self.page.recompute_checksum();
        Ok(slot_no)
    }

    pub fn read_tuple(&self, slot_no: usize) -> Option<&[u8]> {
        if slot_no >= self.slot_count() {
            return None;
        }
        let slot = self.read_slot(slot_no);
        if slot.len == 0 {
            return None;
        }
        let off = slot.off as usize;
        let len = slot.len as usize;
        Some(&self.page.buf[off..off + len])
    }

    pub fn delete_tuple(&mut self, slot_no: usize) -> anyhow::Result<()> {
        if slot_no >= self.slot_count() {
            anyhow::bail!("slot out of range")
        }
        let slot = self.read_slot(slot_no);
        if slot.len == 0 {
            return Ok(());
        }
        self.write_slot(
            slot_no,
            Slot {
                off: slot.off,
                len: 0,
            },
        );
        self.page.recompute_checksum();
        Ok(())
    }

    pub fn compact(&mut self) {
        let mut hdr = self.page.header();
        let slots = self.slot_count();

        let mut live: Vec<(usize, Slot)> = (0..slots)
            .map(|i| (i, self.read_slot(i)))
            .filter(|(_, s)| s.len != 0)
            .collect();

        let mut lower = PageHeader::LEN as u16;

        let mut scratch = vec![0u8; PAGE_SIZE - PageHeader::LEN];
        for (i, s) in live.iter_mut() {
            let data = &self.page.buf[s.off as usize..(s.off + s.len) as usize];
            let off = lower as usize;
            scratch[off - PageHeader::LEN..off - PageHeader::LEN + data.len()]
                .copy_from_slice(data);
            s.off = lower;
            lower += s.len;
            self.write_slot(*i, *s);
        }

        let tgt = PageHeader::LEN..(lower as usize);
        self.page.buf[tgt.clone()].copy_from_slice(&scratch[0..tgt.len()]);
        hdr.lower = lower;
        self.page.write_header(&hdr);
        self.page.recompute_checksum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::disk_manager::DiskManager;
    use crate::disk::file_system::FsDiskManager;
    use crate::page::page_id::PageId;

    #[test]
    fn heap_insert_read_delete_round_trip() {
        let pid = PageId::new(1, 0);
        let mut hp = HeapPage::new_empty(pid);

        let a = b"hello";
        let b = b"world!!!";
        let c = b"1234567890";
        let sa = hp.insert_tuple(a).unwrap();
        let sb = hp.insert_tuple(b).unwrap();
        let sc = hp.insert_tuple(c).unwrap();

        assert_eq!(hp.read_tuple(sa).unwrap(), a);
        assert_eq!(hp.read_tuple(sb).unwrap(), b);
        assert_eq!(hp.read_tuple(sc).unwrap(), c);

        hp.delete_tuple(sb).unwrap();
        assert!(hp.read_tuple(sb).is_none());

        hp.compact();
        assert_eq!(hp.read_tuple(sa).unwrap(), a);
        assert_eq!(hp.read_tuple(sc).unwrap(), c);
    }

    #[test]
    fn heap_page_free_space_and_slots() {
        let pid = PageId::new(1, 1);
        let mut hp = HeapPage::new_empty(pid);
        let initial_free = hp.page.free_space();
        let payload = vec![0u8; 100];
        let mut slots = 0;
        while hp.page.free_space() >= payload.len() + 4 {
            let _ = hp.insert_tuple(&payload).unwrap();
            slots += 1;
        }
        assert!(slots > 50);
        assert!(hp.page.free_space() < initial_free);
    }

    #[test]
    fn disk_write_read_with_checksum() {
        let td = tempfile::tempdir().unwrap();
        let mut dm = FsDiskManager::new(td.path()).unwrap();
        let pid = dm.allocate_page(7).unwrap();

        let mut hp = HeapPage::new_empty(pid);
        for i in 0..200u16 {
            let mut v = vec![0u8; 16];
            v[0..2].copy_from_slice(&i.to_le_bytes());
            hp.insert_tuple(&v).unwrap();
        }

        dm.write_page(&hp.page).unwrap();

        let p2 = dm.read_page(pid).unwrap();
        let hp2 = HeapPage { page: p2 };
        let t0 = hp2.read_tuple(0).unwrap();
        assert_eq!(t0[0..2], 0u16.to_le_bytes());
    }

    #[test]
    fn checksum_catches_corruption() {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};

        let td = tempfile::tempdir().unwrap();
        let mut dm = FsDiskManager::new(td.path()).unwrap();
        let pid = dm.allocate_page(42).unwrap();

        let mut hp = HeapPage::new_empty(pid);
        hp.insert_tuple(b"abc").unwrap();
        dm.write_page(&hp.page).unwrap();

        let path = td.path().join("base_42.db");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        f.seek(SeekFrom::Start(
            (pid.page_no() as u64) * (PAGE_SIZE as u64) + 100,
        ))
        .unwrap();
        f.write_all(&[0xFF]).unwrap();

        let res = dm.read_page(pid);
        assert!(res.is_err(), "corruption should be detected");
    }

    #[test]
    fn heap_page_edge_cases() {
        let pid = PageId::new(1, 2);
        let mut hp = HeapPage::new_empty(pid);

        let single_slot = hp.insert_tuple(b"X").unwrap();
        assert_eq!(hp.read_tuple(single_slot).unwrap(), b"X");

        assert!(hp.read_tuple(999).is_none());
        assert!(hp.delete_tuple(999).is_err());
    }

    #[test]
    fn heap_page_compaction_detailed() {
        let pid = PageId::new(1, 3);
        let mut hp = HeapPage::new_empty(pid);

        let data: &[&[u8]] = &[b"first", b"second", b"third", b"fourth", b"fifth"];
        let mut slots = Vec::new();
        for tuple in data {
            slots.push(hp.insert_tuple(tuple).unwrap());
        }

        hp.delete_tuple(slots[1]).unwrap();
        hp.delete_tuple(slots[3]).unwrap();

        let pre_compact_free = hp.page.free_space();

        hp.compact();
        let post_compact_free = hp.page.free_space();

        assert!(post_compact_free > pre_compact_free);

        assert_eq!(hp.read_tuple(slots[0]).unwrap(), b"first");
        assert_eq!(hp.read_tuple(slots[2]).unwrap(), b"third");
        assert_eq!(hp.read_tuple(slots[4]).unwrap(), b"fifth");
        assert!(hp.read_tuple(slots[1]).is_none());
        assert!(hp.read_tuple(slots[3]).is_none());
    }

    #[test]
    fn heap_page_near_full() {
        let pid = PageId::new(1, 4);
        let mut hp = HeapPage::new_empty(pid);

        let small_tuple = b"x";
        let mut inserted_count = 0;

        while hp.page.free_space() >= small_tuple.len() + 4 {
            if hp.insert_tuple(small_tuple).is_ok() {
                inserted_count += 1;
            } else {
                break;
            }
        }

        assert!(inserted_count > 1000);
        assert!(hp.page.free_space() < 10);
        assert!(hp.insert_tuple(b"cannot fit").is_err());
    }

    #[test]
    fn heap_page_variable_tuple_sizes() {
        let pid = PageId::new(1, 5);
        let mut hp = HeapPage::new_empty(pid);

        let small = b"hi";
        let medium = b"this is a medium sized tuple data";
        let large = vec![b'A'; 1000];

        let slot_small = hp.insert_tuple(small).unwrap();
        let slot_medium = hp.insert_tuple(medium).unwrap();
        let slot_large = hp.insert_tuple(&large).unwrap();

        assert_eq!(hp.read_tuple(slot_small).unwrap(), small);
        assert_eq!(hp.read_tuple(slot_medium).unwrap(), medium);
        assert_eq!(hp.read_tuple(slot_large).unwrap(), &large);

        hp.delete_tuple(slot_medium).unwrap();
        assert_eq!(hp.read_tuple(slot_small).unwrap(), small);
        assert_eq!(hp.read_tuple(slot_large).unwrap(), &large);
        assert!(hp.read_tuple(slot_medium).is_none());
    }

    #[test]
    fn heap_page_slot_directory() {
        let pid = PageId::new(1, 6);
        let mut hp = HeapPage::new_empty(pid);

        let mut slots = Vec::new();
        for i in 0..10u8 {
            let data = vec![i; 10];
            slots.push(hp.insert_tuple(&data).unwrap());
        }

        for (i, &slot_no) in slots.iter().enumerate() {
            assert_eq!(slot_no as usize, i);
        }

        for (i, &slot_no) in slots.iter().enumerate() {
            let expected = vec![i as u8; 10];
            assert_eq!(hp.read_tuple(slot_no).unwrap(), &expected);
        }
    }
}
