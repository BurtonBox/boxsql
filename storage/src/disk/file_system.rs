use crate::disk::disk_manager::DiskManager;
use crate::page::{constants::PAGE_SIZE, page_file::Page, page_id::PageId};
use anyhow::Context;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum DiskError {
    #[error("checksum mismatch for {0:?}")]
    Checksum(PageId),
}

pub struct FsDiskManager {
    base: PathBuf,
}

impl FsDiskManager {
    pub fn new<P: AsRef<Path>>(base: P) -> anyhow::Result<Self> {
        let base = base.as_ref();
        fs::create_dir_all(base).with_context(|| format!("creating data dir {:?}", base))?;
        Ok(Self {
            base: base.to_path_buf(),
        })
    }

    fn file_path(&self, file_id: u32) -> PathBuf {
        self.base.join(format!("base_{}.db", file_id))
    }

    fn open_rw(&self, path: &Path) -> anyhow::Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?)
    }
}

impl DiskManager for FsDiskManager {
    fn allocate_page(&mut self, file_id: u32) -> anyhow::Result<PageId> {
        let path = self.file_path(file_id);
        let mut file = self.open_rw(&path)?;
        let len = file.metadata()?.len() as usize;
        let page_no = (len / PAGE_SIZE) as u32;

        file.seek(SeekFrom::End(0))?;
        file.write_all(&vec![0u8; PAGE_SIZE])?;
        Ok(PageId::new(file_id, page_no))
    }

    fn read_page(&self, pid: PageId) -> anyhow::Result<Page> {
        let path = self.file_path(pid.file_id());
        let mut file = self.open_rw(&path)?;
        let mut buf = [0u8; PAGE_SIZE];

        let off = (pid.page_no() as u64) * (PAGE_SIZE as u64);
        file.seek(SeekFrom::Start(off))?;
        file.read_exact(&mut buf)?;

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

        let off = (pid.page_no() as u64) * (PAGE_SIZE as u64);
        file.seek(SeekFrom::Start(off))?;
        file.write_all(&page.clone().buf)?;
        Ok(())
    }

    fn sync(&mut self) -> anyhow::Result<()> {
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

    #[test]
    fn disk_manager_creation() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let nested_path = temp_directory.path().join("nested").join("database");

        let _dm = FsDiskManager::new(&nested_path)?;
        assert!(nested_path.exists());
        assert!(nested_path.is_dir());

        let _dm2 = FsDiskManager::new(&nested_path)?;
        assert!(nested_path.exists());

        Ok(())
    }

    #[test]
    fn page_allocation() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_directory.path())?;

        let pid1 = dm.allocate_page(1)?;
        let pid2 = dm.allocate_page(1)?;
        assert_eq!(pid1.file_id(), 1);
        assert_eq!(pid1.page_no(), 0);
        assert_eq!(pid2.file_id(), 1);
        assert_eq!(pid2.page_no(), 1);

        let pid3 = dm.allocate_page(2)?;
        assert_eq!(pid3.file_id(), 2);
        assert_eq!(pid3.page_no(), 0);

        assert!(temp_directory.path().join("base_1.db").exists());
        assert!(temp_directory.path().join("base_2.db").exists());

        Ok(())
    }

    #[test]
    fn page_write_read_round_trip() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_directory.path())?;

        let pid = dm.allocate_page(10)?;
        let mut pg = Page::new(pid, PageFlags::Heap);
        pg.set_lsn(12345);
        pg.buf[100] = 0xAB;
        pg.buf[200] = 0xCD;
        pg.recompute_checksum();

        dm.write_page(&pg)?;

        let read_pg = dm.read_page(pid)?;
        assert_eq!(read_pg.page_id(), pid);
        assert_eq!(read_pg.header().page_lsn, 12345);
        assert_eq!(read_pg.buf[100], 0xAB);
        assert_eq!(read_pg.buf[200], 0xCD);
        assert!(read_pg.verify_checksum());

        Ok(())
    }

    #[test]
    fn checksum_verification() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path())?;

        let pid = dm.allocate_page(5)?;
        let pg = Page::new(pid, PageFlags::Index);
        dm.write_page(&pg)?;

        let read_pg = dm.read_page(pid)?;
        assert!(read_pg.verify_checksum());

        let file_path = temp_dir.path().join("base_5.db");
        let mut file = OpenOptions::new().write(true).open(&file_path)?;
        file.seek(SeekFrom::Start(100))?;
        file.write_all(&[0xFF])?;
        drop(file);

        let result = dm.read_page(pid);
        assert!(result.is_err());

        let error = result.unwrap_err();
        let error_string = format!("{:?}", error);
        assert!(error_string.contains("checksum") || error_string.contains("Checksum"));

        Ok(())
    }

    #[test]
    fn multiple_pages_same_file() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_dir.path())?;

        let pid1 = dm.allocate_page(7)?;
        let pid2 = dm.allocate_page(7)?;
        let pid3 = dm.allocate_page(7)?;

        let mut page1 = Page::new(pid1, PageFlags::Heap);
        let mut page2 = Page::new(pid2, PageFlags::Index);
        let mut page3 = Page::new(pid3, PageFlags::Meta);

        page1.buf[500] = 0x11;
        page2.buf[500] = 0x22;
        page3.buf[500] = 0x33;
        page1.recompute_checksum();
        page2.recompute_checksum();
        page3.recompute_checksum();

        dm.write_page(&page1)?;
        dm.write_page(&page2)?;
        dm.write_page(&page3)?;

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

    #[test]
    fn disk_synchronization() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_directory.path())?;

        let pid = dm.allocate_page(99)?;
        let pg = Page::new(pid, PageFlags::Heap);
        dm.write_page(&pg)?;

        let _ = dm.sync();
        let _ = dm.sync();

        let read_pg = dm.read_page(pid)?;
        assert_eq!(read_pg.page_id(), pid);

        Ok(())
    }

    #[test]
    fn file_path_generation() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let dm = FsDiskManager::new(temp_directory.path())?;

        assert_eq!(dm.file_path(0), temp_directory.path().join("base_0.db"));
        assert_eq!(dm.file_path(42), temp_directory.path().join("base_42.db"));
        assert_eq!(
            dm.file_path(u32::MAX),
            temp_directory.path().join("base_4294967295.db")
        );

        Ok(())
    }

    #[test]
    fn error_handling() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let dm = FsDiskManager::new(temp_directory.path())?;

        let pid = PageId::new(999, 0);
        let result = dm.read_page(pid);
        assert!(result.is_err());

        let mut dm_mut = FsDiskManager::new(temp_directory.path())?;
        dm_mut.allocate_page(1)?;
        let invalid_pid = PageId::new(1, 10);
        let result = dm.read_page(invalid_pid);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn large_numbers() -> anyhow::Result<()> {
        let temp_directory = TempDir::new()?;
        let mut dm = FsDiskManager::new(temp_directory.path())?;

        let pid = dm.allocate_page(1000000)?;
        assert_eq!(pid.file_id(), 1000000);
        assert_eq!(pid.page_no(), 0);

        let page = Page::new(pid, PageFlags::Heap);
        dm.write_page(&page)?;
        let read_page = dm.read_page(pid)?;
        assert_eq!(read_page.page_id(), pid);

        Ok(())
    }
}
