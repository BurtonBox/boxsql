use crate::page::page_file::Page;
use crate::page::page_id::PageId;

pub trait DiskManager {
    fn allocate_page(&mut self, file_id: u32) -> anyhow::Result<PageId>;
    fn read_page(&self, pid: PageId) -> anyhow::Result<Page>;
    fn write_page(&mut self, page: &Page) -> anyhow::Result<()>;
    fn sync(&mut self) -> anyhow::Result<()>;
}
