use storage::disk::disk_manager::DiskManager;
use storage::disk::file_system::FsDiskManager;
use storage::heap::heap_page::HeapPage;

fn main() -> anyhow::Result<()> {

    let base = std::env::var("BOXSQLD_DATA").unwrap_or_else(|_| "./data".to_string());
    let mut dm = FsDiskManager::new(&base)?;
    let pid = dm.allocate_page(1)?;
    let mut hp = HeapPage::new_empty(pid);

    for i in 0..1000u32 {
        let mut row = [0u8; 12];
        row[0..4].copy_from_slice(&i.to_le_bytes());
        row[4..8].copy_from_slice(&(i.wrapping_mul(7)).to_le_bytes());
        row[8..12].copy_from_slice(&(i.reverse_bits()).to_le_bytes());
        if hp.page.free_space() < row.len() + 4 { break; }
        let _ = hp.insert_tuple(&row)?;
    }

    dm.write_page(&hp.page)?;

    let p2 = dm.read_page(pid)?; // verify checksum and content
    let hp2 = HeapPage { page: p2 };
    if let Some(bytes) = hp2.read_tuple(0) {
        println!("first tuple bytes: {:?}", &bytes[0..12]);
    }

    println!("wrote page {:?} into {}", pid, base);
    Ok(())

}
