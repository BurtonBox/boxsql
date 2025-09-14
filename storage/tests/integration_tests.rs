use storage::disk::disk_manager::DiskManager;
use storage::disk::file_system::FsDiskManager;
use storage::heap::heap_page::HeapPage;
use storage::page::page_file::Page;
use storage::page::page_id::{PageFlags, PageId};
use tempfile::TempDir;

#[test]
fn end_to_end_storage_workflow() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let pid = dm.allocate_page(1)?;
    let mut hp = HeapPage::new_empty(pid);

    let slot1 = hp.insert_tuple(b"user:john:age:25")?;
    let slot2 = hp.insert_tuple(b"user:jane:age:30")?;
    let slot3 = hp.insert_tuple(b"product:laptop:price:999.99")?;

    dm.write_page(&hp.page)?;

    let page = dm.read_page(pid)?;
    let hp2 = HeapPage { page };

    assert_eq!(hp2.read_tuple(slot1).unwrap(), b"user:john:age:25");
    assert_eq!(hp2.read_tuple(slot2).unwrap(), b"user:jane:age:30");
    assert_eq!(
        hp2.read_tuple(slot3).unwrap(),
        b"product:laptop:price:999.99"
    );

    Ok(())
}

#[test]
fn multiple_files_multiple_pages() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let file1_page1 = dm.allocate_page(1)?;
    let file1_page2 = dm.allocate_page(1)?;
    let file2_page1 = dm.allocate_page(2)?;
    let file3_page1 = dm.allocate_page(3)?;

    let mut hp1_1 = HeapPage::new_empty(file1_page1);
    let mut hp1_2 = HeapPage::new_empty(file1_page2);
    let mut hp2_1 = HeapPage::new_empty(file2_page1);
    let mut hp3_1 = HeapPage::new_empty(file3_page1);

    hp1_1.insert_tuple(b"file1_page1_data")?;
    hp1_2.insert_tuple(b"file1_page2_data")?;
    hp2_1.insert_tuple(b"file2_page1_data")?;
    hp3_1.insert_tuple(b"file3_page1_data")?;

    dm.write_page(&hp1_1.page)?;
    dm.write_page(&hp1_2.page)?;
    dm.write_page(&hp2_1.page)?;
    dm.write_page(&hp3_1.page)?;

    let read_hp3 = HeapPage {
        page: dm.read_page(file3_page1)?,
    };
    let read_hp1_1 = HeapPage {
        page: dm.read_page(file1_page1)?,
    };
    let read_hp2 = HeapPage {
        page: dm.read_page(file2_page1)?,
    };
    let read_hp1_2 = HeapPage {
        page: dm.read_page(file1_page2)?,
    };

    assert_eq!(read_hp1_1.read_tuple(0).unwrap(), b"file1_page1_data");
    assert_eq!(read_hp1_2.read_tuple(0).unwrap(), b"file1_page2_data");
    assert_eq!(read_hp2.read_tuple(0).unwrap(), b"file2_page1_data");
    assert_eq!(read_hp3.read_tuple(0).unwrap(), b"file3_page1_data");

    Ok(())
}

#[test]
fn large_dataset_handling() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let mut pages = Vec::new();
    let mut all_slots = Vec::new();

    for page_num in 0..10 {
        let pid = dm.allocate_page(1)?;
        let mut hp = HeapPage::new_empty(pid);
        let mut slots = Vec::new();

        for i in 0..100 {
            let data = format!("page_{}_tuple_{}_data_payload", page_num, i);
            if let Ok(slot) = hp.insert_tuple(data.as_bytes()) {
                slots.push((slot, data));
            }
        }

        dm.write_page(&hp.page)?;
        pages.push(pid);
        all_slots.push(slots);
    }

    for (page_idx, pid) in pages.iter().enumerate() {
        let page = dm.read_page(*pid)?;
        let hp = HeapPage { page };

        for (slot_no, expected_data) in &all_slots[page_idx] {
            let actual_data = hp.read_tuple(*slot_no).unwrap();
            assert_eq!(actual_data, expected_data.as_bytes());
        }
    }

    println!(
        "Successfully stored and verified {} pages with ~{} tuples each",
        pages.len(),
        all_slots[0].len()
    );

    Ok(())
}

#[test]
fn database_recovery_simulation() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let original_data: &[&[u8]] = &[
        b"critical_user_data",
        b"important_transaction_log",
        b"essential_system_metadata",
    ];

    let pid;
    let slots;

    {
        let mut dm = FsDiskManager::new(temp_dir.path())?;
        pid = dm.allocate_page(1)?;
        let mut hp = HeapPage::new_empty(pid);

        slots = original_data
            .iter()
            .map(|data| hp.insert_tuple(data))
            .collect::<Result<Vec<_>, _>>()?;

        dm.write_page(&hp.page)?;
        let _ = dm.sync();
    }

    {
        let dm = FsDiskManager::new(temp_dir.path())?;
        let page = dm.read_page(pid)?;
        let hp = HeapPage { page };

        for (i, &slot) in slots.iter().enumerate() {
            let recovered_data = hp.read_tuple(slot).unwrap();
            assert_eq!(recovered_data, original_data[i]);
        }
    }

    Ok(())
}

#[test]
fn simulated_concurrent_operations() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let mut operations = Vec::new();

    for i in 0..5 {
        let pid = dm.allocate_page(i + 1)?;
        let mut hp = HeapPage::new_empty(pid);

        for j in 0..10 {
            let data = format!("page_{}_data_{}", i, j);
            let slot = hp.insert_tuple(data.as_bytes())?;
            operations.push((pid, slot, data));
        }

        dm.write_page(&hp.page)?;
    }

    for (pid, slot, expected_data) in operations {
        let page = dm.read_page(pid)?;
        let hp = HeapPage { page };
        let actual_data = hp.read_tuple(slot).unwrap();
        assert_eq!(actual_data, expected_data.as_bytes());
    }

    Ok(())
}

#[test]
fn error_handling_integration() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let fake_pid = PageId::new(999, 999);
    assert!(dm.read_page(fake_pid).is_err());

    let real_pid = dm.allocate_page(1)?;
    let mut hp = HeapPage::new_empty(real_pid);
    let slot = hp.insert_tuple(b"normal_data")?;
    dm.write_page(&hp.page)?;

    let read_page = dm.read_page(real_pid)?;
    let read_hp = HeapPage { page: read_page };
    assert_eq!(read_hp.read_tuple(slot).unwrap(), b"normal_data");

    let mut full_hp = HeapPage::new_empty(real_pid);
    let large_data = vec![b'X'; 2000];

    while full_hp.page.free_space() > large_data.len() + 4 {
        if full_hp.insert_tuple(&large_data).is_err() {
            break;
        }
    }

    let very_large_data = vec![b'Y'; 8000];
    assert!(full_hp.insert_tuple(&very_large_data).is_err());
    assert!(full_hp.read_tuple(0).is_some());

    Ok(())
}

#[test]
fn page_types_integration() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut dm = FsDiskManager::new(temp_dir.path())?;

    let heap_pid = dm.allocate_page(1)?;
    let index_pid = dm.allocate_page(2)?;
    let meta_pid = dm.allocate_page(3)?;

    let heap_page = Page::new(heap_pid, PageFlags::Heap);
    let index_page = Page::new(index_pid, PageFlags::Index);
    let meta_page = Page::new(meta_pid, PageFlags::Meta);

    dm.write_page(&heap_page)?;
    dm.write_page(&index_page)?;
    dm.write_page(&meta_page)?;

    let read_heap = dm.read_page(heap_pid)?;
    let read_index = dm.read_page(index_pid)?;
    let read_meta = dm.read_page(meta_pid)?;

    assert_eq!(read_heap.header().page_flags, PageFlags::Heap as u16);
    assert_eq!(read_index.header().page_flags, PageFlags::Index as u16);
    assert_eq!(read_meta.header().page_flags, PageFlags::Meta as u16);

    Ok(())
}

#[test]
fn resource_management() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;

    for iteration in 0..10 {
        let mut dm = FsDiskManager::new(temp_dir.path())?;

        let pid = dm.allocate_page(iteration)?;
        let hp = HeapPage::new_empty(pid);
        dm.write_page(&hp.page)?;
        let _ = dm.sync();
    }

    let dm = FsDiskManager::new(temp_dir.path())?;
    for iteration in 0..10 {
        let pid = PageId::new(iteration, 0);
        let page = dm.read_page(pid)?;
        assert!(page.verify_checksum());
    }

    Ok(())
}
