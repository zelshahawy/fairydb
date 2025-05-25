#[cfg(test)]
mod tests {
    use crate::page::Page;
    use common::{ids::PageId, testutil::get_rng};
    use rand::seq::SliceRandom;
    use std::path::Path;

    use crate::base_file::{BaseFile, BaseFileTrait};

    fn base_file_gen(db_dir: &Path) -> BaseFile {
        BaseFile::new(db_dir, 0).unwrap()
    }

    #[test]
    fn test_base_file_write_read() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());
        let page_id = 0;
        let mut page = Page::new(page_id);

        let data = b"Hello, World!";
        page[0..data.len()].copy_from_slice(data);

        base_file.write_page(page_id, &page).unwrap();

        let mut read_page = Page::new_empty();
        base_file.read_page(page_id, &mut read_page).unwrap();

        assert_eq!(&read_page[0..data.len()], data);
        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().read_count(),
            1,
            "Read count should be 1"
        );
        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().write_count(),
            1,
            "write count should be 1"
        );
    }

    #[test]
    fn test_base_file_prefetch_page() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());

        let num_pages = 1000;
        let mut page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        // Write the pages
        for i in 0..num_pages {
            let mut page = Page::new(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            base_file.write_page(i, &page).unwrap();
        }

        let mut rng = get_rng();
        page_id_vec.shuffle(&mut rng);

        for i in page_id_vec {
            base_file.prefetch_page(i).unwrap();
            let mut read_page = Page::new_empty();
            base_file.read_page(i, &mut read_page).unwrap();
        }

        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().write_count(),
            1000,
            "write count should be 1000"
        );

        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().read_count(),
            1000,
            "read count should be 1000"
        );
    }

    #[test]
    fn test_base_file_page_write_read_sequential() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());

        let num_pages = 1000;

        for i in 0..num_pages {
            let mut page = Page::new(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            base_file.write_page(i, &page).unwrap();
        }

        for i in 0..num_pages {
            let mut read_page = Page::new_empty();
            base_file.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[test]
    fn test_base_file_page_write_read_random() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());

        let num_pages = 1000;
        let mut page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        let mut rng = get_rng();
        page_id_vec.shuffle(&mut rng);
        // Write the page in random order
        for i in &page_id_vec {
            let mut page = Page::new(*i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            base_file.write_page(*i, &page).unwrap();
        }

        // Shuffle the page_id_vec again to read in random order
        page_id_vec.shuffle(&mut rng);

        // Read the page in random order
        for i in page_id_vec {
            let mut read_page = Page::new_empty();
            base_file.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[test]
    fn test_base_file_page_write_read_interleave() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());

        let num_pages = 1000;
        let mut page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        let mut rng = get_rng();
        page_id_vec.shuffle(&mut rng);

        // Write the page in random order
        for i in page_id_vec {
            let mut page = Page::new(i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            base_file.write_page(i, &page).unwrap();

            let mut read_page = Page::new_empty();
            base_file.read_page(i, &mut read_page).unwrap();

            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[test]
    fn test_base_file_file_flush() {
        // Create two file managers with the same path.
        // Issue multiple write operations to one of the file managers.
        // Check if the other file manager can read the pages.

        let temp_path = tempfile::tempdir().unwrap();
        let base_file1 = base_file_gen(temp_path.path());
        let base_file2 = base_file_gen(temp_path.path());

        let num_pages = 2;
        let mut page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        let mut rng = get_rng();
        page_id_vec.shuffle(&mut rng);

        // Write the page in random order
        for i in page_id_vec.iter() {
            let mut page = Page::new(*i);

            let data = format!("Hello, World! {}", i);
            page[0..data.len()].copy_from_slice(data.as_bytes());

            base_file1.write_page(*i, &page).unwrap();
        }

        base_file1.flush().unwrap(); // If we remove this line, the test is likely to fail.

        // Shuffle the page_id_vec again to read in random order
        page_id_vec.shuffle(&mut rng);

        // Read the page in random order
        for i in page_id_vec {
            let mut read_page = Page::new_empty();
            base_file2.read_page(i, &mut read_page).unwrap();

            let data = format!("Hello, World! {}", i);
            assert_eq!(&read_page[0..data.len()], data.as_bytes());
        }
    }

    #[test]
    fn test_base_file_concurrent_read_write_file() {
        let temp_path = tempfile::tempdir().unwrap();
        let base_file = base_file_gen(temp_path.path());

        let num_pages = 1000;
        let page_id_vec = (0..num_pages).collect::<Vec<PageId>>();

        let num_threads = 2;

        // Partition the page_id_vec into num_threads partitions.
        let partitions: Vec<Vec<PageId>> = {
            let mut partitions = vec![];
            let partition_size = num_pages / num_threads;
            for i in 0..num_threads {
                let start = (i * partition_size) as usize;
                let end = if i == num_threads - 1 {
                    num_pages
                } else {
                    (i + 1) * partition_size
                } as usize;
                partitions.push(page_id_vec[start..end].to_vec());
            }
            partitions
        };

        std::thread::scope(|s| {
            for mut partition in partitions.clone() {
                s.spawn(|| {
                    let mut rng = get_rng();
                    partition.shuffle(&mut rng);

                    for i in partition {
                        let mut page = Page::new(i);

                        let data = format!("Hello, World! {}", i);
                        page[0..data.len()].copy_from_slice(data.as_bytes());

                        base_file.write_page(i, &page).unwrap();
                    }
                });
            }
        });

        // Issue concurrent read
        std::thread::scope(|s| {
            for mut partition in partitions {
                s.spawn(|| {
                    let mut rng = get_rng();
                    partition.shuffle(&mut rng);
                    for i in partition {
                        let mut read_page = Page::new_empty();
                        base_file.read_page(i, &mut read_page).unwrap();

                        let data = format!("Hello, World! {}", i);
                        assert_eq!(&read_page[0..data.len()], data.as_bytes());
                    }
                });
            }
        });
        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().write_count(),
            num_pages,
            "write count should be {}",
            num_pages
        );
        #[cfg(feature = "stat")]
        assert_eq!(
            base_file.get_stats().read_count(),
            num_pages,
            "read count should be {}",
            num_pages
        );
    }
}
