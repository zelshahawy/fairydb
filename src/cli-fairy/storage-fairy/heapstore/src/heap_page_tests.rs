#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::page::Page;
    use crate::page::PAGE_FIXED_HEADER_LEN;

    use crate::heap_page::*;
    use common::ids::SlotId;
    use common::testutil::init;
    use common::testutil::*;
    use common::PAGE_SIZE;
    use rand::Rng;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = HEAP_PAGE_FIXED_METADATA_SIZE + PAGE_FIXED_HEADER_LEN;
    pub const HEADER_PER_VAL_SIZE: usize = SLOT_METADATA_SIZE;

    /// This is a test for helping to debug the page by using records of ascending values
    #[test]
    fn debug_page_insert() {
        init();
        let mut p = Page::new(0);
        let mut rng = get_rng();
        p.init_heap_page();
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(&mut rng, n, size, size);
        for x in &vals {
            p.add_value(x);
        }
        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
        );
    }

    #[test]
    fn hs_page_sizes_header_free_space() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        assert_eq!(0, p.get_page_id());

        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(5);
        p.init_heap_page();
        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        let byte_len = tuple_bytes.len();
        assert_eq!(byte_len, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_free_space()
        );
        let tuple_bytes2 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        let size = 10;
        let mut rng = get_rng();
        let bytes = get_random_byte_vec(&mut rng, size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);

        let tuple_bytes2 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        p.init_heap_page();
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let mut rng = get_rng();
        let bytes = get_random_byte_vec(&mut rng, 10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        p.init_heap_page();
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let mut rng = get_rng();
        let bytes = get_random_byte_vec(&mut rng, byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_free_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        let size = PAGE_SIZE / 4;
        let mut rng = get_rng();
        let bytes = get_random_byte_vec(&mut rng, size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(&mut rng, size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);

        let tuple_bytes2 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_simple_update() {
        init();
        let mut p = Page::new(2);
        p.init_heap_page();
        let mut rng = get_rng();

        // Get 10 records of size 100 each
        let vals = get_ascending_vec_of_byte_vec_02x(&mut rng, 10, 100, 100);
        // Add them to the page
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(Some(i as SlotId), p.add_value(val));
        }
        // Check that we can get them back
        for (i, val) in vals.iter().enumerate() {
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(val, &check_bytes);
        }
        // Check that we can update them
        for i in 0..vals.len() {
            let new_bytes = get_random_byte_vec(&mut rng, 100);
            assert_eq!(Some(()), p.update_value(i as SlotId, &new_bytes));
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(new_bytes, check_bytes);
        }
    }

    #[test]
    fn hs_page_update() {
        init();
        let mut p = Page::new(3);
        p.init_heap_page();
        let mut rng = get_rng();

        // Get 10 records of size 100 each
        let mut vals = get_ascending_vec_of_byte_vec_02x(&mut rng, 20, 100, 100);
        // Add them to the page
        for (i, val) in vals.iter().enumerate() {
            assert_eq!(Some(i as SlotId), p.add_value(val));
        }
        // Check that we can get them back
        for (i, val) in vals.iter().enumerate() {
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(val, &check_bytes);
        }

        // Delete the second value
        assert_eq!(Some(()), p.delete_value(1));
        // Check that we can get the rest back
        for (i, val) in vals.iter().enumerate() {
            if i == 1 {
                continue;
            }
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(val, &check_bytes);
        }

        // Check that we can update the rest
        #[allow(clippy::needless_range_loop)]
        for i in 0..vals.len() {
            if i == 1 {
                continue;
            }
            let new_bytes = get_random_byte_vec(&mut rng, 100);
            assert_eq!(Some(()), p.update_value(i as SlotId, &new_bytes));
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(new_bytes, check_bytes);
            vals[i] = new_bytes.clone();
        }

        // Check the values again
        for (i, val) in vals.iter().enumerate() {
            if i == 1 {
                continue;
            }
            let check_bytes = p.get_value(i as SlotId).unwrap();
            assert_eq!(val, &check_bytes);
        }

        // Update 2nd value should be none
        let new_bytes = get_random_byte_vec(&mut rng, 100);
        assert_eq!(None, p.update_value(1, &new_bytes));

        // Update the 4th value to be larger
        let new_bytes = get_random_byte_vec(&mut rng, 200);
        assert_eq!(Some(()), p.update_value(4, &new_bytes));
        let check_bytes = p.get_value(4).unwrap();
        assert_eq!(new_bytes, check_bytes);
        vals[4] = new_bytes.clone();
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();

        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 20);
        let tuple_bytes2 = get_random_byte_vec(&mut rng, 20);
        let tuple_bytes3 = get_random_byte_vec(&mut rng, 20);
        let tuple_bytes4 = get_random_byte_vec(&mut rng, 20);
        let tuple_bytes_big = get_random_byte_vec(&mut rng, 40);
        let tuple_bytes_small1 = get_random_byte_vec(&mut rng, 5);
        let tuple_bytes_small2 = get_random_byte_vec(&mut rng, 5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        p.init_heap_page();
        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        p.init_heap_page();
        let mut rng = get_rng();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple_bytes2 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let mut p2 = p.clone();
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        let check_bytes = p2.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);

        //Add a new tuple to the new page
        let tuple_bytes3 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_serialization_is_deterministic() {
        init();

        // Create a page and serialize it
        let mut p0 = Page::new(0);
        p0.init_heap_page();
        let mut rng = get_rng();
        let bytes = get_random_byte_vec(&mut rng, 100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(&mut rng, 100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(&mut rng, 100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        // Reconstruct the page
        let p1 = Page::from_bytes(*p0_bytes);
        let p1_bytes = p1.to_bytes();

        // Enforce that the two pages serialize determinestically
        assert_eq!(p0_bytes, p1_bytes);
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let mut rng = get_rng();
        p.init_heap_page();
        let tuple_bytes = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple_bytes2 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple_bytes3 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple_bytes4 = get_random_byte_vec(&mut rng, 45);
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = [
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = *p.to_bytes();

        // Test iteration 1
        let mut iter = p.iter();
        assert_eq!(Some((tuple_bytes.as_slice(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.as_slice(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.as_slice(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.as_slice(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(page_bytes);
        assert_eq!(Some(tuple_bytes.as_slice()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
        }

        let p = Page::from_bytes(page_bytes);
        let mut count = 0;
        for _ in &p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.to_bytes();
        count = 0;
        for _ in &p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(*page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.as_slice(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.as_slice(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.as_slice(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.as_slice(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let mut rng = get_rng();
        let values = get_ascending_vec_of_byte_vec_02x(&mut rng, 6, size, size);
        let mut p = Page::new(0);
        p.init_heap_page();
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let mut rng = get_rng();
        let values = get_ascending_vec_of_byte_vec_02x(&mut rng, 8, size, size);
        let larger_val = get_random_byte_vec(&mut rng, size * 2 - 20);
        let mut p = Page::new(0);
        p.init_heap_page();
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let mut rng = get_rng();
        let values = [
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size / 4),
        ];
        let mut p = Page::new(0);
        p.init_heap_page();
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let mut rng = get_rng();
        let values = [
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
            get_random_byte_vec(&mut rng, size),
        ];
        let mut p = Page::new(0);
        p.init_heap_page();
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let mut p2 = p.clone();
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let mut p3 = p2.clone();
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let p4 = p3.clone();
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        init();
        let mut p = Page::new(23);
        p.init_heap_page();
        let mut rng = get_rng();
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(&mut rng, 300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }
        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let p_clone = p.clone();
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a.to_vec()).collect();
        print!("Stored slots {:?}", stored_vals);
        print!("#########\n\n\n\n\n #########\n");
        print!("Stored vals {:?}", check_vals);
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let p_clone = p.clone();
                        check_vals = p_clone.iter().map(|(a, _)| a.to_vec()).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.random_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }
}
