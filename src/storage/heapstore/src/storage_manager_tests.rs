#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use crate::storage_manager::StorageManager as HeapStorageManager;
    use common::ids::{ContainerId, Permissions, TransactionId, ValueId};
    use common::physical::config::ServerConfig;
    use common::testutil::{
        compare_unordered_byte_vecs, gen_random_int, get_ascending_vec_of_byte_vec_02x,
        get_random_byte_vec, get_random_vec_of_byte_vec, get_rng, init,
    };
    use common::traits::storage_trait::StorageTrait;

    use common::util::vec_compare::compare_unordered;

    const RO: Permissions = Permissions::ReadOnly;

    fn get_test_sm<T: StorageTrait>() -> T {
        T::new_test_sm()
    }

    #[allow(dead_code)]
    fn get_sm<T: StorageTrait>(config: &'static ServerConfig) -> T {
        T::new(config)
    }

    #[test]
    fn sm_inserts() {
        let instance = get_test_sm::<HeapStorageManager>();
        let t = TransactionId::new();
        let sizes: Vec<usize> = vec![10, 50, 75, 100, 500, 1000];
        let mut rng = get_rng();

        for (i, size) in sizes.iter().enumerate() {
            let expected: Vec<Vec<u8>> = (0..*size)
                .map(|_| {
                    let size = gen_random_int(&mut rng, 50, 100);
                    get_random_byte_vec(&mut rng, size)
                })
                .collect();
            let cid = i as ContainerId;
            instance.create_table(cid).unwrap();
            instance.insert_values(cid, expected.clone(), t);
            let result: Vec<Vec<u8>> = instance.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
            assert!(compare_unordered(&expected, &result));
        }
    }

    #[test]
    fn sm_insert_delete() {
        let instance = get_test_sm::<HeapStorageManager>();
        let t = TransactionId::new();

        let mut rng = get_rng();
        let mut expected: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let size = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, size)
            })
            .collect();
        let cid = 1;
        instance.create_table(cid).unwrap();
        let mut val_ids = instance.insert_values(cid, expected.clone(), t);
        for _ in 0..10 {
            let idx_to_del = gen_random_int(&mut rng, 0, expected.len() - 1);
            instance.delete_value(val_ids[idx_to_del], t).unwrap();
            let result: Vec<Vec<u8>> = instance.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
            // Check that the value is not in the result
            println!("{:?}", expected.len());
            print!("\n\n\n\n\n\n\n");
            println!("{:?}", result.len());
            assert!(!compare_unordered(&expected, &result));
            expected.swap_remove(idx_to_del);
            val_ids.swap_remove(idx_to_del);
            assert!(compare_unordered(&expected, &result));
        }
    }

    #[test]
    fn sm_insert_updates() {
        let instance = get_test_sm::<HeapStorageManager>();
        let t = TransactionId::new();
        let mut rng = get_rng();
        let mut expected: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let size = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, size)
            })
            .collect();
        let cid = 1;
        instance.create_table(cid).unwrap();
        let mut val_ids = instance.insert_values(cid, expected.clone(), t);
        for _ in 0..10 {
            let idx_to_upd = gen_random_int(&mut rng, 0, expected.len() - 1);
            let new_bytes = get_random_byte_vec(&mut rng, 15);
            let new_val_id = instance
                .update_value(new_bytes.clone(), val_ids[idx_to_upd], t)
                .unwrap();
            expected[idx_to_upd] = new_bytes;
            let result: Vec<Vec<u8>> = instance.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
            assert!(compare_unordered(&expected, &result));
            val_ids[idx_to_upd] = new_val_id;
        }
    }

    #[test]
    #[should_panic]
    fn sm_no_container() {
        let instance = get_test_sm::<HeapStorageManager>();
        let t = TransactionId::new();
        instance.insert_value(1, vec![1, 2, 3, 4, 5], t);
    }

    #[test]
    fn test_not_found() {
        let instance = get_test_sm::<HeapStorageManager>();
        let t = TransactionId::new();
        let cid = 1 as ContainerId;
        instance.create_table(cid).unwrap();

        let val_id1 = ValueId::new_slot(cid, 1, 1);
        assert!(instance.get_value(val_id1, t, RO).is_err());
        assert!(instance.delete_value(val_id1, t).is_err());
        assert!(instance.update_value(vec![], val_id1, t).is_err());
    }

    #[test]
    fn sm_shutdown() {
        // create path if it doesn't exist
        let config: &'static ServerConfig = Box::leak(Box::new(ServerConfig::temporary()));
        let t = TransactionId::new();
        let instance1 = get_sm::<HeapStorageManager>(config);

        let mut rng = get_rng();
        let expected: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let random_int = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, random_int)
            })
            .collect();
        let cid = 1;
        instance1.create_table(cid).unwrap();
        let _val_ids = instance1.insert_values(cid, expected.clone(), t);
        instance1.shutdown();
        drop(instance1);

        let instance2 = get_sm::<HeapStorageManager>(config);
        let result: Vec<Vec<u8>> = instance2.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        assert!(compare_unordered(&expected, &result));
        instance2.reset().unwrap();
    }

    #[test]
    fn sm_shutdown_then_add_vals() {
        // create path if it doesn't exist
        let config: &'static ServerConfig = Box::leak(Box::new(ServerConfig::temporary()));
        let t = TransactionId::new();
        let instance1 = get_sm::<HeapStorageManager>(config);

        let mut rng = get_rng();
        let expected1: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let random_int = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, random_int)
            })
            .collect();
        let cid = 1;
        instance1.create_table(cid).unwrap();
        let _val_ids = instance1.insert_values(cid, expected1.clone(), t);
        instance1.shutdown();
        drop(instance1);

        let instance2 = get_sm::<HeapStorageManager>(config);

        // add more values to check accuracy across spin-ups
        let mut rng = get_rng();
        let expected2: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let random_int = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, random_int)
            })
            .collect();
        let cid = 1;
        let _val_ids = instance2.insert_values(cid, expected2.clone(), t);
        let result: Vec<Vec<u8>> = instance2.get_iterator(cid, t, RO).map(|(a, _)| a).collect();

        let mut expected = expected1;
        expected.extend(expected2);
        assert!(compare_unordered(&expected, &result));
        instance2.reset().unwrap();
    }

    #[test]
    fn sm_shutdown_check_for_wasted_page() {
        // create path if it doesn't exist
        let config: &'static ServerConfig = Box::leak(Box::new(ServerConfig::temporary()));
        let t = TransactionId::new();
        let instance1 = get_sm::<HeapStorageManager>(config);

        let mut rng = get_rng();
        let expected1: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let random_int = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, random_int)
            })
            .collect();
        let cid = 1;
        instance1.create_table(cid).unwrap();
        let _val_ids = instance1.insert_values(cid, expected1.clone(), t);
        instance1.shutdown();
        drop(instance1);

        let instance2 = get_sm::<HeapStorageManager>(config);

        // add more values
        let mut rng = get_rng();
        let expected2: Vec<Vec<u8>> = (0..100)
            .map(|_| {
                let random_int = gen_random_int(&mut rng, 50, 100);
                get_random_byte_vec(&mut rng, random_int)
            })
            .collect();
        let cid = 1;
        let _val_ids = instance2.insert_values(cid, expected2.clone(), t);
        let result: Vec<Vec<u8>> = instance2.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        let mut expected = expected1;
        expected.extend(expected2);
        assert!(compare_unordered(&expected, &result));

        // make sure that sm spin up didn't try to write new metadata pages to existing cid in bp
        // ^ we do this by checking that all data pages are consecutive
        let mut pid = 0;
        for (_, vid) in instance2.get_iterator(cid, t, RO) {
            let cur_pid = vid.page_id.unwrap();
            assert!(cur_pid - pid == 1 || cur_pid == pid);
            pid = cur_pid;
        }

        instance2.reset().unwrap();
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = HeapStorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        let mut rng = get_rng();
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(&mut rng, 400),
            get_random_byte_vec(&mut rng, 400),
            get_random_byte_vec(&mut rng, 400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(&mut rng, 400),
            get_random_byte_vec(&mut rng, 400),
            get_random_byte_vec(&mut rng, 400),
            get_random_byte_vec(&mut rng, 400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(&mut rng, 300),
            get_random_byte_vec(&mut rng, 500),
            get_random_byte_vec(&mut rng, 400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    fn hs_sm_1() {
        init();
        let large_size = (common::PAGE_SIZE - 30) / 4;
        let mut rng = get_rng();
        let vals = get_random_vec_of_byte_vec(&mut rng, 20, large_size, large_size);
        let sm = HeapStorageManager::new_test_sm();
        let tid = TransactionId::new();
        sm.create_table(0).unwrap();
        let val_ids = sm.insert_values(0, vals, tid);
        let mut counter: common::prelude::PageId = 0;
        for (i, v) in val_ids.iter().enumerate() {
            if i % 3 == 0 {
                counter += 1;
            }
            assert_eq!(v.page_id.unwrap(), counter);
        }
    }

    #[test]
    fn hs_sm_2() {
        init();
        let large_size = (common::PAGE_SIZE - 30) / 4;
        let mut rng = get_rng();
        let mut vals = get_ascending_vec_of_byte_vec_02x(&mut rng, 20, large_size, large_size);
        let sm = HeapStorageManager::new_test_sm();
        let tid = TransactionId::new();
        sm.create_table(0).unwrap();
        let mut val_ids = sm.insert_values(0, vals.clone(), tid);
        // Vec of what to keep from the original list of values and value ids
        let mut vals_to_del: Vec<bool> = Vec::new();
        let mut counter: common::prelude::PageId = 0;

        for (i, v) in val_ids.iter().enumerate() {
            debug!("{} {:?}", counter / 4, v);
            if i % 3 == 0 {
                counter += 1;
            }
            assert_eq!(v.page_id.unwrap(), counter);
            //delete all for page 1
            if v.page_id.unwrap() == 1 {
                let del_res = sm.delete_value(*v, tid);
                assert!(del_res.is_ok());
                vals_to_del.push(false);
            } else {
                vals_to_del.push(true);
            }
        }
        let mut keep_iter = vals_to_del.iter();
        vals.retain(|_| *keep_iter.next().unwrap());
        keep_iter = vals_to_del.iter();
        val_ids.retain(|_| *keep_iter.next().unwrap());

        // let check_vals: Vec<Vec<u8>> = sm.get_iterator(0, tid, RO).map(|(a, _)| a).collect();
        // let check_vals: Vec<(Vec<u8>, ValueId)> = sm.get_iterator(0, tid, RO).collect();
        let (check_vals, _check_val_ids): (Vec<Vec<u8>>, Vec<ValueId>) =
            sm.get_iterator(0, tid, Permissions::ReadOnly).unzip();

        // For debugging. Take a slice of each element to keep smaller for printing
        // let vals_smaller: Vec<&[u8]> = vals.iter().map(|f| &f[..5]).collect();
        // let cv_smaller: Vec<&[u8]> = check_vals.iter().map(|f| &f[..5]).collect();
        assert!(compare_unordered_byte_vecs(&vals, check_vals));
    }
}
