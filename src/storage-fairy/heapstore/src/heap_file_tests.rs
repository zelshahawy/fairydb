#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use std::sync::Arc;

    use crate::buffer_pool::buffer_pool::get_test_bp;

    use crate::heap_file::*;

    fn gen_bytes(key: usize, size: usize) -> Vec<u8> {
        let mut bytes = vec![0; size];
        bytes[0..std::mem::size_of::<usize>()].copy_from_slice(&key.to_be_bytes());
        bytes
    }

    fn gen_values(num: usize) -> Vec<Vec<u8>> {
        (0..num)
            .map(|i| gen_bytes(i, 100))
            .collect::<Vec<Vec<u8>>>()
    }

    #[cfg(feature = "hs_33500")]
    const BP_FRAMES: usize = 100;
    #[cfg(not(feature = "hs_33500"))]
    const BP_FRAMES: usize = 1000;

    #[test]
    fn hs_hf_one_insertion_one_lookup() {
        let cid = 0;
        let bp = get_test_bp(BP_FRAMES);
        let hf = Arc::new(HeapFile::new(cid, bp.clone()).unwrap());

        let to_insert = gen_values(1)[0].clone();
        let val_id = hf.add_val(&to_insert).unwrap();

        let val = hf
            .get_val(val_id.page_id.unwrap(), val_id.slot_id.unwrap())
            .unwrap();
        assert_eq!(val, to_insert);
        #[cfg(not(feature = "hs_33500"))]
        assert!(bp.count_empty_frames() > 0 && bp.disk_size() == 0);
    }

    #[test]
    fn hs_hf_insert_ge_lookup() {
        let cid = 0;
        let bp = get_test_bp(BP_FRAMES);
        let hf = Arc::new(HeapFile::new(cid, bp.clone()).unwrap());

        let to_insert = gen_values(10000);
        let val_ids = hf.add_vals(to_insert.clone().into_iter()).unwrap();
        assert_eq!(val_ids.len(), to_insert.len());

        for i in 0..to_insert.len() {
            let val = hf
                .get_val(val_ids[i].page_id.unwrap(), val_ids[i].slot_id.unwrap())
                .unwrap();
            assert_eq!(val, to_insert[i], "Failed at val_id: {:?}", val_ids[i]);
        }
        #[cfg(not(feature = "hs_33500"))]
        assert!(bp.count_empty_frames() > 0 && bp.disk_size() == 0);
        #[cfg(feature = "hs_33500")]
        assert!(bp.disk_size() > 0, "Eviction not working");
    }

    #[test]
    fn hs_hfiter_insert_and_iterate() {
        let cid = 0;
        let bp = get_test_bp(BP_FRAMES);
        let hf = Arc::new(HeapFile::new(cid, bp.clone()).unwrap());

        let to_insert = gen_values(10000);
        let val_ids = hf.add_vals(to_insert.clone().into_iter()).unwrap();
        assert_eq!(val_ids.len(), to_insert.len());

        // Zip the inserted values with the hf iterator
        let mut iter = hf.iter();
        for (hf_iter_result, expected) in iter.by_ref().zip(to_insert.iter()) {
            let (val, val_id) = hf_iter_result;
            assert_eq!(&val, expected, "Failed at val_id: {:?}", val_id);
        }
        #[cfg(not(feature = "hs_33500"))]
        assert!(bp.count_empty_frames() > 0 && bp.disk_size() == 0);
        #[cfg(feature = "hs_33500")]
        assert!(bp.disk_size() > 0, "Eviction not working");
    }

    #[test]
    fn hs_hfiter_insert_drop_and_load() {
        let cid = 0;
        let bp = get_test_bp(BP_FRAMES);
        let hf = Arc::new(HeapFile::new(cid, bp.clone()).unwrap());

        let to_insert = gen_values(10000);
        let val_ids = hf.add_vals(to_insert.clone().into_iter()).unwrap();
        assert_eq!(val_ids.len(), to_insert.len());

        drop(hf);

        let hf = Arc::new(HeapFile::load(cid, bp.clone()).unwrap());
        let mut iter = hf.iter();
        for (hf_iter_result, expected) in iter.by_ref().zip(to_insert.iter()) {
            let (val, val_id) = hf_iter_result;
            assert_eq!(&val, expected, "Failed at val_id: {:?}", val_id);
        }
        #[cfg(not(feature = "hs_33500"))]
        assert!(bp.count_empty_frames() > 0 && bp.disk_size() == 0);
        #[cfg(feature = "hs_33500")]
        assert!(bp.disk_size() > 0, "Eviction not working");
    }
}
