use common::testutil::{get_random_vec_of_byte_vec, get_rng};
use common::traits::storage_trait::StorageTrait;
use criterion::{black_box, BatchSize, Criterion};
use heapstore::storage_manager::StorageManager;
use heapstore::testutil::{bench_hs_mixed, bench_sm_insert, gen_hf_bench_workload};

pub fn sm_mixed_bench(c: &mut Criterion) {
    let mut rng = get_rng();

    c.bench_function("sm mixed ops - fixed size 5k", |b| {
        b.iter_batched(
            // SETUP: new, empty StorageManager for *this* iteration
            || {
                let sm = StorageManager::new_test_sm();
                let workload = gen_hf_bench_workload(&mut rng, 5000, 100, 100);
                (sm, workload)
            },
            // MEASURE: do exactly your insert
            |(sm, workload)| {
                bench_hs_mixed(&sm, black_box(&workload));
            },
            // How often you want Criterion to regenerate the state—
            // SmallInput is a good default for cheap setups:
            BatchSize::PerIteration,
        )
    });

    c.bench_function("sm mixed ops - var size 5k", |b| {
        b.iter_batched(
            // SETUP: new, empty StorageManager for *this* iteration
            || {
                let sm = StorageManager::new_test_sm();
                let workload = gen_hf_bench_workload(&mut rng, 5000, 50, 100);
                (sm, workload)
            },
            // MEASURE: do exactly your insert
            |(sm, workload)| {
                bench_hs_mixed(&sm, black_box(&workload));
            },
            // How often you want Criterion to regenerate the state—
            // SmallInput is a good default for cheap setups:
            BatchSize::PerIteration,
        )
    });
}

pub fn sm_ins_bench(c: &mut Criterion) {
    let mut rng = get_rng();
    let to_insert = get_random_vec_of_byte_vec(&mut rng, 1000, 80, 100);
    let cid = 1;

    c.bench_function("sm insert 1k", |b| {
        b.iter_batched(
            // SETUP: new, empty StorageManager for *this* iteration
            || {
                let sm = StorageManager::new_test_sm();
                sm.create_table(cid).unwrap();
                sm
            },
            // MEASURE: do exactly your insert
            |sm| {
                bench_sm_insert(&sm, black_box(&to_insert));
            },
            // How often you want Criterion to regenerate the state—
            // SmallInput is a good default for cheap setups:
            BatchSize::SmallInput,
        )
    });
}
