use criterion::{criterion_group, criterion_main};

use criterion::{black_box, Criterion};

use common::testutil::get_rng;
use heapstore::testutil::{bench_page_mixed, gen_page_bench_workload};

pub fn heap_page_mixed_benchmark(c: &mut Criterion) {
    let mut rng = get_rng();
    let workload = gen_page_bench_workload(&mut rng, 5000, 20, 40);
    c.bench_function("heap_page_mixed", |b| {
        b.iter(|| bench_page_mixed(black_box(&workload)))
    });
}

criterion_group!(benches, heap_page_mixed_benchmark);
criterion_main!(benches);
