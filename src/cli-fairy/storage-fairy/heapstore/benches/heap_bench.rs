use criterion::{criterion_group, criterion_main};

mod sm_bench;

criterion_group!(benches, sm_bench::sm_ins_bench, sm_bench::sm_mixed_bench);
criterion_main!(benches);
