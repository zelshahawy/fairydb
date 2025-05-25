use common::physical::tuple_conv0::{self};
use common::physical::tuple_writer::TupleConverterTrait;
use common::physical::{tuple_conv1, tuple_conv2, tuple_conv3};
use common::testutil::{gen_random_tuples_fixed, gen_random_tuples_var_and_null, get_rng};
use common::{TableSchema, Tuple};
use criterion::{criterion_group, criterion_main};

use criterion::{black_box, Criterion};

const N: usize = 5000;
const BUF_SIZE: usize = 2_000_000;

fn bench_conv<T>((tuple_conv0, mut buf, _schema, tuples): (T, Vec<u8>, TableSchema, Vec<Tuple>))
where
    T: TupleConverterTrait,
{
    let mut offset = 0;
    for t in tuples.iter() {
        let res = tuple_conv0.write_tuple(t, &mut buf, offset);
        if res.is_none() {
            panic!("write_tuple returned None");
        }
        let len = res.unwrap();
        if len == 0 {
            panic!("write_tuple returned 0");
        }
        let _tuple2 = tuple_conv0.read_tuple(&buf, offset, len).unwrap();
        // assert_eq!(t, &tuple2);
        offset += len;
    }
}

pub fn tuple_bench(c: &mut Criterion) {
    let mut rng = get_rng();
    let attrs = 10;
    let null_prob = 0.1;

    c.bench_function("fixed_tuple_conv0", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) = gen_random_tuples_fixed(&mut rng, N, attrs - 1);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv0 = tuple_conv0::TupleConverter0::new(schema.clone());
                (tuple_conv0, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("var_tuple_conv0", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) =
                    gen_random_tuples_var_and_null(&mut rng, N, attrs - 1, null_prob);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv0 = tuple_conv0::TupleConverter0::new(schema.clone());
                (tuple_conv0, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("fixed_tuple_conv1", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) = gen_random_tuples_fixed(&mut rng, N, attrs - 1);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv1 = tuple_conv1::TupleConverter1::new(schema.clone());
                (tuple_conv1, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("var_tuple_conv1", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) =
                    gen_random_tuples_var_and_null(&mut rng, N, attrs - 1, null_prob);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv = tuple_conv1::TupleConverter1::new(schema.clone());
                (tuple_conv, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("fixed_tuple_conv2", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) = gen_random_tuples_fixed(&mut rng, N, attrs - 1);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv2 = tuple_conv2::TupleConverter2::new(schema.clone());
                (tuple_conv2, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("var_tuple_conv2", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) =
                    gen_random_tuples_var_and_null(&mut rng, N, attrs - 1, null_prob);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv = tuple_conv2::TupleConverter2::new(schema.clone());
                (tuple_conv, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("fixed_tuple_conv3", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) = gen_random_tuples_fixed(&mut rng, N, attrs - 1);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv3 = tuple_conv3::TupleConverter3::new(schema.clone());
                (tuple_conv3, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });

    c.bench_function("var_tuple_conv3", |b| {
        b.iter_batched(
            || {
                let (schema, tuples) =
                    gen_random_tuples_var_and_null(&mut rng, N, attrs - 1, null_prob);
                let buffer = vec![0u8; BUF_SIZE];
                let tuple_conv = tuple_conv3::TupleConverter3::new(schema.clone());
                (tuple_conv, buffer, schema, tuples)
            },
            |data| bench_conv(black_box(data)),
            criterion::BatchSize::PerIteration,
        )
    });
}

criterion_group!(benches, tuple_bench);
criterion_main!(benches);
