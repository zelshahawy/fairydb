// benches/ops_bench.rs
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use heapstore::buffer_pool::{
    buffer_pool::{gen_random_pathname, BufferPool},
    mem_pool_trait::{MemPool, PageFrameId},
};
use heapstore::container_file_catalog::ContainerFileCatalog;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
    sync::Arc,
};

type Op = (u8 /*opcode*/, u32 /*c_id*/, u32 /*p_id*/);

/// Scan for `prefix` ("c:" or "p:") and return the number after it.
/// Returns `None` if the prefix isn't present.
fn scan_number(line: &str, prefix: &str) -> Option<u32> {
    let start = line.find(prefix)? + prefix.len();
    let bytes = line.as_bytes();
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    line[start..end].parse().ok()
}

fn parse_ops<P: AsRef<std::path::Path>>(path: P) -> io::Result<Vec<Op>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut vec = Vec::new();
    for line in reader.lines() {
        // if _i % 1000 == 0 {
        //     println!("Parsed {} lines", i);
        // }
        let line = line?;

        let opcode = if line.contains("Page create") {
            0u8
        } else if line.contains("Page read") {
            1u8
        } else if line.contains("Page write") {
            2u8
        } else {
            continue;
        };

        // SAFETY: log lines are trusted ASCII; parsing errors -> panic
        let c_id = scan_number(&line, "c:").expect("missing c:");
        let p_id = match scan_number(&line, "p:") {
            Some(id) => id,
            None => u32::MAX, // create
        };

        vec.push((opcode, c_id, p_id));
    }
    Ok(vec)
}

/// 👉 replace this with the real function you want to time
fn simulate_tpcc(bp: &mut Arc<BufferPool>, input: &[Op]) {
    for op in input {
        match op {
            (0, c_id, _) => {
                let result = bp.create_new_page_for_write(*c_id as u16).unwrap();
                black_box(result);
            }
            (1, c_id, p_id) => {
                let result = bp
                    .get_page_for_read(PageFrameId::new(*c_id as u16, *p_id))
                    .unwrap();
                black_box(result);
            }
            (2, c_id, p_id) => {
                let result = bp
                    .get_page_for_write(PageFrameId::new(*c_id as u16, *p_id))
                    .unwrap();
                black_box(result);
            }
            _ => {
                panic!("Unknown operation code: {:?}", op);
            }
        }
    }
}
fn bench_eviction_policy(c: &mut Criterion, file_name: &str) {
    // ───── one-time setup ───────────────────────────────────────────────
    let ops = parse_ops(file_name).expect("failed to parse log");
    let bench_name = format!("run_tpcc_{}", file_name);

    // ───── actual benchmark ─────────────────────────────────────────────
    c.bench_function(&bench_name, |b| {
        b.iter_batched_ref(
            || {
                let base_dir = gen_random_pathname(Some("bench_tpcc"));
                let cfc = Arc::new(ContainerFileCatalog::new(base_dir, true).unwrap());
                for i in 0..15 {
                    // Create TPC-C tables and add 10000 pages to each.
                    cfc.register_container(i, false);
                    cfc.get_container(i).inc_page_count(10000);
                }
                Arc::new(BufferPool::new(8 * 1024, cfc).unwrap())
            },
            |bp| {
                simulate_tpcc(black_box(bp), black_box(&ops));
            },
            BatchSize::SmallInput,
        )
    });
}

fn bench_w2(c: &mut Criterion) {
    bench_eviction_policy(c, "w2_d2.txt")
}
fn bench_w4(c: &mut Criterion) {
    bench_eviction_policy(c, "w4_d2.txt")
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_w2, bench_w4
}
criterion_main!(benches);
