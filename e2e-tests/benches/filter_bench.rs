use cli_crusty::Response;
use common::QueryResult;
use criterion::{criterion_group, criterion_main, Criterion};
use e2e_tests::test_db::TestDB;

// Measuring commands should not modify the database itself
fn bench_template(
    c: &mut Criterion,
    bench_name: &str,
    setup_commands: Vec<&str>,
    measuring_commands: Vec<(&str, usize)>,
) {
    let mut db = TestDB::new();
    for command in setup_commands {
        let res = db.send_command(command.as_bytes());
        assert_eq!(res.len(), 1);
        if matches!(
            res[0],
            Response::SystemErr(_) | Response::QuietErr | Response::QueryExecutionError(_)
        ) {
            panic!("Error in setup command: {}", command);
        }
    }

    c.bench_function(bench_name, |b| {
        b.iter(|| {
            for (command, result_size) in &measuring_commands {
                let res = db.send_command(command.as_bytes());
                assert_eq!(res.len(), 1);
                match &res[0] {
                    Response::QueryResult(QueryResult::Select { result, .. }) => {
                        assert_eq!(result.len(), *result_size);
                    }
                    _ => {
                        panic!("Error in measuring command: {}", command);
                    }
                }
            }
        });
    });
}

fn bench_filter_large(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/large_data.csv testA",
    ];
    let measuring_commands = vec![("select * from testA where a < 500", 500)];
    bench_template(c, "filter_large", setup_commands, measuring_commands);
}

criterion_group! {
    name = filter_bench;
    config = Criterion::default().sample_size(10);
    targets = bench_filter_large,
}

criterion_main!(filter_bench);
