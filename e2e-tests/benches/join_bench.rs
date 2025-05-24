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

fn bench_join_tiny(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/tiny_data.csv testA",
        "CREATE TABLE testB (a INT, b INT, primary key (a));",
        "\\i csv/tiny_data.csv testB",
    ];

    let measuring_commands = vec![("select * from testA join testB on testA.a = testB.a", 4)];

    bench_template(c, "join_tiny", setup_commands, measuring_commands);
}

fn bench_join_small(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/small_data.csv testA",
        "CREATE TABLE testB (a INT, b INT, primary key (a));",
        "\\i csv/small_data.csv testB",
    ];

    let measuring_commands = vec![("select * from testA join testB on testA.a = testB.a", 21)];

    bench_template(c, "join_small", setup_commands, measuring_commands);
}

fn bench_join_right(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/right_data.csv testA",
        "CREATE TABLE testB (a INT, b INT, primary key (a));",
        "\\i csv/left_data.csv testB",
    ];

    let measuring_commands = vec![("select * from testA join testB on testA.a = testB.a", 100)];
    bench_template(c, "join_right", setup_commands, measuring_commands);
}

fn bench_join_left(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/right_data.csv testA",
        "CREATE TABLE testB (a INT, b INT, primary key (a));",
        "\\i csv/left_data.csv testB",
    ];
    let measuring_commands = vec![("select * from testB join testA on testB.a = testA.a", 100)];
    bench_template(c, "join_left", setup_commands, measuring_commands);
}

fn bench_join_large(c: &mut Criterion) {
    let setup_commands = vec![
        "CREATE TABLE testA (a INT, b INT, primary key (a));",
        "\\i csv/large_data.csv testA",
        "CREATE TABLE testB (a INT, b INT, primary key (a));",
        "\\i csv/large_data.csv testB",
    ];
    let measuring_commands = vec![("select * from testA join testB on testB.a = testA.a", 1000)];
    bench_template(c, "join_large", setup_commands, measuring_commands);
}

criterion_group! {
    name = join_bench;
    config = Criterion::default().sample_size(10);
    targets =
    bench_join_tiny,
    bench_join_small,
    bench_join_right,
    bench_join_left,
    bench_join_large,
}

criterion_main!(join_bench);
