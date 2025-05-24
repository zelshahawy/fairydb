use clap::Parser;
use e2e_tests::sqllogictest_utils::{run_sqllogictests_from_files, SQLLogicTestConfig};

fn main() {
    env_logger::init();

    let config = SQLLogicTestConfig::parse();
    let (succeeded, failed) = run_sqllogictests_from_files(config);

    // Print summary
    println!("================ SUMMARY ================");
    println!("Succeeded tests: {:?}", succeeded);
    println!("Failed tests: {:?}", failed);

    if !failed.is_empty() {
        panic!("Some tests failed");
    }
}
