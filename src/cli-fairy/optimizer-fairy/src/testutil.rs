use common::{
    catalog::{Catalog, CatalogRef},
    ids::{StateType, TransactionId, ValueId},
    physical::{
        col_id_generator::ColIdGenerator, config::ServerConfig, small_string::StringManager,
    },
    query::rules::Rules,
    table::TableInfo,
    testutil::{create_tuple_list, gen_test_table_and_tuples, get_rng},
    traits::{stat_manager_trait::StatManagerTrait, storage_trait::StorageTrait},
    DataType, Field, TableSchema, Tuple,
};
use itertools::Itertools;
use queryexe::{
    opiterator::OpIterator,
    query::{planner::physical_plan_to_op_iterator, Translator},
    stats::reservoir_stat_manager::ReservoirStatManager,
    IndexManager, Managers, StorageManager, TransactionManager,
};
use std::{fmt::Debug, sync::Arc};

use crate::{
    cascades_optimizer::CascadesOptimizer,
    cost::{cardinality_cost_model::CardinalityCostModel, dummy_cost_model::DummyCost},
    memo::Memo,
};

const T1_TABLE_NAME: &str = "t1";
const T1_WIDTH: usize = 5;
const A_COLS: [&str; T1_WIDTH] = ["a", "b", "p", "q", "r"];
const A_TYPES: [DataType; T1_WIDTH] = [
    DataType::BigInt,
    DataType::BigInt,
    DataType::BigInt,
    DataType::BigInt,
    DataType::BigInt,
];

const T2_TABLE_NAME: &str = "t2";
const T2_WIDTH: usize = 2;
const B_COLS: [&str; T2_WIDTH] = ["c", "d"];
const B_TYPES: [DataType; T2_WIDTH] = [DataType::BigInt, DataType::BigInt];

const T3_TABLE_NAME: &str = "t3";
const T3_WIDTH: usize = 2;
const C_COLS: [&str; T3_WIDTH] = ["e", "f"];
const C_TYPES: [DataType; T3_WIDTH] = [DataType::BigInt, DataType::BigInt];

const A_ROWS: [[i64; T1_WIDTH]; 5] = [
    [1, 2, 3, 4, 5],
    [2, 3, 4, 5, 6],
    [3, 4, 5, 6, 7],
    [4, 5, 6, 7, 8],
    [5, 6, 7, 8, 9],
];
const A_LENGTH: usize = A_ROWS.len();

const B_ROWS: [[i64; T2_WIDTH]; 5] = [[1, 2], [2, 3], [3, 4], [4, 5], [5, 6]];
const B_LENGTH: usize = B_ROWS.len();

const C_ROWS: [[i64; T3_WIDTH]; 5] = [[1, 2], [2, 3], [3, 4], [4, 5], [5, 6]];
const C_LENGTH: usize = C_ROWS.len();

pub fn init() {
    // To change the log level for tests change the filter_level
    let _ = env_logger::builder()
        .is_test(true) // comment out this line to see logs
        // .filter_level(log::LevelFilter::Trace)
        // .filter_level(log::LevelFilter::Info)
        // .filter_module("optimizer", log::LevelFilter::Trace)
        .try_init();
}

pub fn get_results(op: &mut Box<dyn OpIterator>) -> Vec<Tuple> {
    std::iter::from_fn(|| op.next().unwrap()).collect::<Vec<Tuple>>()
}

pub fn tuple_to_vec_i64(row: &Tuple) -> Vec<i64> {
    row.field_vals
        .iter()
        .map(|f| match f {
            Field::BigInt(i) => *i,

            // TODO: Not sure if should be f32 or f64
            Field::Decimal(..) => format!("{}", f).parse::<f32>().unwrap() as i64,

            _ => panic!("Expected int or decimal"),
        })
        .collect::<Vec<i64>>()
}

pub fn check_equality_ignore_order<T: Ord + Eq + Clone + Debug>(
    expected: &[Vec<T>],
    actual: &[Vec<T>],
) {
    // First sort expected and actual
    let mut expected = expected.to_vec();
    expected.sort();
    let mut actual = actual.to_vec();
    actual.sort();

    assert_eq!(expected, actual);
}

/// Concatenate tables. For example, if table_a is vec![vec![1, 2], vec![3, 4]] and table_b is
/// vec![vec![5, 6], vec![7, 8]], then the result will be vec![vec![1, 2, 5, 6], vec![3, 4, 7, 8]].
fn concatenate_tables<T: Clone>(tables: &[&Vec<Vec<T>>]) -> Vec<Vec<T>> {
    let mut result = Vec::new();
    for i in 0..tables[0].len() {
        let mut row = Vec::new();
        for table in tables {
            row.extend(table[i].clone());
        }
        result.push(row);
    }
    result
}

/// Only use this function when the order of the tables in the expected_tables
/// does not matter. For example, "SELECT * FROM t1 JOIN t2 ON t1.a = t2.c";`
/// can return t1 | t2 or t2 | t1.
pub fn check_join_equality_ignore_order<T: Ord + Eq + Clone + Debug>(
    expected_tables: &[Vec<Vec<T>>],
    actual: &[Vec<T>],
) {
    // for example, can be (table a | table b) or (table b | table a)
    let permutations = expected_tables.iter().permutations(expected_tables.len());
    for perm in permutations {
        let mut expected = concatenate_tables(&perm);
        println!("Expected: {:?}", expected);

        // sort expected and actual
        expected.sort();
        let mut actual = actual.to_vec();
        actual.sort();

        if expected == actual {
            return;
        }
    }

    panic!(
        "Expected concatenation (any permutation) of: {:?}\nActual: {:?}",
        expected_tables, actual
    );
}

pub fn get_test_catalog() -> Arc<Catalog> {
    let catalog = Catalog::new();
    let t1_table_name = String::from(T1_TABLE_NAME);
    let t1_schema = TableSchema::from_vecs(A_COLS.to_vec(), A_TYPES.to_vec());
    let t1_cid = catalog.get_table_id(&t1_table_name);
    let t1_table = TableInfo::new(t1_cid, t1_table_name, t1_schema);
    catalog.add_table(t1_table);

    let t2_table_name = String::from(T2_TABLE_NAME);
    let t2_schema = TableSchema::from_vecs(B_COLS.to_vec(), B_TYPES.to_vec());
    let t2_cid = catalog.get_table_id(&t2_table_name);
    let t2_table = TableInfo::new(t2_cid, t2_table_name, t2_schema);
    catalog.add_table(t2_table);

    let t3_table_name = String::from(T3_TABLE_NAME);
    let t3_schema = TableSchema::from_vecs(C_COLS.to_vec(), C_TYPES.to_vec());
    let t3_cid = catalog.get_table_id(&t3_table_name);
    let t3_table = TableInfo::new(t3_cid, t3_table_name, t3_schema);
    catalog.add_table(t3_table);

    catalog
}

pub fn parse_sql(sql: &str) -> sqlparser::ast::Query {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = Parser::new(&dialect)
        .try_with_sql(sql)
        .unwrap()
        .parse_statements()
        .unwrap();
    let query = {
        let statement = statements.into_iter().next().unwrap();
        if let sqlparser::ast::Statement::Query(query) = statement {
            query
        } else {
            panic!("Expected a query");
        }
    };
    *query
}

pub fn get_translator() -> Translator {
    let catalog = get_test_catalog();
    let enabled_rules = Arc::new(Rules::default());
    // enabled_rules.disable(Rule::Decorrelate);
    // enabled_rules.disable(Rule::Hoist);
    // enabled_rules.disable(Rule::ProjectionPushdown);
    let col_id_gen = Arc::new(ColIdGenerator::new());
    Translator::new(&catalog, &enabled_rules, &col_id_gen)
}

pub fn insert_table_a_rows(
    sm: &mut StorageManager,
    catalog: CatalogRef,
    a_factor: usize,
) -> (Vec<Tuple>, Vec<ValueId>) {
    let t1_cid = catalog.get_table_id_if_exists(T1_TABLE_NAME).unwrap();
    sm.create_container(
        t1_cid,
        Some(T1_TABLE_NAME.to_string()),
        StateType::BaseTable,
        None,
    )
    .unwrap();

    /*
       a | b | p | q | r
       1 | 2 | 3 | 4 | 5
       2 | 3 | 4 | 5 | 6
       3 | 4 | 5 | 6 | 7
       4 | 5 | 6 | 7 | 8
       5 | 6 | 7 | 8 | 9
    */
    let mut rows = Vec::with_capacity(A_LENGTH * a_factor);
    for _ in 0..a_factor {
        let tuples = create_tuple_list(A_ROWS.iter().map(|row| row.to_vec()).collect());
        rows.extend(tuples);
    }
    let tid = TransactionId::new();

    (
        rows.clone(),
        sm.insert_values(
            t1_cid,
            rows.into_iter()
                .map(|row| row.to_bytes())
                .collect::<Vec<Vec<u8>>>(),
            tid,
        ),
    )
}

pub fn insert_table_b_rows(
    sm: &mut StorageManager,
    catalog: CatalogRef,
    b_factor: usize,
) -> (Vec<Tuple>, Vec<ValueId>) {
    let t2_cid = catalog.get_table_id_if_exists(T2_TABLE_NAME).unwrap();
    sm.create_container(
        t2_cid,
        Some(T2_TABLE_NAME.to_string()),
        StateType::BaseTable,
        None,
    )
    .unwrap();

    /*
       c | d
       1 | 2
       2 | 3
       3 | 4
       4 | 5
       5 | 6
    */
    let mut rows = Vec::with_capacity(B_LENGTH * b_factor);
    for _ in 0..b_factor {
        let tuples = create_tuple_list(B_ROWS.iter().map(|row| row.to_vec()).collect());
        rows.extend(tuples);
    }
    let tid = TransactionId::new();

    (
        rows.clone(),
        sm.insert_values(
            t2_cid,
            rows.into_iter()
                .map(|row| row.to_bytes())
                .collect::<Vec<Vec<u8>>>(),
            tid,
        ),
    )
}

pub fn insert_table_c_rows(
    sm: &mut StorageManager,
    catalog: CatalogRef,
    c_factor: usize,
) -> (Vec<Tuple>, Vec<ValueId>) {
    let t3_cid = catalog.get_table_id_if_exists(T3_TABLE_NAME).unwrap();
    sm.create_container(
        t3_cid,
        Some(T3_TABLE_NAME.to_string()),
        StateType::BaseTable,
        None,
    )
    .unwrap();

    /*
       e | f
       1 | 2
       2 | 3
       3 | 4
       4 | 5
       5 | 6
    */
    let mut rows = Vec::with_capacity(C_LENGTH * c_factor);
    for _ in 0..c_factor {
        let tuples = create_tuple_list(C_ROWS.iter().map(|row| row.to_vec()).collect());
        rows.extend(tuples);
    }
    let tid = TransactionId::new();

    (
        rows.clone(),
        sm.insert_values(
            t3_cid,
            rows.into_iter()
                .map(|row| row.to_bytes())
                .collect::<Vec<Vec<u8>>>(),
            tid,
        ),
    )
}

pub fn get_sm_and_stats(
    catalog: Option<CatalogRef>,
    scale_factors: (usize, usize, usize),
) -> (&'static StorageManager, &'static ReservoirStatManager) {
    let mut sm = StorageManager::new_test_sm();
    let stats = ReservoirStatManager::new_test_stat_manager();
    if let Some(catalog) = catalog {
        let (a_factor, b_factor, c_factor) = scale_factors;
        let (a_rows, a_vids) = insert_table_a_rows(&mut sm, catalog.clone(), a_factor);
        let (b_rows, b_vids) = insert_table_b_rows(&mut sm, catalog.clone(), b_factor);
        let (c_rows, c_vids) = insert_table_c_rows(&mut sm, catalog.clone(), c_factor);

        // Add table A to stats
        let t1_cid = catalog.get_table_id_if_exists(T1_TABLE_NAME).unwrap();
        stats
            .register_table(t1_cid, catalog.get_table_schema(t1_cid).unwrap())
            .unwrap();
        for (record, id) in a_rows.iter().zip(a_vids.iter()) {
            stats.new_record(record, *id).unwrap();
        }

        // Add table B to stats
        let t2_cid = catalog.get_table_id_if_exists(T2_TABLE_NAME).unwrap();
        stats
            .register_table(t2_cid, catalog.get_table_schema(t2_cid).unwrap())
            .unwrap();
        for (record, id) in b_rows.iter().zip(b_vids.iter()) {
            stats.new_record(record, *id).unwrap();
        }

        // Add table C to stats
        let t3_cid = catalog.get_table_id_if_exists(T3_TABLE_NAME).unwrap();
        stats
            .register_table(t3_cid, catalog.get_table_schema(t3_cid).unwrap())
            .unwrap();
        for (record, id) in c_rows.iter().zip(c_vids.iter()) {
            stats.new_record(record, *id).unwrap();
        }
    }
    let sm_box = Box::new(sm);
    let sm: &'static StorageManager = Box::leak(sm_box);
    let stats_box = Box::new(stats);
    let stats: &'static ReservoirStatManager = Box::leak(stats_box);
    (sm, stats)
}

pub fn get_tm(config: &'static ServerConfig) -> &'static TransactionManager {
    let tm = TransactionManager::new(config);
    let tm_box = Box::new(tm);
    let tm: &'static TransactionManager = Box::leak(tm_box);
    tm
}

pub fn get_im(
    config: &'static ServerConfig,
    sm: &'static StorageManager,
    tm: &'static TransactionManager,
) -> &'static IndexManager {
    let im = IndexManager::new(config, sm, tm);
    let im_box = Box::new(im);
    let im: &'static IndexManager = Box::leak(im_box);
    im
}

pub fn get_managers(
    config: &'static ServerConfig,
    catalog: Option<CatalogRef>,
    scale_factors: (usize, usize, usize),
) -> &'static Managers {
    let (sm, stats) = get_sm_and_stats(catalog.clone(), scale_factors);
    let tm = get_tm(config);
    let im = get_im(config, sm, tm);

    let strm = StringManager::new(config, 1024 * 100, 0);
    let string_manager_box = Box::new(strm);
    let string_manager = Box::leak(string_manager_box);

    let managers = Managers::new(config, sm, tm, im, stats, string_manager);
    let managers_box = Box::new(managers);
    let managers: &'static Managers = Box::leak(managers_box);
    managers
}

pub fn get_plan(sql: &str) -> String {
    let query = parse_sql(sql);
    let mut translator = get_translator();
    let query = translator.process_query(&query).unwrap();

    let memo: Memo<DummyCost> = Memo::new();
    let plan = memo.add_group_from_plan(query.get_plan());
    println!("{}", memo.pretty_string(plan));

    memo.logical_to_physical(plan);
    let plan = memo.get_best_group_binding(plan);
    plan.pretty_string()
}

pub fn get_translator_from_catalog(catalog: CatalogRef) -> Translator {
    let enabled_rules = Arc::new(Rules::default());
    let col_id_gen = Arc::new(ColIdGenerator::new());
    Translator::new(&catalog, &enabled_rules, &col_id_gen)
}

pub fn get_plan_and_opiterator(sql: &str) -> Box<dyn OpIterator> {
    let query = parse_sql(sql);
    let catalog = get_test_catalog();
    let mut translator = get_translator_from_catalog(catalog.clone());

    let query = translator.process_query(&query).unwrap();
    let memo: Memo<DummyCost> = Memo::new();
    let plan = memo.add_group_from_plan(query.get_plan());
    println!("Logical plan\n{}", memo.pretty_string(plan));

    memo.logical_to_physical(plan);
    let plan = memo.get_best_group_binding(plan);
    println!("Physical plan\n{}", plan.pretty_string());

    let managers = get_managers(
        Box::leak(Box::new(ServerConfig::temporary())),
        Some(catalog.clone()),
        (1, 1, 1),
    );
    let transaction_id = TransactionId::new();

    (physical_plan_to_op_iterator(managers, &catalog, &plan, transaction_id, 0).unwrap()) as _
}

pub fn get_opiterator_after_optimization(
    sql: &str,
    scale_factors: Option<(usize, usize, usize)>,
) -> Box<dyn OpIterator> {
    let catalog = get_test_catalog();
    let managers = get_managers(
        Box::leak(Box::new(ServerConfig::temporary())),
        Some(catalog.clone()),
        scale_factors.unwrap_or((1, 1, 1)),
    );
    get_opiterator_after_optimization_with_managers_and_catalog(sql, managers, catalog)
}

/// Generate tables using `gen_test_table_and_tuples`
pub fn get_opiterator_after_optimization_alt(
    sql: &str,
    tuple_counts: Vec<u64>,
) -> Box<dyn OpIterator> {
    let catalog = Catalog::new();
    let config = ServerConfig::temporary();
    let config_box = Box::new(config);
    let config = Box::leak(config_box);
    let sm = StorageManager::new_test_sm();
    let stats = ReservoirStatManager::new_test_stat_manager();
    let mut rng = get_rng();
    for (i, tuple_count) in tuple_counts.iter().enumerate() {
        let (table, tuples) = gen_test_table_and_tuples(
            &mut rng,
            catalog.get_table_id(format!("test_table_{}", i).as_str()),
            *tuple_count,
        );
        catalog.add_table(table.clone());
        sm.create_container(
            table.c_id,
            Some(table.name.clone()),
            StateType::BaseTable,
            None,
        )
        .unwrap();
        let value_ids = sm.insert_values(
            table.c_id,
            tuples.iter().map(|tuple| tuple.to_bytes()).collect(),
            TransactionId::new(),
        );
        stats
            .register_table(table.c_id, table.schema.clone())
            .unwrap();
        for (record, id) in tuples.iter().zip(value_ids.iter()) {
            stats.new_record(record, *id).unwrap();
        }
    }
    let sm_box = Box::new(sm);
    let sm: &'static StorageManager = Box::leak(sm_box);
    let stats_box = Box::new(stats);
    let stats: &'static ReservoirStatManager = Box::leak(stats_box);
    let tm = get_tm(config);
    let im = get_im(config, sm, tm);

    let strm = StringManager::new(config, 1024 * 100, 0);
    let string_manager_box = Box::new(strm);
    let string_manager = Box::leak(string_manager_box);

    let managers = Managers::new(config, sm, tm, im, stats, string_manager);
    let managers_box = Box::new(managers);
    let managers: &'static Managers = Box::leak(managers_box);

    get_opiterator_after_optimization_with_managers_and_catalog(sql, managers, catalog)
}

/// Helper function to get an OpIterator after optimization
fn get_opiterator_after_optimization_with_managers_and_catalog(
    sql: &str,
    managers: &'static Managers,
    catalog: CatalogRef,
) -> Box<dyn OpIterator> {
    let query = parse_sql(sql);
    let mut translator = get_translator_from_catalog(catalog.clone());

    let query = translator.process_query(&query).unwrap();
    let cost_model = CardinalityCostModel::new(managers.stats);
    let optimizer = CascadesOptimizer::new(cost_model, managers);
    let optimized_physical_plan = optimizer.optimize(&query, None);
    println!(
        "Optimized physical plan\n{}",
        optimized_physical_plan.pretty_string()
    );

    let transaction_id = TransactionId::new();
    physical_plan_to_op_iterator(
        managers,
        &catalog,
        &optimized_physical_plan,
        transaction_id,
        0,
    )
    .unwrap()
}
