use crate::opiterator::OpIterator;
use crate::stats::reservoir_stat_manager::ReservoirStatManager;
use crate::Managers;
use crate::StorageManager;
use crate::TransactionManager;
use common::catalog::Catalog;
use common::catalog::CatalogRef;
use common::physical::small_string::StringManager;
use common::prelude::TransactionId;
use common::table::TableInfo;
use common::traits::stat_manager_trait::StatManagerTrait;
use common::traits::storage_trait::StorageTrait;
use common::CrustyError;
use common::Field;
use common::{Attribute, DataType, TableSchema, Tuple};
use index::IndexManager;

pub fn execute_iter(iter: &mut dyn OpIterator, sorted: bool) -> Result<Vec<Tuple>, CrustyError> {
    let mut tuples = Vec::new();
    iter.open()?;
    while let Some(tuple) = iter.next()? {
        tuples.push(tuple);
    }
    if sorted {
        tuples.sort_by(|a, b| a.field_vals.cmp(&b.field_vals));
    }
    Ok(tuples)
}

#[allow(dead_code)]
pub struct TestTuples {
    pub schema: TableSchema,
    pub tuples: Vec<Tuple>,
}

impl TestTuples {
    #[allow(dead_code)]
    pub fn new(table_name: &str) -> Self {
        // Creates a vector of tuples to create the following table:
        //
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G
        let tuples = vec![
            Tuple::new(vec![
                Field::BigInt(1),
                Field::BigInt(1),
                Field::BigInt(3),
                Field::String("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::BigInt(2),
                Field::BigInt(1),
                Field::BigInt(3),
                Field::String("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::BigInt(3),
                Field::BigInt(1),
                Field::BigInt(4),
                Field::String("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::BigInt(4),
                Field::BigInt(2),
                Field::BigInt(4),
                Field::String("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::BigInt(5),
                Field::BigInt(2),
                Field::BigInt(5),
                Field::String("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::BigInt(6),
                Field::BigInt(2),
                Field::BigInt(5),
                Field::String("G".to_string()),
            ]),
        ];

        let schema = TableSchema::new(vec![
            Attribute::new(format!("{}.{}", table_name, "a"), DataType::BigInt),
            Attribute::new(format!("{}.{}", table_name, "b"), DataType::BigInt),
            Attribute::new(format!("{}.{}", table_name, "c"), DataType::BigInt),
            Attribute::new(format!("{}.{}", table_name, "d"), DataType::String),
        ]);

        Self { schema, tuples }
    }
}

pub fn new_test_managers() -> &'static Managers {
    let sm = StorageManager::new_test_sm();
    let config = common::physical::config::ServerConfig::temporary();
    let config_box = Box::new(config);
    let config = Box::leak(config_box);
    let storage_manager_box = Box::new(sm);
    let storage_manager = Box::leak(storage_manager_box);
    let transaction_manager_box = Box::new(TransactionManager {});
    let transaction_manager = Box::leak(transaction_manager_box);
    let im = Box::new(IndexManager::new(
        config,
        storage_manager,
        transaction_manager,
    ));
    let index_manager = Box::leak(im);
    let stats_box = Box::new(ReservoirStatManager::new(config, 0));
    let stats = Box::leak(stats_box);
    let strm = StringManager::new(config, 1024 * 100, 0);
    let string_manager_box = Box::new(strm);
    let string_manager = Box::leak(string_manager_box);
    let managers = Box::new(Managers::new(
        config,
        storage_manager,
        transaction_manager,
        index_manager,
        stats,
        string_manager,
    ));
    Box::leak(managers)
}

pub struct TestSetup {
    pub catalog: CatalogRef,
    pub managers: &'static Managers,
}

impl TestSetup {
    pub fn new_with_content() -> Self {
        let catalog = Catalog::new();
        let managers = new_test_managers();
        // insert three tables into the catalog and storage manager
        for i in 0..3 {
            let name = format!("table{}", i);
            let test_tuples = TestTuples::new(&name);
            let c_id = catalog.get_table_id(&name);
            let table = TableInfo::new(c_id, name.clone(), test_tuples.schema.clone());
            catalog.add_table(table.clone()).unwrap();
            managers.sm.create_table(c_id).unwrap();
            managers.stats.register_table(c_id, table.schema).unwrap();
            let mut inserting_values = Vec::with_capacity(test_tuples.tuples.len());
            for tuple in &test_tuples.tuples {
                inserting_values.push(tuple.to_bytes());
            }
            managers
                .sm
                .insert_values(c_id, inserting_values, TransactionId::new());
        }

        Self { catalog, managers }
    }

    pub fn new_empty() -> Self {
        let catalog = Catalog::new();
        let managers = new_test_managers();

        Self { catalog, managers }
    }

    pub fn get_catalog(&self) -> &CatalogRef {
        &self.catalog
    }

    pub fn get_storage_manager(&self) -> &'static StorageManager {
        self.managers.sm
    }

    pub fn get_transaction_manager(&self) -> &'static TransactionManager {
        self.managers.tm
    }
}

impl Default for TestSetup {
    fn default() -> Self {
        TestSetup::new_empty()
    }
}
