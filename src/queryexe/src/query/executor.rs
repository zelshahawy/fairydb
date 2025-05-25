use crate::mutator;
use crate::opiterator::*;
use crate::Managers;

use common::prelude::*;
use common::tuple::ConvertedResult;
use common::util::data_reader::DataReader;
use common::QueryResult;
use sqlparser::ast::Values;

/// Manages the execution of queries using OpIterators and converts a LogicalPlan to a tree of OpIterators and runs it.
pub struct Executor {
    /// Executor state
    pub plan: Option<Box<dyn OpIterator>>,
    pub managers: &'static Managers,
}

impl Executor {
    /// Initializes an executor.
    ///
    /// Takes in the database catalog, query's logical plan, and transaction id to create the
    /// physical plan for the executor.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `storage_manager` - The SM for the DB to get access to files/buffer pool
    /// * `logical_plan` - Translated logical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    pub fn new_ref(managers: &'static Managers) -> Self {
        Self {
            plan: None,
            managers,
        }
    }

    pub fn configure_query(&mut self, opiterator: Box<dyn OpIterator>) {
        self.plan = Some(opiterator);
    }

    /// Consumes the opiterator and stores the result in a QueryResult.    
    pub fn execute(&mut self) -> Result<QueryResult, CrustyError> {
        let mut opiterator = self.plan.take().unwrap();
        let schema = opiterator.get_schema().clone(); // clone the schema for returning

        let mut res = Vec::new();
        // TODO: Magic number
        let result_buffer_initial_size = 10000;
        res.reserve(result_buffer_initial_size);

        opiterator.configure(false);
        opiterator.open()?;
        while let Some(t) = opiterator.next()? {
            res.push(t);
        }
        opiterator.close()?;

        Ok(QueryResult::new_select_result(&schema, res, None)) // Setting paging_info as None.
    }

    pub fn import_tuples(
        &self,
        values: &Values,
        _table_name: &str,
        table_id: &ContainerId,
        table_schema: &TableSchema,
        txn_id: TransactionId,
    ) -> Result<usize, CrustyError> {
        let converted_result = mutator::convert_insert_vals(values)?; // This returns Vec<u8>
        let validated_converted_result =
            mutator::validate_tuples(table_id, table_schema, None, converted_result, &txn_id)?;

        if !validated_converted_result.unconverted.is_empty() {
            return Err(CrustyError::ValidationError(format!(
                "Some records were not valid: {:?}",
                validated_converted_result.unconverted
            )));
        }

        let insert_count = mutator::insert_validated_tuples(
            *table_id,
            &validated_converted_result.converted,
            txn_id,
            self.managers,
        )?;

        Ok(insert_count)
    }

    /// Import database from csv file at path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path of the csv file containing database.
    /// * `table_name` - Destination table
    /// * `txn_id` - Transaction Id of loading client
    pub fn import_records_from_reader(
        &self,
        rdr: &mut dyn DataReader,
        table_id: &ContainerId,
        txn_id: TransactionId,
    ) -> Result<usize, CrustyError> {
        // TODO: Magic number
        let max_records_in_mem = 100000;
        let mut result_set = ConvertedResult::new();
        result_set.converted.reserve(max_records_in_mem);

        let mut total_insert_count = 0;

        loop {
            result_set.reset();

            // Read chunk of records
            // If the number of records in memory exceeds max_records_in_mem, insert them into the table
            let mut count = 0;
            while let Some(tuple) = rdr.read_next()? {
                result_set.converted.push(tuple);

                count += 1;
                if count >= max_records_in_mem {
                    break;
                }
            }

            if result_set.converted.is_empty() {
                break;
            }

            // TODO: Validate and insert tuples
            // let validated_converted_result =
            //     mutator::validate_tuples(table_id, table_schema, None, result_set, &txn_id)?;
            if !result_set.unconverted.is_empty() {
                return Err(CrustyError::ValidationError(format!(
                    "Some records were not valid: {:?}",
                    result_set.unconverted
                )));
            } else {
                let insert_count = mutator::insert_validated_tuples(
                    *table_id,
                    &result_set.converted,
                    txn_id,
                    self.managers,
                )?;
                total_insert_count += insert_count;
            }
        }

        Ok(total_insert_count)
    }
}

#[cfg(test)]
mod test {
    use common::{
        traits::stat_manager_trait::StatManagerTrait, traits::storage_trait::StorageTrait,
        util::data_reader::CsvReader,
    };

    use crate::testutil::TestSetup;

    use super::*;

    #[test]
    pub fn test_insert_tuples() {
        let test_setup = TestSetup::new_with_content();
        let sm = test_setup.get_storage_manager();
        let _tm = test_setup.get_transaction_manager();
        let catalog = test_setup.get_catalog();

        let schema =
            TableSchema::from_vecs(vec!["a", "b"], vec![DataType::BigInt, DataType::BigInt]);
        let c_id = catalog.get_table_id("test");
        catalog
            .add_table(TableInfo::new(c_id, "test".to_string(), schema.clone()))
            .unwrap();
        sm.create_container(c_id, None, StateType::BaseTable, None)
            .unwrap();
        test_setup
            .managers
            .stats
            .register_table(c_id, schema.clone())
            .unwrap();
        // create a csv file with more than 1000 records
        let mut csv_reader = CsvReader::new(
            std::io::Cursor::new("a,b\n1,2\n3,4\n5,6\n7,8\n9,10\n"),
            &schema,
            b',',
            true,
        )
        .unwrap();

        let exec = Executor::new_ref(test_setup.managers);
        exec.import_records_from_reader(
            &mut csv_reader as &mut dyn DataReader,
            &c_id,
            TransactionId::new(),
        )
        .unwrap();

        // check that all records were inserted
        let iter = sm.get_iterator(c_id, TransactionId::new(), Permissions::ReadOnly);
        let mut count = 0;
        for (i, (t, _)) in iter.enumerate() {
            let t = Tuple::from_bytes(&t);
            assert_eq!(t.get_field(0).unwrap(), &Field::BigInt((2 * i + 1) as i64));
            assert_eq!(t.get_field(1).unwrap(), &Field::BigInt((2 * i + 2) as i64));
            count += 1;
        }
        assert_eq!(count, 5);
    }
}

/* FIXME
#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;
    use crate::bufferpool::*;
    use crate::DBSERVER;
    use common::{DataType, Field, TableSchema};

    fn test_logical_plan() -> LogicalPlan {
        let mut lp = LogicalPlan::new();
        let scan = LogicalOp::Scan(ScanNode {
            alias: TABLE_A.to_string(),
        });
        let project = LogicalOp::Project(ProjectNode {
            identifiers: ProjectionExpr::Wildcard,
        });
        let si = lp.add_node(scan);
        let pi = lp.add_node(project);
        lp.add_edge(pi, si);
        lp
    }

    #[test]
    fn test_to_op_iterator() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut op = Executor::logical_plan_to_op_iterator(&db, &lp, tid).unwrap();
        op.open()?;
        let mut sum = 0;
        while let Some(t) = op.next()? {
            for i in 0..t.size() {
                sum += match t.get_field(i).unwrap() {
                    Field::Int(n) => n,
                    _ => panic!("Not an Int"),
                }
            }
        }
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_started() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start()?;
        let mut sum = 0;
        while let Some(t) = executor.next()? {
            for i in 0..t.size() {
                sum += *match t.get_field(i).unwrap() {
                    Field::Int(n) => n,
                    _ => panic!("Not an Int"),
                }
            }
        }
        println!("sum: {}", sum);
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_close() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start().unwrap();
        executor.close().unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_get_field_indices_names() -> Result<(), CrustyError> {
        let names = vec!["one", "two", "three", "four"];
        let aliases = vec!["1", "2", "3", "4"];
        let indices = vec![0, 1, 2, 3];
        let types = std::iter::repeat(DataType::Int).take(4).collect();
        let schema = TableSchema::from_vecs(names.clone(), types);

        // Test without aliases.
        let fields = names.iter().map(|s| FieldIdent::new("", s)).collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, names);

        // Test with aliases.
        let fields = names
            .iter()
            .zip(aliases.iter())
            .map(|(n, a)| FieldIdent::new_column_alias("", n, a))
            .collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, aliases);
        Ok(())
    }
}*/
