use super::OpIterator;
use crate::{Managers, StorageManager};
use common::ids::Permissions;
use common::ids::{ContainerId, TransactionId};
use common::prelude::ValueId;
use common::query::bytecode_expr::ByteCodeExpr;
use common::traits::storage_trait::StorageTrait;
use common::{CrustyError, Field, TableSchema, Tuple};

/// Sequential scan operator
pub struct SeqScan {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    managers: &'static Managers,
    container_id: ContainerId,
    transaction_id: TransactionId,
    filter: Option<ByteCodeExpr>,
    projection: Option<Vec<ByteCodeExpr>>,

    // States (Need to reset on close)
    open: bool,
    index: Option<ValueId>, // Stores the value_id of the last tuple returned
    file_iter: Option<<StorageManager as StorageTrait>::ValIterator>,
}

impl SeqScan {
    /// Constructor for the sequential scan operator.
    ///
    /// # Arguments
    ///
    /// * `table` - Table to scan over.
    /// * `table_alias` - Table alias given by the user.
    /// * `tid` - Transaction used to read the table.
    pub fn new(
        managers: &'static Managers,
        schema: &TableSchema,
        container_id: &ContainerId,
        tid: TransactionId,
        filter: Option<ByteCodeExpr>,
        projection: Option<Vec<ByteCodeExpr>>,
    ) -> Self {
        Self {
            open: false,
            schema: schema.clone(),
            managers,
            container_id: *container_id,
            transaction_id: tid,
            index: None,
            file_iter: None,
            filter,
            projection,
        }
    }
}

impl OpIterator for SeqScan {
    fn configure(&mut self, _will_rewind: bool) {
        // do nothing
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.file_iter = if let Some(index) = self.index {
                Some(self.managers.sm.get_iterator_from(
                    self.container_id,
                    self.transaction_id,
                    Permissions::ReadOnly,
                    index,
                ))
            } else {
                Some(self.managers.sm.get_iterator(
                    self.container_id,
                    self.transaction_id,
                    Permissions::ReadOnly,
                ))
            };
        }
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        let file_iter = self
            .file_iter
            .as_mut()
            .expect("File iterator should be set on open");

        for (bytes, id) in file_iter.by_ref() {
            // Create the tuple
            let mut tuple = Tuple::from_bytes(&bytes);
            tuple.value_id = Some(id);
            self.index = Some(id);

            if let Some(filter) = &self.filter {
                match filter.eval(&tuple) {
                    Field::Bool(b) => {
                        if !b {
                            continue;
                        }
                    }
                    _ => panic!("Filter must evaluate to a boolean"),
                }
            }

            if let Some(projection) = &self.projection {
                let mut new_field_vals = Vec::with_capacity(projection.len());
                for expr in projection {
                    let t = expr.eval(&tuple);
                    new_field_vals.push(t);
                }
                return Ok(Some(Tuple::new(new_field_vals)));
            } else {
                return Ok(Some(tuple));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.file_iter = None;
        self.index = None;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.file_iter = Some(self.managers.sm.get_iterator(
            self.container_id,
            self.transaction_id,
            Permissions::ReadOnly,
        ));
        self.index = None;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::testutil::{execute_iter, new_test_managers, TestTuples};
    use common::ids::TransactionId;

    fn get_iter() -> Box<dyn OpIterator> {
        // Create test SM with a container
        let managers = new_test_managers();
        let cid = 0;
        managers.sm.create_table(cid).unwrap();

        let setup = TestTuples::new("");
        let tid = TransactionId::new();

        for t in setup.tuples {
            managers.sm.insert_value(cid, t.to_bytes(), tid);
        }

        let mut iter = Box::new(SeqScan::new(managers, &setup.schema, &cid, tid, None, None));
        iter.configure(false);
        iter
    }

    fn run_scan() -> Vec<Tuple> {
        let mut iter = get_iter();
        execute_iter(&mut *iter, false).unwrap()
    }

    mod scan_test {
        use super::*;

        #[test]
        fn test_scan() {
            let tuples = run_scan();
            let expected = TestTuples::new("");
            for (t, e) in tuples.iter().zip(expected.tuples.iter()) {
                assert_eq!(t.field_vals, e.field_vals)
            }
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter();
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter();
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter();
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter();
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter();
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
