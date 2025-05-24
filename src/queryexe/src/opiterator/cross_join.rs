use super::OpIterator;
use common::{CrustyError, TableSchema, Tuple};

pub struct CrossJoin {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    // States (Need to reset on close)
    open: bool,
    current_tuple: Option<Tuple>,
}

impl CrossJoin {
    pub fn new(
        schema: TableSchema,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            schema,
            left_child,
            right_child,
            open: false,
            current_tuple: None,
        }
    }
}

impl OpIterator for CrossJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(will_rewind);
        self.right_child.configure(true); // right child will always be rewound by CJ
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.left_child.open()?;
            self.right_child.open()?;
            self.current_tuple = self.left_child.next()?;
            self.open = true;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Iterator is not open");
        }
        while let Some(left_tuple) = &self.current_tuple {
            if let Some(right_tuple) = self.right_child.next()? {
                let t = left_tuple.merge(&right_tuple);
                return Ok(Some(t));
            }
            self.right_child.rewind()?;
            self.current_tuple = self.left_child.next()?;
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.current_tuple = self.left_child.next()?;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::execute_iter;
    use crate::testutil::TestTuples;

    fn get_iter() -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let mut iter = Box::new(CrossJoin::new(
            setup.schema.clone(),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_cross_join() -> Vec<Tuple> {
        let mut iter = get_iter();
        execute_iter(&mut *iter, true).unwrap()
    }

    mod cross_join_test {
        use super::*;

        #[test]
        fn test_join() {
            // Joining two tables each containing the following tuples:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            // Output:
            // Simple cross join, number of tuples = 36
            let tuples = run_cross_join();
            assert_eq!(tuples.len(), 36);
            let expected = {
                let t_vec = TestTuples::new("").tuples;
                let mut expected = Vec::new();
                for t in t_vec.iter() {
                    for u in t_vec.iter() {
                        expected.push(t.merge(u));
                    }
                }
                expected
            };
            for (t, e) in tuples.iter().zip(expected.iter()) {
                assert_eq!(t.field_vals, e.field_vals);
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
