use super::OpIterator;
use common::error::c_err;
use common::query::bytecode_expr::ByteCodeExpr;
use common::{CrustyError, Field, TableSchema, Tuple};

/// Filter oeprator.
pub struct Filter {
    // Parameters (No need to reset on close)
    /// Schema of the child.
    schema: TableSchema,
    /// Predicate to filter by.
    predicate: ByteCodeExpr,
    /// Child operator passing data into operator.
    child: Box<dyn OpIterator>,

    // States (Need to reset on close)
    /// Boolean determining if iterator is open.
    open: bool,
}

impl Filter {
    /// Filter constructor.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate to filter by.
    /// * `child` - Child OpIterator passing data into the operator.
    pub fn new(predicate: ByteCodeExpr, schema: TableSchema, child: Box<dyn OpIterator>) -> Self {
        Self {
            open: false,
            schema,
            predicate,
            child,
        }
    }
}

impl OpIterator for Filter {
    fn configure(&mut self, will_rewind: bool) {
        self.child.configure(will_rewind);
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.child.open()?;
            self.open = true;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        let mut res = None;
        while let Some(t) = self.child.next()? {
            match self.predicate.eval(&t) {
                Field::Bool(b) => {
                    if b {
                        res = Some(t);
                        break;
                    }
                }
                _ => {
                    return Err(c_err("Predicate did not evaluate to a boolean"));
                }
            }
        }
        Ok(res)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use common::datatypes::{f_int, f_str};
    use common::query::bytecode_expr::ByteCodeExpr;
    use common::query::bytecode_expr::ByteCodes;

    use super::*;
    use crate::opiterator::TupleIterator;
    use crate::testutil::execute_iter;
    use crate::testutil::TestTuples;

    fn get_iter(predicate: ByteCodeExpr) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let mut iter = Box::new(Filter::new(
            predicate,
            setup.schema.clone(),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn get_predicate() -> ByteCodeExpr {
        // Input:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // Predicate:
        // ((col(0) / col(1)) = 2) OR (col(2) = 3)
        // Tree:
        //           (OR)
        //          /    \
        //        (=)     (=)
        //       /   \   /   \
        //    (/)    2  c(2)  3
        //   /  \
        // c(0) c(1)

        // Output:
        // 1 1 3 E
        // 2 1 3 G
        // 4 2 4 G
        // 5 2 5 G
        let mut expr = ByteCodeExpr::new();
        expr.add_code(ByteCodes::PushField as usize);
        expr.add_code(0);
        expr.add_code(ByteCodes::PushField as usize);
        expr.add_code(1);
        expr.add_code(ByteCodes::Div as usize);
        expr.add_code(ByteCodes::PushLit as usize);
        let i = expr.add_literal(Field::BigInt(2));
        expr.add_code(i);
        expr.add_code(ByteCodes::Eq as usize);
        expr.add_code(ByteCodes::PushField as usize);
        expr.add_code(2);
        expr.add_code(ByteCodes::PushLit as usize);
        let i = expr.add_literal(Field::BigInt(3));
        expr.add_code(i);
        expr.add_code(ByteCodes::Eq as usize);
        expr.add_code(ByteCodes::Or as usize);
        expr
    }

    fn run_filter(predicate: ByteCodeExpr) -> Vec<Tuple> {
        let mut iter = get_iter(predicate);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod filter_test {

        use super::*;

        #[test]
        #[should_panic]
        fn test_empty_predicate() {
            let predicate = ByteCodeExpr::new();
            let _ = run_filter(predicate);
        }

        #[test]
        fn test_filter() {
            let predicate = get_predicate();
            let t = run_filter(predicate);
            // Output:
            // 1 1 3 E
            // 2 1 3 G
            // 4 2 4 G
            // 5 2 5 G
            assert!(t.len() == 4);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_int(1), f_int(1), f_int(3), f_str("E")])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_int(2), f_int(1), f_int(3), f_str("G")])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_int(4), f_int(2), f_int(4), f_str("G")])
            );
            assert_eq!(
                t[3],
                Tuple::new(vec![f_int(5), f_int(2), f_int(5), f_str("G")])
            );
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(ByteCodeExpr::new());
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(ByteCodeExpr::new());
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(ByteCodeExpr::new());
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(ByteCodeExpr::new());
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(get_predicate());
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
