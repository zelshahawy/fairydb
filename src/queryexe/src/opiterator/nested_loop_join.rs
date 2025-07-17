use super::OpIterator;

#[allow(unused_imports)]
use common::datatypes::compare_fields; // QO compare fields with op
use common::query::bytecode_expr::ByteCodeExpr;
use common::{BinaryOp, FairyError, TableSchema, Tuple};

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct NestedLoopJoin {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    op: BinaryOp,
    left_expr: ByteCodeExpr,
    right_expr: ByteCodeExpr,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    // States (Need to reset on close)
    open: bool,
    current_tuple: Option<Tuple>, // Current tuple in left table
}

impl NestedLoopJoin {
    /// NestedLoopJoin constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: BinaryOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
        schema: TableSchema,
    ) -> Self {
        Self {
            op,
            left_expr,
            right_expr,
            open: false,
            schema,
            left_child,
            right_child,
            current_tuple: None,
        }
    }
}

impl OpIterator for NestedLoopJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.left_child.configure(will_rewind);
        self.right_child.configure(true); // right child will always be rewound by NLJ
    }

    fn open(&mut self) -> Result<(), FairyError> {
        if !self.open {
            self.left_child.open()?;
            self.right_child.open()?;
            self.current_tuple = self.left_child.next()?;
            self.open = true;
        }
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    /// hint look at `compare_fields` and `Tuple.merge` functions
    fn next(&mut self) -> Result<Option<Tuple>, FairyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        // outer loop over left tuple
        while let Some(left) = self.current_tuple.clone() {
            // inner loop over right tuples
            while let Some(right) = self.right_child.next()? {
                let lval = self.left_expr.eval(&left);
                let rval = self.right_expr.eval(&right);
                if compare_fields(self.op, &lval, &rval) {
                    return Ok(Some(left.clone().merge(&right)));
                }
            }

            // Continue
            self.current_tuple = self.left_child.next()?;
            if self.current_tuple.is_none() {
                break;
            }
            self.right_child.rewind()?;
        }

        Ok(None)
    }

    fn close(&mut self) -> Result<(), FairyError> {
        if self.open {
            self.left_child.close()?;
            self.right_child.close()?;
            self.open = false;
            self.current_tuple = None;
        }
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), FairyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.current_tuple = self.left_child.next()?;
        Ok(())
    }

    /// return schema of the result
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
    use common::query::bytecode_expr::{ByteCodeExpr, ByteCodes};
    use common::Field;

    fn get_join_predicate() -> (ByteCodeExpr, ByteCodeExpr) {
        // Joining two tables each containing the following tuples:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // left(col(0) + col(1)) OP right(col(2))
        let mut left = ByteCodeExpr::new();
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(0);
        left.add_code(ByteCodes::PushField as usize);
        left.add_code(1);
        left.add_code(ByteCodes::Add as usize);

        let mut right = ByteCodeExpr::new();
        right.add_code(ByteCodes::PushField as usize);
        right.add_code(2);

        (left, right)
    }

    fn get_iter(
        op: BinaryOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let mut iter = Box::new(NestedLoopJoin::new(
            op,
            left_expr,
            right_expr,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
            setup.schema.clone(),
        ));
        iter.configure(false);
        iter
    }

    fn run_nested_loop_join(
        op: BinaryOp,
        left_expr: ByteCodeExpr,
        right_expr: ByteCodeExpr,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(op, left_expr, right_expr);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod nested_loop_join_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_empty_predicate_join() {
            let left_expr = ByteCodeExpr::new();
            let right_expr = ByteCodeExpr::new();
            let _ = run_nested_loop_join(BinaryOp::Eq, left_expr, right_expr);
        }

        #[test]
        fn test_eq_join() {
            // Joining two tables each containing the following tuples:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            // left(col(0) + col(1)) == right(col(2))

            // Output:
            // 2 1 3 G 1 1 3 E
            // 2 1 3 G 2 1 3 G
            // 3 1 4 A 3 1 4 A
            // 3 1 4 A 4 2 4 G
            let (left_expr, right_expr) = get_join_predicate();
            let t = run_nested_loop_join(BinaryOp::Eq, left_expr, right_expr);
            assert_eq!(t.len(), 4);
            assert_eq!(
                t[0],
                Tuple::new(vec![
                    Field::BigInt(2),
                    Field::BigInt(1),
                    Field::BigInt(3),
                    Field::String("G".to_string()),
                    Field::BigInt(1),
                    Field::BigInt(1),
                    Field::BigInt(3),
                    Field::String("E".to_string()),
                ])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![
                    Field::BigInt(2),
                    Field::BigInt(1),
                    Field::BigInt(3),
                    Field::String("G".to_string()),
                    Field::BigInt(2),
                    Field::BigInt(1),
                    Field::BigInt(3),
                    Field::String("G".to_string()),
                ])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![
                    Field::BigInt(3),
                    Field::BigInt(1),
                    Field::BigInt(4),
                    Field::String("A".to_string()),
                    Field::BigInt(3),
                    Field::BigInt(1),
                    Field::BigInt(4),
                    Field::String("A".to_string()),
                ])
            );
            assert_eq!(
                t[3],
                Tuple::new(vec![
                    Field::BigInt(3),
                    Field::BigInt(1),
                    Field::BigInt(4),
                    Field::String("A".to_string()),
                    Field::BigInt(4),
                    Field::BigInt(2),
                    Field::BigInt(4),
                    Field::String("G".to_string()),
                ])
            );
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BinaryOp::Eq, left_expr, right_expr);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BinaryOp::Eq, left_expr, right_expr);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BinaryOp::Eq, left_expr, right_expr);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BinaryOp::Eq, left_expr, right_expr);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(BinaryOp::Eq, left_expr, right_expr);
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
