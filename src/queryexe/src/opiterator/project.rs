use super::OpIterator;
use common::query::bytecode_expr::ByteCodeExpr;

use common::{CrustyError, TableSchema, Tuple};

/// Projection operator.
pub struct Project {
    // Parameters (No need to reset on close)
    schema: TableSchema,
    fields: Vec<ByteCodeExpr>,
    child: Box<dyn OpIterator>,
    // States (Need to reset on close)
    open: bool,
}

impl Project {
    pub fn new(fields: Vec<ByteCodeExpr>, schema: TableSchema, child: Box<dyn OpIterator>) -> Self {
        Self {
            fields,
            open: false,
            schema,
            child,
        }
    }
}

impl OpIterator for Project {
    fn configure(&mut self, will_rewind: bool) {
        self.child.configure(will_rewind);
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.child.open()?;
        }
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        if let Some(tuple) = self.child.next()? {
            let mut new_field_vals = Vec::with_capacity(self.fields.len());
            for expr in &self.fields {
                let t = expr.eval(&tuple);
                new_field_vals.push(t);
            }
            let t = Tuple::new(new_field_vals);
            return Ok(Some(t));
        }

        Ok(None)
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
    use super::*;
    use crate::opiterator::TupleIterator;
    use crate::testutil::{execute_iter, TestTuples};
    use common::query::bytecode_expr::{ByteCodeExpr, ByteCodes};
    use common::TableSchema;

    use super::Project;
    use common::datatypes::{f_int, f_str};

    fn get_iter(fields: Vec<ByteCodeExpr>) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let dummy = TableSchema::new(vec![]);
        let mut iter = Box::new(Project::new(
            fields,
            dummy,
            Box::new(TupleIterator::new(setup.tuples, setup.schema)),
        ));
        iter.configure(false);
        iter
    }

    fn get_fields_expression() -> Vec<ByteCodeExpr> {
        // Input:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // (col0 + col1) * col2, col3

        // Output:
        // 6 E
        // 9 G
        // 16 A
        // 24 G
        // 35 G
        // 40 G
        let mut expr1 = ByteCodeExpr::new();
        expr1.add_code(ByteCodes::PushField as usize);
        expr1.add_code(0);
        expr1.add_code(ByteCodes::PushField as usize);
        expr1.add_code(1);
        expr1.add_code(ByteCodes::Add as usize);
        expr1.add_code(ByteCodes::PushField as usize);
        expr1.add_code(2);
        expr1.add_code(ByteCodes::Mul as usize);
        let mut expr2 = ByteCodeExpr::new();
        expr2.add_code(ByteCodes::PushField as usize);
        expr2.add_code(3);
        vec![expr1, expr2]
    }

    fn run_projection(fields: Vec<ByteCodeExpr>) -> Vec<Tuple> {
        let mut iter = get_iter(fields);
        execute_iter(&mut *iter, false).unwrap()
    }

    mod projection_test {
        use super::*;

        #[test]
        fn test_empty_fields() {
            let fields = vec![];
            let tuples = run_projection(fields);
            // Number of tuples should be the same
            assert_eq!(tuples.len(), 6);
            // Each tuple should be empty
            for t in tuples {
                assert_eq!(t.len(), 0);
            }
        }

        #[test]
        fn test_projection() {
            let fields = get_fields_expression();
            let t = run_projection(fields);
            // Output:
            // 6 E
            // 9 G
            // 16 A
            // 24 G
            // 35 G
            // 40 G
            assert!(t.len() == 6);
            assert_eq!(t[0], Tuple::new(vec![f_int(6), f_str("E")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(9), f_str("G")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(16), f_str("A")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(24), f_str("G")]));
            assert_eq!(t[4], Tuple::new(vec![f_int(35), f_str("G")]));
            assert_eq!(t[5], Tuple::new(vec![f_int(40), f_str("G")]));
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(vec![]);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(vec![]);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(vec![]);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(vec![]);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(get_fields_expression());
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
