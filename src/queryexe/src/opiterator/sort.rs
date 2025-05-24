use super::OpIterator;
use crate::Managers;
use common::query::bytecode_expr::ByteCodeExpr;

use common::{CrustyError, Field, TableSchema, Tuple};

use std::cmp::Ordering;

/// Sort operator
pub struct Sort {
    #[allow(dead_code)]
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    schema: TableSchema,
    fields: Vec<(ByteCodeExpr, bool)>, // (field, asc)
    child: Box<dyn OpIterator>,
    will_rewind: bool,

    // States (Need to reset on close)
    open: bool,
    sorted_data: Vec<(Vec<Field>, Tuple)>,
    index: usize, // Stores the index of next tuple to return
}

impl Sort {
    pub fn new(
        managers: &'static Managers,
        fields: Vec<(ByteCodeExpr, bool)>,
        schema: TableSchema,
        child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            managers,
            open: false,
            schema,
            fields,
            sorted_data: Vec::new(),
            child,
            index: 0,
            will_rewind: true,
        }
    }
}

impl OpIterator for Sort {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        self.child.configure(false); // will_rewind is false for child because the sort is stateful and rewinding sort operator does not rewind child
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.child.open()?;
            while let Some(tuple) = self.child.next()? {
                let mut sort_key = Vec::new();
                for (field, _) in &self.fields {
                    sort_key.push(field.eval(&tuple));
                }
                self.sorted_data.push((sort_key, tuple));
            }
            self.child.close()?;
            self.sorted_data
                .sort_by(|(a_sort_key, _), (b_sort_key, _)| {
                    for (i, (_, asc)) in self.fields.iter().enumerate() {
                        // Sort it by reverse order so that we can
                        // pop the elements from the back when
                        // returning the tuples by next().
                        // Note that pop is O(1), but remove(0) is O(n)
                        let res = if !*asc {
                            a_sort_key[i].cmp(&b_sort_key[i])
                        } else {
                            b_sort_key[i].cmp(&a_sort_key[i])
                        };
                        if res != Ordering::Equal {
                            return res;
                        }
                    }
                    Ordering::Equal
                });
            self.index = self.sorted_data.len(); // index of the last element
            self.open = true;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if self.will_rewind {
            // do not consume the iterator
            if self.index == 0 {
                Ok(None)
            } else {
                self.index -= 1;
                match self.sorted_data.get(self.index) {
                    None => Ok(None),
                    Some((_, tuple)) => Ok(Some(tuple.clone())),
                }
            }
        } else {
            // consume the iterator
            match self.sorted_data.pop() {
                None => Ok(None),
                Some((_, tuple)) => Ok(Some(tuple)),
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.sorted_data.clear();
        self.index = 0;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if !self.will_rewind {
            panic!("Cannot rewind a Sort operator with will_rewind set to false")
        }
        self.index = self.sorted_data.len();
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use common::datatypes::{f_int, f_str};
    use common::query::bytecode_expr::colidx_expr;
    use common::query::bytecode_expr::ByteCodeExpr;

    use super::*;
    use crate::opiterator::TupleIterator;
    use crate::testutil::execute_iter;
    use crate::testutil::TestTuples;

    fn get_iter(fields: Vec<(ByteCodeExpr, bool)>) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = crate::testutil::new_test_managers();
        let mut iter = Box::new(Sort::new(
            managers,
            fields,
            setup.schema.clone(),
            Box::new(TupleIterator::new(setup.tuples, setup.schema)),
        ));
        iter.configure(false);
        iter
    }

    fn get_sort_fields() -> Vec<(ByteCodeExpr, bool)> {
        // Input:
        // 1 1 3 E
        // 2 1 3 G
        // 3 1 4 A
        // 4 2 4 G
        // 5 2 5 G
        // 6 2 5 G

        // Output:
        // 6 2 5 G
        // 5 2 5 G
        // 4 2 4 G
        // 3 1 4 A
        // 2 1 3 G
        // 1 1 3 E
        vec![(colidx_expr(1), false), (colidx_expr(0), false)]
    }

    fn run_sort(fields: Vec<(ByteCodeExpr, bool)>) -> Vec<Tuple> {
        let mut iter = get_iter(fields);
        execute_iter(&mut *iter, false).unwrap()
    }

    mod sort_test {
        use super::*;
        #[test]
        fn test_sort() {
            let t = run_sort(get_sort_fields());
            // 6 2 5 G
            // 5 2 5 G
            // 4 2 4 G
            // 3 1 4 A
            // 2 1 3 G
            // 1 1 3 E
            assert_eq!(t.len(), 6);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_int(6), f_int(2), f_int(5), f_str("G")])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_int(5), f_int(2), f_int(5), f_str("G")])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_int(4), f_int(2), f_int(4), f_str("G")])
            );
            assert_eq!(
                t[3],
                Tuple::new(vec![f_int(3), f_int(1), f_int(4), f_str("A")])
            );
            assert_eq!(
                t[4],
                Tuple::new(vec![f_int(2), f_int(1), f_int(3), f_str("G")])
            );
            assert_eq!(
                t[5],
                Tuple::new(vec![f_int(1), f_int(1), f_int(3), f_str("E")])
            );
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(get_sort_fields());
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(get_sort_fields());
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(get_sort_fields());
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(get_sort_fields());
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(get_sort_fields());
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
