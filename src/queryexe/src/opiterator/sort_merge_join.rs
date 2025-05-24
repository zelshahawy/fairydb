use super::OpIterator;
use crate::Managers;
use common::error::c_err;
use common::query::bytecode_expr::ByteCodeExpr;

use common::{CrustyError, Field, TableSchema, Tuple};

use std::cmp::{self, Ordering};

fn compare_keys<I: Iterator<Item = bool>>(
    l_sort_key: &[Field],
    r_sort_key: &[Field],
    ascs: I,
) -> cmp::Ordering {
    for (i, asc) in ascs.enumerate() {
        let res = if asc {
            l_sort_key[i].cmp(&r_sort_key[i])
        } else {
            r_sort_key[i].cmp(&l_sort_key[i])
        };
        if res != cmp::Ordering::Equal {
            return res;
        }
    }
    Ordering::Equal
}

pub struct SortMergeJoin {
    #[allow(dead_code)]
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    schema: TableSchema,
    left_expr: Vec<(ByteCodeExpr, bool)>,
    right_expr: Vec<(ByteCodeExpr, bool)>,
    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    will_rewind: bool,

    // States (Reset on close)
    open: bool,
    left_child_read: bool,
    right_child_read: bool,
    left_sorted_data: Vec<(Vec<Field>, Tuple)>,
    right_sorted_data: Vec<(Vec<Field>, Tuple)>,
    l_first: usize,
    r_first: usize,
    l_end: usize,
    r_end: usize,
    l_cursor: usize,
    r_cursor: usize,
}

impl SortMergeJoin {
    pub fn new(
        managers: &'static Managers,
        schema: TableSchema,
        left_expr: Vec<(ByteCodeExpr, bool)>,
        right_expr: Vec<(ByteCodeExpr, bool)>,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Result<Self, CrustyError> {
        if left_expr.len() != right_expr.len() {
            return Err(c_err(
                "SMJ: Left and right expressions must have the same length",
            ));
        }
        if left_expr.is_empty() {
            return Err(c_err("SMJ: Join predicate cannot be empty"));
        }
        Ok(Self {
            managers,
            schema,
            left_expr,
            right_expr,
            left_child,
            right_child,
            will_rewind: true,
            open: false,
            left_sorted_data: vec![],
            right_sorted_data: vec![],
            l_first: 0,
            l_end: 0,
            l_cursor: 0,
            r_first: 0,
            r_end: 0,
            r_cursor: 0,
            left_child_read: false,
            right_child_read: false,
        })
    }
}

impl OpIterator for SortMergeJoin {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        // will_rewind is false for both children because the sort is stateful and rewinding sort operator does not rewind child
        self.left_child.configure(false);
        self.right_child.configure(false);
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            if !self.left_child_read {
                self.left_child.open()?;
                while let Some(left_tuple) = self.left_child.next()? {
                    let mut sort_key = Vec::new();
                    for (field, _) in &self.left_expr {
                        sort_key.push(field.eval(&left_tuple));
                    }
                    self.left_sorted_data.push((sort_key, left_tuple));
                }
                self.left_child.close()?;
                self.left_child_read = true;
                self.left_sorted_data
                    .sort_by(|(a_sort_key, _), (b_sort_key, _)| {
                        for (i, (_, asc)) in self.left_expr.iter().enumerate() {
                            let res = if *asc {
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
            }
            if !self.right_child_read {
                self.right_child.open()?;
                while let Some(right_tuple) = self.right_child.next()? {
                    let mut sort_key = Vec::new();
                    for (field, _) in &self.right_expr {
                        sort_key.push(field.eval(&right_tuple));
                    }
                    self.right_sorted_data.push((sort_key, right_tuple));
                }
                self.right_child.close()?;
                self.right_child_read = true;
                self.right_sorted_data
                    .sort_by(|(a_sort_key, _), (b_sort_key, _)| {
                        for (i, (_, asc)) in self.right_expr.iter().enumerate() {
                            let res = if *asc {
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
            }
            self.open = true;

            // dbg!("original left_sorted_data.len() = {}", self.left_sorted_data.len());
            // dbg!("original right_sorted_data.len() = {}", self.right_sorted_data.len());
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        loop {
            // If l_cursor \in [l_first, l_end) and r_cursor \in [r_first, r_end), then we have a match
            // dbg!("********************** next **********************");
            // dbg!((
            //     self.l_first,
            //     self.l_end,
            //     self.l_cursor,
            //     self.r_first,
            //     self.r_end,
            //     self.r_cursor
            // ));
            if self.l_first <= self.l_cursor
                && self.l_cursor < self.l_end
                && self.r_first <= self.r_cursor
                && self.r_cursor < self.r_end
            {
                // dbg!("returning tuple with cursor ({}/{}, {}/{})", self.l_cursor, self.left_sorted_data.len(), self.r_cursor, self.right_sorted_data.len());
                let left_tuple = &self.left_sorted_data[self.l_cursor].1;
                let right_tuple = &self.right_sorted_data[self.r_cursor].1;
                let new_tuple = left_tuple.merge(right_tuple);
                self.r_cursor += 1;
                if self.r_cursor == self.r_end {
                    self.r_cursor = self.r_first;
                    self.l_cursor += 1;
                }
                return Ok(Some(new_tuple));
            }
            // Finished reading from range [..., l_end) and [..., r_end)
            self.l_first = self.l_end;
            self.l_cursor = self.l_first;
            self.r_first = self.r_end;
            self.r_cursor = self.r_first;

            if !self.will_rewind {
                // remove the tuples we have already used for join from the sorted data
                // this means all the data in range [..., l_end) and [..., r_end) can be deleted
                self.left_sorted_data.drain(..self.l_end);
                self.l_first = 0;
                self.l_end = 0;
                self.l_cursor = 0;
                self.right_sorted_data.drain(..self.r_end);
                self.r_first = 0;
                self.r_end = 0;
                self.r_cursor = 0;
                // dbg!("left_sorted_data.len() = {}", self.left_sorted_data.len());
                // dbg!("right_sorted_data.len() = {}", self.right_sorted_data.len());
            }

            // Generate a new range of left and right tuples that have equal sort keys
            // dbg!("********************** generate new range **********************");
            while self.l_first < self.left_sorted_data.len()
                && self.r_first < self.right_sorted_data.len()
            {
                let l_tuple = &self.left_sorted_data[self.l_first];
                let r_tuple = &self.right_sorted_data[self.r_first];
                let ascs = self.left_expr.iter().map(|(_, asc)| *asc);
                match compare_keys(&l_tuple.0, &r_tuple.0, ascs.clone()) {
                    Ordering::Less => {
                        self.l_first += 1;
                    }
                    Ordering::Greater => {
                        self.r_first += 1;
                    }
                    Ordering::Equal => {
                        // Find the range of tuples in left and right that are equal to l_tuple and r_tuple
                        let mut temp_left = self.l_first;
                        let mut temp_right = self.r_first;
                        while temp_left < self.left_sorted_data.len()
                            && compare_keys(
                                &l_tuple.0,
                                &self.left_sorted_data[temp_left].0,
                                ascs.clone(),
                            ) == Ordering::Equal
                        {
                            temp_left += 1;
                        }
                        while temp_right < self.right_sorted_data.len()
                            && compare_keys(
                                &r_tuple.0,
                                &self.right_sorted_data[temp_right].0,
                                ascs.clone(),
                            ) == Ordering::Equal
                        {
                            temp_right += 1;
                        }
                        self.l_end = temp_left;
                        self.r_end = temp_right;
                        self.l_cursor = self.l_first;
                        self.r_cursor = self.r_first;
                        break;
                    }
                }
            }
            // dbg!((
            //     self.l_first,
            //     self.l_end,
            //     self.l_cursor,
            //     self.r_first,
            //     self.r_end,
            //     self.r_cursor
            // ));
            if self.l_first == self.left_sorted_data.len()
                || self.r_first == self.right_sorted_data.len()
            {
                return Ok(None);
            }
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // Children operators are closed in open()
        self.r_first = 0;
        self.l_first = 0;
        self.right_sorted_data.clear();
        self.left_sorted_data.clear();
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.l_first = 0;
        self.l_end = 0;
        self.l_cursor = 0;
        self.r_first = 0;
        self.r_end = 0;
        self.r_cursor = 0;
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
    use crate::testutil::new_test_managers;
    use crate::testutil::TestTuples;
    use common::query::bytecode_expr::{ByteCodeExpr, ByteCodes};
    use common::Field;

    #[allow(clippy::type_complexity)]
    fn get_join_predicate() -> (Vec<(ByteCodeExpr, bool)>, Vec<(ByteCodeExpr, bool)>) {
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

        let left_expr = vec![(left, false)];
        let right_expr = vec![(right, false)];
        (left_expr, right_expr)
    }

    fn get_iter(
        left_expr: Vec<(ByteCodeExpr, bool)>,
        right_expr: Vec<(ByteCodeExpr, bool)>,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let mut iter = Box::new(
            SortMergeJoin::new(
                managers,
                setup.schema.clone(),
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
            )
            .unwrap(),
        );
        iter.configure(false);
        iter
    }

    fn run_sort_merge_join(
        left_expr: Vec<(ByteCodeExpr, bool)>,
        right_expr: Vec<(ByteCodeExpr, bool)>,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(left_expr, right_expr);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod sort_merge_join_test {
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

            // left(col(0) + col(1)) == right(col(2))

            // Output:
            // 2 1 3 G 1 1 3 E
            // 2 1 3 G 2 1 3 G
            // 3 1 4 A 3 1 4 A
            // 3 1 4 A 4 2 4 G
            let (left_expr, right_expr) = get_join_predicate();
            let t = run_sort_merge_join(left_expr, right_expr);
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
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let (left_expr, right_expr) = get_join_predicate();
            let mut iter = get_iter(left_expr, right_expr);
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
