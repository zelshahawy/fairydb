use super::OpIterator;
use crate::Managers;
#[allow(unused_imports)]
use common::datatypes::f_decimal; // For generating a decimal field
use common::query::bytecode_expr::ByteCodeExpr;
use common::{AggOp, CrustyError, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    #[allow(dead_code)]
    // Static objects (No need to reset on close)
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Group by fields
    groupby_expr: Vec<ByteCodeExpr>,
    /// Aggregated fields.
    agg_expr: Vec<ByteCodeExpr>,
    /// Aggregation operations.
    ops: Vec<AggOp>,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    /// If true, then the operator will be rewinded in the future.
    will_rewind: bool,

    // States (Need to reset on close)
    /// Boolean if the iterator is open.
    open: bool,
    /// Accumulator for the aggregation. Key:groupby values. Value: (count, aggregated values).
    acc: HashMap<Vec<Field>, (usize, Vec<Field>)>, // groupby values -> (count, aggregate values)
    /// Accumulator iter
    acc_iter: Vec<Tuple>,
    /// Index of the current tuple in the accumulator iter
    index: usize,
}

impl Aggregate {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `managers` - Static managers.
    /// * `groupby_expr` - List of `ByteCodeExpr`s to groupby over. `ByteCodeExpr`s contains the field to groupby over.
    /// * `agg_expr` - List of `ByteCodeExpr`s to aggregate over. `ByteCodeExpr`s contains the field to aggregate over.
    /// * `ops` - List of `AggOp`s to aggregate over. `AggOp`s contains the aggregation function to apply.
    /// * `child` - Child operator to get the data from.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    pub fn new(
        managers: &'static Managers,
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
        schema: TableSchema,
        child: Box<dyn OpIterator>,
    ) -> Self {
        assert!(ops.len() == agg_expr.len());

        Self {
            managers,
            open: false,
            schema,
            groupby_expr,
            agg_expr,
            ops,
            child,
            will_rewind: true,
            acc: HashMap::new(),
            acc_iter: Vec::new(),
            index: 0,
        }
    }

    /// Updates the accumulated value with the given field_val.
    /// Hint: you can clone the acc value and then update it with *
    ///
    /// # Arguments
    ///
    /// * `op` - Aggregation operation.
    /// * `field_val` - Current value of the field to add to the accumulation.
    /// * `acc` - Current accumulated value.
    fn merge_fields(op: AggOp, field_val: &Field, acc: &mut Field) -> Result<(), CrustyError> {
        match op {
            AggOp::Count => *acc = (acc.clone() + Field::BigInt(1))?,
            AggOp::Max => {
                let max = max(acc.clone(), field_val.clone());
                *acc = max;
            }
            AggOp::Min => {
                let min = min(acc.clone(), field_val.clone());
                *acc = min;
            }
            AggOp::Sum => {
                *acc = (acc.clone() + field_val.clone())?;
            }
            AggOp::Avg => {
                *acc = (acc.clone() + field_val.clone())?; // This will be divided by the count later
            }
        }
        Ok(())
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// Hint: you are trying to update self.acc based on the group key and the agg_expr.
    /// Hint: self.agg_expr is a vec of `ByteCodeExpr`. For each one you can call eval(record: &Tuple) -> Field
    /// this will give you the value of the field in the tuple to either `merge_fields` to the acc or to create a new group.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        // Reference for extracting group key, which should be the key for self.acc
        let group_key = self
            .groupby_expr
            .iter()
            .map(|expr| expr.eval(tuple))
            .collect::<Vec<Field>>();
        let entry = self.acc.entry(group_key.clone()).or_insert_with(|| {
            // initial agg fields
            let mut init = Vec::with_capacity(self.ops.len());
            for (op, expr) in self.ops.iter().zip(self.agg_expr.iter()) {
                let first_val = expr.eval(tuple);
                let f = match op {
                    AggOp::Count => Field::BigInt(0),
                    AggOp::Sum | AggOp::Avg => Field::BigInt(0),
                    AggOp::Max | AggOp::Min => first_val.clone(),
                };
                init.push(f);
            }
            (0usize, init) // (count, agg fields)
        });

        // increment tuple count
        entry.0 += 1;

        for (i, op) in self.ops.iter().enumerate() {
            let val = self.agg_expr[i].eval(tuple);
            Self::merge_fields(*op, &val, &mut entry.1[i]).unwrap();
        }
    }
}

impl OpIterator for Aggregate {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        self.child.configure(false); // child of a aggregate will never be rewinded
                                     // because aggregate will buffer all the tuples from the child
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            // consume all input into acc
            self.child.open()?;
            while let Some(t) = self.child.next()? {
                self.merge_tuple_into_group(&t);
            }

            let groups = self
                .acc
                .iter()
                .map(|(k, (cnt, agg))| (k.clone(), *cnt, agg.clone()))
                .collect::<Vec<_>>();
            // Might add this groups.sort_by(|(k1, _, _), (k2, _, _)| k1.cmp(k2));

            // Output
            self.acc_iter.clear();
            for (key, cnt, agg) in groups {
                let mut row = key.clone();
                for (i, op) in self.ops.iter().enumerate() {
                    let out_field = match op {
                        AggOp::Avg => {
                            let sum_f = &agg[i];
                            let avg = match sum_f {
                                Field::BigInt(v) => (*v as f64) / (cnt as f64),
                                Field::Decimal(d, _) => (*d as f64) / (cnt as f64),
                                _ => panic!("AVG on non-numeric"),
                            };
                            f_decimal(avg)
                        }
                        _ => agg[i].clone(),
                    };
                    row.push(out_field);
                }
                self.acc_iter.push(Tuple::new(row));
            }

            self.index = 0;
            self.open = true;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if self.index < self.acc_iter.len() {
            let t = self.acc_iter[self.index].clone();
            self.index += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if self.open {
            self.child.close()?;
            self.acc.clear();
            self.acc_iter.clear();
            self.index = 0;
            self.open = false;
        }
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if !self.will_rewind {
            panic!("Cannot rewind a Aggregate with will_rewind set to false")
        }
        self.index = 0;
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
    use crate::testutil::{execute_iter, new_test_managers, TestTuples};
    use common::{
        datatypes::{f_int, f_str},
        query::bytecode_expr::colidx_expr,
    };

    fn get_iter(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let dummy_schema = TableSchema::new(vec![]);
        let mut iter = Box::new(Aggregate::new(
            managers,
            groupby_expr,
            agg_expr,
            ops,
            dummy_schema,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_aggregate(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(groupby_expr, agg_expr, ops);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod aggregation_test {
        use super::*;

        #[test]
        fn test_empty_group() {
            let group_by = vec![];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 1);
            assert_eq!(t[0], Tuple::new(vec![f_int(6), f_int(2), f_decimal(4.0)]));
        }

        #[test]
        fn test_empty_aggregation() {
            let group_by = vec![colidx_expr(2)];
            let agg = vec![];
            let ops = vec![];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 3);
            assert_eq!(t[0], Tuple::new(vec![f_int(3)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(4)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(5)]));
        }

        #[test]
        fn test_count() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Count];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 2
            // 1 4 1
            // 2 4 1
            // 2 5 2
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_int(2)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_int(1)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_int(1)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_int(2)]));
        }

        #[test]
        fn test_sum() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Sum];
            let tuples = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 3
            // 1 4 3
            // 2 4 4
            // 2 5 11
            assert_eq!(tuples.len(), 4);
            assert_eq!(tuples[0], Tuple::new(vec![f_int(1), f_int(3), f_int(3)]));
            assert_eq!(tuples[1], Tuple::new(vec![f_int(1), f_int(4), f_int(3)]));
            assert_eq!(tuples[2], Tuple::new(vec![f_int(2), f_int(4), f_int(4)]));
            assert_eq!(tuples[3], Tuple::new(vec![f_int(2), f_int(5), f_int(11)]));
        }

        #[test]
        fn test_max() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Max];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 G
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("G")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_min() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Min];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 E
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert!(t.len() == 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("E")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_avg() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 1.5
            // 1 4 3.0
            // 2 4 4.0
            // 2 5 5.5
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_decimal(1.5)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_decimal(3.0)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_decimal(4.0)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_decimal(5.5)]));
        }

        #[test]
        fn test_multi_column_aggregation() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(3)];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // A 1 1 4.0
            // E 1 1 3.0
            // G 4 2 4.25
            assert_eq!(t.len(), 3);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_str("A"), f_int(1), f_int(1), f_decimal(4.0)])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_str("E"), f_int(1), f_int(1), f_decimal(3.0)])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_str("G"), f_int(4), f_int(2), f_decimal(4.25)])
            );
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let group_by = vec![];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Avg];
            let _ = run_aggregate(group_by, agg, ops);
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(vec![colidx_expr(2)], vec![colidx_expr(0)], vec![AggOp::Max]);
            iter.configure(true); // if we will rewind in the future, then we set will_rewind to true
            let t_before = execute_iter(&mut *iter, true).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, true).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
