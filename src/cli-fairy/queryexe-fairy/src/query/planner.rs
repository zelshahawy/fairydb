use crate::{
    opiterator::{
        Aggregate, CrossJoin, Filter, HashEqJoin, NestedLoopJoin, OpIterator, Project, SeqScan,
    },
    Managers,
};
use common::{
    catalog::{get_column_index_from_temp_col_id, CatalogRef},
    error::c_err,
    ids::{ColumnId, LogicalTimeStamp, TransactionId},
    logical_expr::prelude::{Expression, JoinType},
    physical_expr::physical_rel_expr::PhysicalRelExpr,
    query::bytecode_expr::{ByteCodeExpr, ByteCodes},
    traits::plan::Plan,
    BinaryOp, CrustyError, TableSchema,
};
use std::collections::HashMap;

/// Convert a physical expression to a bytecode expression.
/// This function may take in `col_id_to_idx` (mapping from the unique column ID to the
/// index of the column in the schema) to replace the column references in the physical
/// expression with the corresponding index in the schema.
///
/// # Arguments
///
/// * `expr` - The physical expression to convert.
///
/// * `col_id_to_idx` - The mapping from the unique column ID to the index of the column
///
/// # Returns
///
/// * `Result<ByteCodeExpr, CrustyError>` - The converted bytecode expression.
pub fn convert_expr_to_bytecode<P: Plan>(
    expr: Expression<P>,
    col_id_to_idx: Option<&HashMap<ColumnId, ColumnId>>,
) -> Result<ByteCodeExpr, CrustyError> {
    let mut bound_expr = expr;
    if let Some(col_id_to_idx) = col_id_to_idx {
        bound_expr = bound_expr.replace_variables(col_id_to_idx);
    }
    let mut bytecode_expr = ByteCodeExpr::new();
    convert_expr_to_bytecode_inner(&bound_expr, &mut bytecode_expr)?;
    Ok(bytecode_expr)
}

/// Helper function called by `convert_ast_to_bytecode` to recursively convert the
/// physical expression to a bytecode expression. This function assumes that the
/// column references in the physical expression have been replaced with the
/// corresponding index in the schema.
///
/// # Arguments
///
/// * `expr` - The physical expression to convert.
///
/// * `bytecode_expr` - A mutable reference to the bytecode expression to be
///   constructed.
///
/// # Returns
///
/// * `Result<(), CrustyError>` - Ok(()) if the conversion is successful
fn convert_expr_to_bytecode_inner<P: Plan>(
    expr: &Expression<P>,
    bytecode_expr: &mut ByteCodeExpr,
) -> Result<(), CrustyError> {
    match expr {
        Expression::Field { val: l } => {
            let i = bytecode_expr.add_literal(l.clone());
            bytecode_expr.add_code(ByteCodes::PushLit as usize);
            bytecode_expr.add_code(i);
        }
        Expression::Binary { op, left, right } => {
            // (a+b)-(c+d) Bytecode will be [a][b][+][c][d][+][-]
            // i, Stack
            // 0, [a]
            // 1, [a][b]
            // 2, [a+b]
            // 3, [a+b][c]
            // 4, [a+b][c][d]
            // 5, [a+b][c+d]
            // 6, [a+b-c-d]
            convert_expr_to_bytecode_inner(left, bytecode_expr)?;
            convert_expr_to_bytecode_inner(right, bytecode_expr)?;
            match op {
                BinaryOp::Add => bytecode_expr.add_code(ByteCodes::Add as usize),
                BinaryOp::Sub => bytecode_expr.add_code(ByteCodes::Sub as usize),
                BinaryOp::Mul => bytecode_expr.add_code(ByteCodes::Mul as usize),
                BinaryOp::Div => bytecode_expr.add_code(ByteCodes::Div as usize),
                BinaryOp::Eq => bytecode_expr.add_code(ByteCodes::Eq as usize),
                BinaryOp::Neq => bytecode_expr.add_code(ByteCodes::Neq as usize),
                BinaryOp::Gt => bytecode_expr.add_code(ByteCodes::Gt as usize),
                BinaryOp::Ge => bytecode_expr.add_code(ByteCodes::Gte as usize),
                BinaryOp::Lt => bytecode_expr.add_code(ByteCodes::Lt as usize),
                BinaryOp::Le => bytecode_expr.add_code(ByteCodes::Lte as usize),
                BinaryOp::And => bytecode_expr.add_code(ByteCodes::And as usize),
                BinaryOp::Or => bytecode_expr.add_code(ByteCodes::Or as usize),
            }
        }
        Expression::ColRef { id: i } => {
            bytecode_expr.add_code(ByteCodes::PushField as usize);
            bytecode_expr.add_code(*i);
        }
        // TODO: Currently does not support `Case` and `Subquery` physical expressions
        _ => return Err(c_err("Unsupported expression")),
    }
    Ok(())
}

/// Convert a physical plan to an opiterator.
///
/// # Arguments
///
/// * `managers` - Managers struct (saved in ServerState)
///
/// * `catalog` - Shared ownership of the catalog
///
/// * `physical_plan` - Root of the physical plan tree
///
/// * `tid` - Transaction ID
///
/// * `timestamp` - Logical timestamp
///
/// # Returns
///
/// * `Result<Box<dyn OpIterator>, CrustyError>` - The converted root opiterator
pub fn physical_plan_to_op_iterator(
    managers: &'static Managers,
    catalog: &CatalogRef,
    physical_plan: &PhysicalRelExpr,
    tid: TransactionId,
    timestamp: LogicalTimeStamp,
) -> Result<Box<dyn OpIterator>, CrustyError> {
    let (result, _) =
        physical_plan_to_op_iterator_helper(managers, catalog, physical_plan, tid, timestamp);
    result
}

/// Helper function called by `physical_plan_to_op_iterator` to recursively convert the
/// physical plan to an opiterator.
///
/// # Arguments
///
/// * `managers` - Managers struct (saved in ServerState)
///
/// * `catalog` - Shared ownership of the catalog
///
/// * `physical_plan` - Root of the physical plan tree
///
/// * `tid` - Transaction ID
///
/// * `timestamp` - Logical timestamp
///
/// # Returns
///
/// * `Result<(Box<dyn OpIterator>, HashMap<ColumnId, ColumnId>), CrustyError>` -
///   The converted opiterator and a mapping from the unique column ID to the
///   index of the column in the schema
fn physical_plan_to_op_iterator_helper(
    managers: &'static Managers,
    catalog: &CatalogRef,
    physical_plan: &PhysicalRelExpr,
    tid: TransactionId,
    _timestamp: LogicalTimeStamp,
) -> (
    Result<Box<dyn OpIterator>, CrustyError>,
    HashMap<ColumnId, ColumnId>,
) {
    let err = CrustyError::ExecutionError(String::from("Malformed logical plan"));

    match physical_plan {
        PhysicalRelExpr::Scan {
            cid,
            table_name: _,
            column_names,
            ..
        } => {
            let in_schema = catalog.get_table_schema(*cid).unwrap();

            let mut out_schema_att = Vec::new();
            let mut fields = Vec::new();
            for name in column_names {
                // first locate the offset of the column in the base relation
                let name = get_column_index_from_temp_col_id(*name);
                out_schema_att.push(in_schema.get_attribute(name).unwrap().clone());
                fields.push(
                    convert_expr_to_bytecode(
                        Expression::<PhysicalRelExpr>::ColRef { id: name },
                        None,
                    )
                    .unwrap(),
                );
            }
            let out_schema = TableSchema::new(out_schema_att);

            let col_id_to_idx = column_names
                .iter()
                .enumerate()
                .map(|(i, id)| (*id, i as ColumnId))
                .collect::<HashMap<ColumnId, ColumnId>>();

            let scan_iter = SeqScan::new(managers, &out_schema, cid, tid, None, Some(fields));
            (Ok(Box::new(scan_iter)), col_id_to_idx)
        }

        PhysicalRelExpr::Project { src, cols, .. } => {
            let (src_iter, col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, src, tid, _timestamp);
            let input_schema = src_iter.as_ref().unwrap().get_schema();

            let indexes = cols
                .iter()
                .map(|id| match col_id_to_idx.get(id) {
                    Some(idx) => *idx,
                    None => panic!("Column id not found in col_id_to_idx"),
                })
                .collect::<Vec<_>>();
            let attrs = indexes
                .iter()
                .map(|i| input_schema.get_attribute(*i).unwrap().clone())
                .collect::<Vec<_>>();
            let schema = TableSchema::new(attrs);

            let project_iter = Project::new(
                cols.iter()
                    .map(|i| Expression::<PhysicalRelExpr>::ColRef { id: *i })
                    .map(|e| convert_expr_to_bytecode(e, Some(&col_id_to_idx)))
                    .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()
                    .unwrap(),
                schema,
                src_iter.unwrap(),
            );
            (
                Ok(Box::new(project_iter)),
                cols.iter()
                    .enumerate()
                    .map(|(i, id)| (*id, i as ColumnId))
                    .collect(),
            )
        }

        PhysicalRelExpr::Rename {
            src, src_to_dest, ..
        } => {
            let (src_iter, col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, src, tid, _timestamp);
            let new_col_id_to_index = col_id_to_idx
                .iter()
                .map(|(old_id, offset)| {
                    let new_id = src_to_dest.get(old_id).unwrap();
                    (*new_id, *offset)
                })
                .collect::<HashMap<ColumnId, ColumnId>>();
            (src_iter, new_col_id_to_index)
        }

        PhysicalRelExpr::Select {
            src, predicates, ..
        } => {
            let (src_iter, col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, src, tid, _timestamp);

            let mut bytecode_exprs = Vec::new();
            for pred in predicates {
                let bytecode_expr =
                    convert_expr_to_bytecode(pred.clone(), Some(&col_id_to_idx)).unwrap();
                bytecode_exprs.push(bytecode_expr);
            }

            let mut child = src_iter.unwrap();
            let mut cur_filter;
            for expr in bytecode_exprs {
                cur_filter = Filter::new(expr, child.get_schema().clone(), child);
                child = Box::new(cur_filter);
            }
            (Ok(child), col_id_to_idx)
        }

        PhysicalRelExpr::CrossJoin {
            join_type: _,
            left,
            right,
            predicates,
            ..
        } => {
            let (left_iter, left_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, left, tid, _timestamp);
            let (right_iter, right_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, right, tid, _timestamp);

            let left_schema = left_iter.as_ref().unwrap().get_schema();
            let right_schema = right_iter.as_ref().unwrap().get_schema();
            let new_schema = left_schema.merge(right_schema);

            let mut new_col_id_to_idx = left_col_id_to_idx;
            for (old_id, offset) in right_col_id_to_idx {
                new_col_id_to_idx.insert(old_id, offset + left_schema.size());
            }

            let join_op = Box::new(CrossJoin::new(
                new_schema.clone(),
                left_iter.unwrap(),
                right_iter.unwrap(),
            ));

            if predicates.is_empty() {
                return (Ok(join_op), new_col_id_to_idx);
            }

            // Code below looks ugly, but this is to avoid ownership issues
            let mut child_filter = Box::new(Filter::new(
                convert_expr_to_bytecode(predicates[0].clone(), Some(&new_col_id_to_idx)).unwrap(),
                new_schema.clone(),
                join_op,
            ));
            for pred in predicates.iter().skip(1) {
                let filter = Filter::new(
                    convert_expr_to_bytecode(pred.clone(), Some(&new_col_id_to_idx)).unwrap(),
                    new_schema.clone(),
                    child_filter,
                );
                child_filter = Box::new(filter);
            }
            (Ok(child_filter), new_col_id_to_idx)
        }

        PhysicalRelExpr::NestedLoopJoin {
            join_type,
            left,
            right,
            predicates,
            ..
        } => {
            debug_assert_eq!(join_type, &JoinType::Inner);
            let (left_iter, left_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, left, tid, _timestamp);
            let (right_iter, right_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, right, tid, _timestamp);

            let left_schema = left_iter.as_ref().unwrap().get_schema();
            let right_schema = right_iter.as_ref().unwrap().get_schema();
            let new_schema = left_schema.merge(right_schema);

            let mut new_col_id_to_idx = left_col_id_to_idx.clone();
            for (old_id, offset) in right_col_id_to_idx.iter() {
                new_col_id_to_idx.insert(*old_id, offset + left_schema.size());
            }

            debug_assert_eq!(predicates.len(), 1);

            let (join_op, left_expr, right_expr) = match &predicates[0] {
                Expression::Binary {
                    op,
                    left: left_expr,
                    right: right_expr,
                } => (*op, left_expr, right_expr),
                _ => {
                    panic!("Expected binary expression in nested loop join");
                }
            };

            let (left_col, right_col) = if left_expr.intersect_with(left) {
                (left_expr, right_expr)
            } else {
                (right_expr, left_expr)
            };

            let join = Box::new(NestedLoopJoin::new(
                join_op,
                convert_expr_to_bytecode(*left_col.clone(), Some(&left_col_id_to_idx)).unwrap(),
                convert_expr_to_bytecode(*right_col.clone(), Some(&right_col_id_to_idx)).unwrap(),
                left_iter.unwrap(),
                right_iter.unwrap(),
                new_schema,
            ));
            (Ok(join), new_col_id_to_idx)
        }

        PhysicalRelExpr::HashJoin {
            join_type,
            left,
            right,
            predicates,
            ..
        } => {
            debug_assert_eq!(join_type, &JoinType::Inner);
            let (left_iter, left_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, left, tid, _timestamp);
            let (right_iter, right_col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, right, tid, _timestamp);

            let left_schema = left_iter.as_ref().unwrap().get_schema();
            let right_schema = right_iter.as_ref().unwrap().get_schema();
            let new_schema = left_schema.merge(right_schema);

            let mut new_col_id_to_idx = left_col_id_to_idx.clone();
            for (old_id, offset) in right_col_id_to_idx.iter() {
                new_col_id_to_idx.insert(*old_id, offset + left_schema.size());
            }

            debug_assert_eq!(predicates.len(), 1);

            let (left_expr, right_expr) = match &predicates[0] {
                Expression::Binary {
                    op: BinaryOp::Eq,
                    left: left_expr,
                    right: right_expr,
                } => (left_expr, right_expr),
                _ => {
                    panic!("Expected binary eq expression in hash eq join");
                }
            };

            let (left_col, right_col) = if left_expr.intersect_with(left) {
                (left_expr, right_expr)
            } else {
                (right_expr, left_expr)
            };

            let join = Box::new(HashEqJoin::new(
                managers,
                new_schema,
                convert_expr_to_bytecode(*left_col.clone(), Some(&left_col_id_to_idx)).unwrap(),
                convert_expr_to_bytecode(*right_col.clone(), Some(&right_col_id_to_idx)).unwrap(),
                left_iter.unwrap(),
                right_iter.unwrap(),
            ));
            (Ok(join), new_col_id_to_idx)
        }

        PhysicalRelExpr::HashAggregate {
            src,
            group_by,
            aggrs,
            ..
        } => {
            let (src_iter, col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, src, tid, _timestamp);
            let in_schema = src_iter.as_ref().unwrap().get_schema();

            let mut out_schema_att = Vec::new();

            // First project the group by columns
            for id in group_by {
                let offset = col_id_to_idx.get(id).unwrap();
                out_schema_att.push(in_schema.get_attribute(*offset).unwrap().clone());
            }

            // Then project the aggregate columns
            for (_, (id, op)) in aggrs {
                let offset = col_id_to_idx.get(id).unwrap();
                let src_att = in_schema.get_attribute(*offset).unwrap();
                out_schema_att.push(op.to_attr(src_att));
            }
            let out_schema = TableSchema::new(out_schema_att);

            let mut group_by_exprs = Vec::new();
            let mut new_col_id_to_idx = HashMap::new();

            for (i, id) in group_by.iter().enumerate() {
                group_by_exprs.push(
                    convert_expr_to_bytecode(
                        Expression::<PhysicalRelExpr>::ColRef { id: *id },
                        Some(&col_id_to_idx),
                    )
                    .unwrap(),
                );
                new_col_id_to_idx.insert(*id, i as ColumnId);
            }

            let mut aggr_exprs = Vec::new();
            let mut ops = Vec::new();
            for (i, (dest_id, (src_id, op))) in aggrs.iter().enumerate() {
                aggr_exprs.push(
                    convert_expr_to_bytecode(
                        Expression::<PhysicalRelExpr>::ColRef { id: *src_id },
                        Some(&col_id_to_idx),
                    )
                    .unwrap(),
                );
                ops.push(*op);
                new_col_id_to_idx.insert(*dest_id, i as ColumnId + group_by.len());
            }

            let agg_iter = Aggregate::new(
                managers,
                group_by_exprs,
                aggr_exprs,
                ops,
                out_schema,
                src_iter.unwrap(),
            );
            (Ok(Box::new(agg_iter)), new_col_id_to_idx)
        }

        PhysicalRelExpr::Map { input, exprs, .. } => {
            let (src_iter, col_id_to_idx) =
                physical_plan_to_op_iterator_helper(managers, catalog, input, tid, _timestamp);
            let in_schema = src_iter.as_ref().unwrap().get_schema();

            // Projecting all the columns
            let mut out_schema_att = in_schema.attributes.clone();
            let mut fields = (0..in_schema.size())
                .map(|i| Expression::<PhysicalRelExpr>::ColRef { id: i })
                .map(|e| convert_expr_to_bytecode(e, None))
                .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()
                .unwrap();
            let mut new_col_id_to_idx = col_id_to_idx.clone();

            // Projecting the additional new columns (generated by the map expressions)
            for (i, (id, expr)) in exprs.iter().enumerate() {
                let bytecode_expr =
                    convert_expr_to_bytecode(expr.clone(), Some(&col_id_to_idx)).unwrap();
                fields.push(bytecode_expr);

                let new_att = expr.to_attr(in_schema, &col_id_to_idx);
                out_schema_att.push(new_att);

                new_col_id_to_idx.insert(*id, i as ColumnId + in_schema.size());
            }
            let out_schema = TableSchema::new(out_schema_att);
            let project_iter = Project::new(fields, out_schema, src_iter.unwrap());
            (Ok(Box::new(project_iter)), new_col_id_to_idx)
        }
        _ => (Err(err), HashMap::new()),
    }
}
