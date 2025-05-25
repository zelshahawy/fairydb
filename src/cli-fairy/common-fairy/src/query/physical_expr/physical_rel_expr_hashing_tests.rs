/// Moved physical_rel_expr hashing tests here because they are specific to a couple select functions
/// in the respective file. This suite basically just looks at the correctness of the intended behaviors
/// of our hashing.
#[cfg(test)]
mod test {
    use crate::{
        physical_expr::physical_rel_expr::PhysicalRelExpr, query::expr::Expression,
        query::join_type::JoinType, AggOp,
    };

    #[test]
    fn test_identical_trees() {
        // two identical trees
        let mut tree1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table_a".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table_a".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes match
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be identical for identical trees"
        );
    }

    #[test]
    fn test_tree_rehash_is_ok() {
        // two identical trees
        let mut tree = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table_a".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None, // hash is alr populated so should err
        };

        // hash twice to make sure it's fine to rehash a tree, and check val consistency after 2 hashes
        assert!(tree.hash_plan().is_ok());
        assert!(tree.get_tree_hash().is_ok());
        let v = tree.get_tree_hash();
        assert!(tree.hash_plan().is_ok());
        assert_eq!(tree.get_tree_hash(), v);
    }

    #[test]
    fn test_diff_col_order() {
        // two almost identical trees
        let mut tree1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table".to_string(),
            column_names: vec![2, 4, 3], // difference is here
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are not equal
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be same for trees with only diff col order"
        );
    }

    #[test]
    fn test_slightly_different_trees_1() {
        // two almost identical trees
        let mut tree1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table".to_string(),
            column_names: vec![2, 3, 5], // difference is here
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are not equal
        assert_ne!(
            hash1, hash2,
            "merkle hashes should be different for logically different trees"
        );
    }

    #[test]
    fn test_slightly_different_trees_2() {
        // two almost identical trees
        let mut tree1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test_table".to_string(),
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "testtable".to_string(), // difference is here
            column_names: vec![2, 3, 4],
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are not equal
        assert_ne!(
            hash1, hash2,
            "merkle hashes should be different for logically different trees"
        );
    }

    #[test]
    fn test_flatmap_identical_trees() {
        // two identical flatmap trees
        let mut tree1 = PhysicalRelExpr::FlatMap {
            tree_hash: None,
            input: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            func: Box::new(PhysicalRelExpr::Scan {
                cid: 2,
                table_name: "func_table".to_string(),
                column_names: vec![5, 6],
                tree_hash: None,
            }),
        };

        let mut tree2 = PhysicalRelExpr::FlatMap {
            tree_hash: None,
            input: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            func: Box::new(PhysicalRelExpr::Scan {
                cid: 2,
                table_name: "func_table".to_string(),
                column_names: vec![5, 6],
                tree_hash: None,
            }),
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes match
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be identical for identical FlatMap trees"
        );
    }

    #[test]
    fn test_map_different_expressions() {
        // two map trees with different expressions
        let mut tree1 = PhysicalRelExpr::Map {
            tree_hash: None,
            input: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            exprs: vec![(1, Expression::col_ref(2)), (2, Expression::col_ref(3))], // different expressions
        };

        let mut tree2 = PhysicalRelExpr::Map {
            tree_hash: None,
            input: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            exprs: vec![(1, Expression::col_ref(3)), (2, Expression::col_ref(4))], // different expressions
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are different
        assert_ne!(
            hash1, hash2,
            "merkle hashes should be different for Map trees with different expressions"
        );
    }

    #[test]
    fn test_equivalent_aggregate_trees() {
        // two logically equivalent aggregate trees
        let mut tree1 = PhysicalRelExpr::HashAggregate {
            tree_hash: None,
            src: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            group_by: vec![2, 3],
            aggrs: vec![(1, (4, AggOp::Sum))], // sum aggregation on column 4
        };

        let mut tree2 = PhysicalRelExpr::HashAggregate {
            tree_hash: None,
            src: Box::new(PhysicalRelExpr::Scan {
                cid: 1,
                table_name: "test_table".to_string(),
                column_names: vec![2, 3, 4],
                tree_hash: None,
            }),
            group_by: vec![3, 2], // same grouping because group_by order shouldn't matter
            aggrs: vec![(1, (4, AggOp::Sum))],
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes match
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be identical for identical Aggregate trees"
        );
    }

    #[test]
    fn test_slightly_different_deep_trees() {
        // create two identical trees with depth > 1
        let mut tree1 = PhysicalRelExpr::Select {
            src: Box::new(PhysicalRelExpr::HashJoin {
                join_type: JoinType::Inner,
                left: Box::new(PhysicalRelExpr::Scan {
                    cid: 1,
                    table_name: "left_table".to_string(),
                    column_names: vec![1, 2],
                    tree_hash: None,
                }),
                right: Box::new(PhysicalRelExpr::Scan {
                    cid: 2,
                    table_name: "right_table".to_string(),
                    column_names: vec![3, 4],
                    tree_hash: None,
                }),
                predicates: vec![],
                tree_hash: None,
            }),
            predicates: vec![],
            tree_hash: None,
        };

        let mut tree2 = PhysicalRelExpr::Select {
            src: Box::new(PhysicalRelExpr::HashJoin {
                join_type: JoinType::Inner,
                left: Box::new(PhysicalRelExpr::Scan {
                    cid: 1,
                    table_name: "left_table".to_string(),
                    column_names: vec![1, 2],
                    tree_hash: None,
                }),
                right: Box::new(PhysicalRelExpr::Scan {
                    cid: 2,
                    table_name: "right_table".to_string(),
                    column_names: vec![3, 5], // difference is here - it was 3, 4 in tree1
                    tree_hash: None,
                }),
                predicates: vec![],
                tree_hash: None,
            }),
            predicates: vec![],
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are not equal
        assert_ne!(
            hash1, hash2,
            "merkle hashes should be different for logically different trees"
        );
    }

    #[test]
    fn test_logically_equal_trees_simple() {
        let child1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "table1".to_string(),
            column_names: vec![2, 3],
            tree_hash: None,
        };
        let child2 = PhysicalRelExpr::Scan {
            cid: 2,
            table_name: "table2".to_string(),
            column_names: vec![2, 3],
            tree_hash: None,
        };

        // create two join trees with flipped right and left
        let mut tree1 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(child1.clone()),
            right: Box::new(child2.clone()),
            predicates: vec![],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(child2),
            right: Box::new(child1), // children are swapped in tree2
            predicates: vec![],
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are equal
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be identical for identical trees"
        );
    }

    #[test]
    fn test_logically_equal_trees_deep() {
        let child1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "table1".to_string(),
            column_names: vec![2, 3],
            tree_hash: None,
        };
        let child2 = PhysicalRelExpr::Scan {
            cid: 2,
            table_name: "table2".to_string(),
            column_names: vec![3, 2], // diff col order
            tree_hash: None,
        };

        let second_level_1 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(child1.clone()),
            right: Box::new(child2.clone()),
            predicates: vec![
                Expression::col_ref(1).eq(Expression::col_ref(2)),
                Expression::col_ref(3).eq(Expression::col_ref(4)),
            ],
            tree_hash: None,
        };

        let second_level_2 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(child2),
            right: Box::new(child1),
            predicates: vec![
                Expression::col_ref(1).eq(Expression::col_ref(2)),
                Expression::col_ref(3).eq(Expression::col_ref(4)),
            ], // same predicate order
            tree_hash: None,
        };

        // define doubly swapped trees (multiple mirrorings but logically same)
        let mut tree1 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(second_level_1.clone()),
            right: Box::new(second_level_2.clone()),
            predicates: vec![
                Expression::col_ref(1).eq(Expression::col_ref(2)),
                Expression::col_ref(3).eq(Expression::col_ref(4)),
            ],
            tree_hash: None,
        };
        let mut tree2 = PhysicalRelExpr::CrossJoin {
            join_type: JoinType::CrossJoin,
            left: Box::new(second_level_2),
            right: Box::new(second_level_1),
            predicates: vec![
                Expression::col_ref(3).eq(Expression::col_ref(4)),
                Expression::col_ref(1).eq(Expression::col_ref(2)),
            ], // different predicate order between tree 1 and tree 2
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let hash1 = tree1.hash_plan().unwrap();
        let hash2 = tree2.hash_plan().unwrap();

        // assert that hashes are equal
        assert_eq!(
            hash1, hash2,
            "merkle hashes should be identical for identical trees"
        );
    }

    #[test]
    fn test_invisible_renames() {
        use std::collections::HashMap;

        let mut scan = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test".to_string(),
            column_names: vec![1],
            tree_hash: None,
        };

        let mut rename_map1 = HashMap::new();
        rename_map1.insert(42, 420); // content irrelevant
        let mut rename = PhysicalRelExpr::Rename {
            src: Box::new(scan.clone()),
            src_to_dest: rename_map1,
            tree_hash: None,
        };

        let scan_hash = scan.hash_plan().unwrap();
        let rename_hash = rename.hash_plan().unwrap();
        assert_eq!(scan_hash, rename_hash, "rename should be invisible");
    }

    #[test]
    fn test_equivalent_trees_with_diff_rename_maps() {
        use std::collections::HashMap;

        // identical scan to be used in both trees
        let scan1 = PhysicalRelExpr::Scan {
            cid: 1,
            table_name: "test".to_string(),
            column_names: vec![1],
            tree_hash: None,
        };
        let scan2 = PhysicalRelExpr::Scan {
            cid: 0,
            table_name: "test".to_string(),
            column_names: vec![0],
            tree_hash: None,
        };

        // og renames
        let mut rename_map1 = HashMap::new();
        rename_map1.insert(1, 10001); // @1 -> @10001
        let rename1 = PhysicalRelExpr::Rename {
            src: Box::new(scan1.clone()),
            src_to_dest: rename_map1,
            tree_hash: None,
        };
        let mut rename_map2 = HashMap::new();
        rename_map2.insert(0, 10002); // @0 -> @10002
        let rename2 = PhysicalRelExpr::Rename {
            src: Box::new(scan2.clone()),
            src_to_dest: rename_map2,
            tree_hash: None,
        };

        // alt tree's renames
        let mut alt_rename_map1 = HashMap::new();
        alt_rename_map1.insert(1, 10003); // @1 -> @10003
        let alt_rename1 = PhysicalRelExpr::Rename {
            src: Box::new(scan1),
            src_to_dest: alt_rename_map1,
            tree_hash: None,
        };
        let mut alt_rename_map2 = HashMap::new();
        alt_rename_map2.insert(0, 10004); // @0 -> @10004
        let alt_rename2 = PhysicalRelExpr::Rename {
            src: Box::new(scan2),
            src_to_dest: alt_rename_map2,
            tree_hash: None,
        };

        // test that renames are equivalent
        let rename_hash_1 = rename1.clone().hash_plan().unwrap();
        let rename_hash_2 = rename2.clone().hash_plan().unwrap();
        let alt_rename_hash_1 = alt_rename1.clone().hash_plan().unwrap();
        let alt_rename_hash_2 = alt_rename2.clone().hash_plan().unwrap();
        assert_eq!(rename_hash_1, alt_rename_hash_1);
        assert_eq!(rename_hash_2, alt_rename_hash_2);

        // og
        let join = PhysicalRelExpr::HashJoin {
            join_type: JoinType::Inner,
            left: Box::new(rename1),
            right: Box::new(rename2),
            predicates: vec![Expression::col_ref(10001).eq(Expression::col_ref(10002))],
            tree_hash: None,
        };

        // alt
        let alt_join = PhysicalRelExpr::HashJoin {
            join_type: JoinType::Inner,
            left: Box::new(alt_rename1),
            right: Box::new(alt_rename2),
            predicates: vec![Expression::col_ref(10003).eq(Expression::col_ref(10004))],
            tree_hash: None,
        };

        // test that joins that take in renames are equivalent
        let join_hash = join.clone().hash_plan().unwrap();
        let alt_join_hash = alt_join.clone().hash_plan().unwrap();
        assert_eq!(join_hash, alt_join_hash, "renamed predicates should match");

        // final trees
        let mut og_tree = PhysicalRelExpr::Project {
            src: Box::new(join),
            cols: vec![10001],
            tree_hash: None,
        };
        let mut alt_tree = PhysicalRelExpr::Project {
            src: Box::new(alt_join),
            cols: vec![10003],
            tree_hash: None,
        };

        // compute Merkle hash for both trees
        let og_hash = og_tree.hash_plan().unwrap();
        let alt_hash = alt_tree.hash_plan().unwrap();

        assert_eq!(
            og_hash, alt_hash,
            "merkle hashes should be identical for identical trees"
        );
    }
}
