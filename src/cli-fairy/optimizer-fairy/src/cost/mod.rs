/*
 * Reference: https://github.com/yongwen/columbia/blob/master/header/supp.h
 */

use std::{fmt::Debug, ops::Add};

use common::ids::GroupId;
use queryexe::query::translate_and_validate::Query;

//TODO milestone qo
use DummyMemoNode as MemoNodeRefWrapper;

pub mod dummy_cost_model;

pub trait Cost: Default + Clone + PartialEq + PartialOrd + Debug + Add<Output = Self> {}

pub trait CostModel: Clone {
    type Cost: Cost;

    fn set_up(&mut self, query: &Query);

    fn calculate_cost<C: Cost>(&mut self, gid: GroupId, expr: MemoNodeRefWrapper<C>) -> Self::Cost;

    fn get_zero_cost(&self) -> Self::Cost;
}

#[allow(dead_code)]
pub struct DummyMemoNode<C: Cost> {
    pub cost: C,
}
