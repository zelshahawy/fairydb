use common::ids::GroupId;
use queryexe::query::translate_and_validate::Query;
use queryexe::stats::reservoir_stat_manager::ReservoirStatManager;
use std::{iter::Sum, ops::Add};

use super::MemoNodeRefWrapper;

use super::{Cost, CostModel};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct DummyCost {
    value: f64,
}

impl Add for DummyCost {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self::new(self.value + other.value)
    }
}

impl Sum for DummyCost {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), |acc, x| acc + x)
    }
}

impl Cost for DummyCost {}

impl DummyCost {
    pub fn new(value: f64) -> Self {
        Self { value }
    }
}

impl Default for DummyCost {
    fn default() -> Self {
        Self::new(0.0)
    }
}

#[derive(Clone)]
pub struct DummyCostModel;

impl DummyCostModel {
    pub fn new(_stat_manager: &'static ReservoirStatManager) -> Self {
        Self
    }
}

impl CostModel for DummyCostModel {
    type Cost = DummyCost;

    fn set_up(&mut self, _query: &Query) {
        // Do nothing
    }

    fn calculate_cost<C: Cost>(
        &mut self,
        _gid: GroupId,
        _expr: MemoNodeRefWrapper<C>,
    ) -> DummyCost {
        DummyCost::new(0.0)
    }

    fn get_zero_cost(&self) -> DummyCost {
        DummyCost::new(0.0)
    }
}
