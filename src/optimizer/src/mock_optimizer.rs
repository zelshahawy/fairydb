use std::{cell::RefCell, rc::Rc};

use common::{
    physical_expr::physical_rel_expr::PhysicalRelExpr, query::query_registrar::QueryStateRegistrar,
};
use queryexe::{query::translate_and_validate::Query, Managers};

use crate::cost::CostModel;

pub struct MockOptimizer<C: CostModel> {
    /// Cost model used to estimate the cost of a plan. Using `Rc` to allow
    /// sharing the cost model with the `Conductor`. The `Conductor` may mutate
    /// the cost model (e.g. updating the statistics) and the optimizer should
    /// use the updated cost model.
    _cost_model: Rc<RefCell<C>>,

    ///Managers
    _managers: &'static Managers,
}

impl<C: CostModel + 'static> MockOptimizer<C> {
    pub fn new(cost_model: C, managers: &'static Managers) -> Self {
        Self {
            _cost_model: Rc::new(RefCell::new(cost_model)),
            _managers: managers,
        }
    }

    /// Optimize a logical plan and return the optimized physical plan
    pub fn optimize(
        &self,
        plan: &Query,
        _query_registrar: Option<&'static QueryStateRegistrar>,
    ) -> PhysicalRelExpr {
        // environment isn't important in a non-optimizing context
        let logical_plan = plan.get_plan();
        logical_plan.to_physical_plan()
    }
}
