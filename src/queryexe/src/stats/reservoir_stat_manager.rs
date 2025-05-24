use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::fs;
use std::sync::Mutex;
use std::{collections::HashMap, path::PathBuf, sync::RwLock};

use common::datatypes::compare_fields;
use common::error::c_err;
use common::ids::StateInfo;
use common::logical_expr::prelude::Expression;
use common::physical::config::ServerConfig;
use common::physical_expr::physical_rel_expr::PhysicalRelExpr;
use common::query::bytecode_expr::ByteCodeExpr;
use common::testutil::get_rng;
use common::{
    ids::ContainerId, traits::stat_manager_trait::StatManagerTrait,
    traits::state_tracker_trait::StateTrackerTrait, Tuple, MANAGERS_DIR_NAME,
};
use common::{prelude::*, BinaryOp};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::query::planner::convert_expr_to_bytecode;

use super::container_samples::{ContainerSamples, SerlializedContainerSamples};
use super::SAMPLE_SIZE;

const PERSIST_CONFIG_FILENAME: &str = "reservoir_stat_manager";

pub struct ReservoirStatManager {
    storage_path: PathBuf,
    _mem_budget_mb: usize,
    samples: RwLock<HashMap<ContainerId, ContainerSamples>>,
    rng: Mutex<rand::rngs::SmallRng>,
    states: RwLock<HashMap<ContainerId, StateInfo>>,
}

/// Used only for (de)serialization purposes.
#[derive(Serialize, Deserialize)]
pub struct SerlializedReservoirStatManager {
    storage_path: PathBuf,
    _mem_budget_mb: usize,
    samples: HashMap<String, SerlializedContainerSamples>,
    states: HashMap<String, StateInfo>,
}

impl StateTrackerTrait for ReservoirStatManager {
    // TS tracking
    fn set_ts(&self, c_id: ContainerId, ts: LogicalTimeStamp) {
        let mut states = self.states.write().unwrap();
        if let Some(state) = states.get_mut(&c_id) {
            state.valid_low = ts;
            state.valid_high = ts;
        }
    }

    fn get_ts(&self, c_id: &ContainerId) -> (LogicalTimeStamp, LogicalTimeStamp) {
        let states = self.states.read().unwrap();
        if let Some(state) = states.get(c_id) {
            (state.valid_low, state.valid_high)
        } else {
            (0, 0)
        }
    }
}

impl StatManagerTrait for ReservoirStatManager {
    fn new(config: &'static ServerConfig, mem_budget: usize) -> Self {
        let mut stat_file = config.db_path.clone();
        stat_file.push(MANAGERS_DIR_NAME);
        stat_file.push(PERSIST_CONFIG_FILENAME);
        info!(
            "Creating new stat manager. Checking for config file {:?}",
            stat_file
        );
        if stat_file.exists() {
            info!("Loading stat manager from config file {:?}", stat_file);
            let reader =
                fs::File::open(stat_file.clone()).expect("error opening persist config file");
            let stat: SerlializedReservoirStatManager =
                serde_json::from_reader(reader).expect("error reading from json");

            let samples: HashMap<ContainerId, ContainerSamples> = stat
                .samples
                .into_iter()
                .map(|(k, v)| (k.parse().unwrap(), v.get_deserializede_container_sample()))
                .collect();
            let states: HashMap<ContainerId, StateInfo> = stat
                .states
                .into_iter()
                .map(|(k, v)| (k.parse().unwrap(), v))
                .collect();

            let cids_to_reset: Vec<u16> = states.clone().into_keys().collect();

            let res_stat_manager = ReservoirStatManager {
                storage_path: stat_file.clone(),
                _mem_budget_mb: mem_budget, // assuming we want a new mem budget, we don't inherit from stored val
                samples: RwLock::new(samples),
                rng: Mutex::new(get_rng()), // not sure if this is okay across runs (but we can't serlialize this so)
                states: RwLock::new(states),
            };

            for cid in cids_to_reset {
                res_stat_manager.set_ts(cid, 0); // we are starting anew so we act as if all "latest updates" to tables happened at the very start
            }

            return res_stat_manager;
        }
        ReservoirStatManager {
            storage_path: stat_file.clone(),
            _mem_budget_mb: mem_budget,
            samples: RwLock::new(HashMap::new()),
            rng: Mutex::new(get_rng()),
            states: RwLock::new(HashMap::new()),
        }
    }

    fn reset(&self) -> Result<(), CrustyError> {
        self.samples.write().unwrap().clear();
        self.states.write().unwrap().clear();
        Ok(())
    }

    fn shutdown(&self) -> Result<(), CrustyError> {
        let path = self.storage_path.clone();

        // e2e tests lack context for parent dir to exist, so we create parent (not needed here, but just in case)
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("Failed to create directory");
        }

        serde_json::to_writer(
            fs::File::create(path.clone()).expect("error creating file"),
            &self.get_serializable_stat_manager(),
        )
        .map_err(|e| c_err(&format!("failed to serialize: {}", e)))?;

        info!("Stat manager saved to {}", path.to_str().unwrap());
        Ok(())
    }

    fn register_table(&self, c_id: ContainerId, schema: TableSchema) -> Result<(), CrustyError> {
        let mut samples = self.samples.write().unwrap();
        let mut states = self.states.write().unwrap();
        match samples.entry(c_id) {
            Vacant(e) => {
                e.insert(ContainerSamples::new(schema));
                states.insert(c_id, StateInfo::new(c_id, true));
            }
            Occupied(_) => {}
        }
        Ok(())
    }

    fn deleted_record(&self, _value_id: &ValueId) -> Result<(), CrustyError> {
        todo!()
    }

    fn updated_record(
        &self,
        _tuple: &Tuple,
        _value_id: &ValueId,
        _old_value_id: Option<&ValueId>,
    ) -> Result<(), CrustyError> {
        todo!()
    }

    fn new_record(&self, tuple: &Tuple, value_id: ValueId) -> Result<(), CrustyError> {
        let mut samples = self.samples.write().unwrap();
        if !samples.contains_key(&value_id.container_id) {
            return Err(CrustyError::CrustyError(
                "Container not found/registered".to_string(),
            ));
        }

        let container_samples = samples.get_mut(&value_id.container_id).unwrap();
        let mut idx: Option<usize> = None;
        if container_samples.get_num_samples() == SAMPLE_SIZE {
            // if samples is full, replace a random sample
            idx = Some(self.rng.lock().unwrap().random_range(0..SAMPLE_SIZE));
        }
        container_samples.add_sample(tuple.clone(), value_id, idx);
        container_samples.increment_record_count();
        Ok(())
    }

    fn get_container_record_count(&self, c_id: ContainerId) -> Result<usize, CrustyError> {
        let samples = self.samples.read().unwrap();
        let container_samples = samples
            .get(&c_id)
            .unwrap_or_else(|| panic!("Container {} not found", c_id));
        Ok(container_samples.get_record_count())
    }

    /// Given a container and a predicate, estimate the selectivity of the predicate.
    /// This is done by evaluating the predicate on the sample and then dividing the result by the
    /// number of samples.
    fn estimate_count_and_sel(
        &self,
        c_id: ContainerId,
        predicate: &[Expression<PhysicalRelExpr>],
    ) -> Result<(usize, f64), CrustyError> {
        let predicate = predicate
            .iter()
            .map(|p| convert_expr_to_bytecode(p.clone(), None))
            .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()?;

        let samples = self.samples.read().unwrap();
        let container_samples = samples
            .get(&c_id)
            .ok_or(CrustyError::CrustyError("Container not found".to_string()))?;

        if container_samples.samples.is_empty() {
            return Ok((0, 0.0));
        }

        let matching_count = container_samples
            .samples
            .iter()
            .filter(|tuple| predicate.iter().all(|p| p.eval(tuple) == Field::Bool(true)))
            .count();

        let sample_size = container_samples.samples.len();

        let selectivity = matching_count as f64 / sample_size as f64;
        let count = (selectivity * container_samples.record_count as f64) as usize;
        Ok((count, selectivity))
    }

    fn estimate_join_count_and_sel(
        &self,
        left_c_id: ContainerId,
        right_c_id: ContainerId,
        join_ops: &[BinaryOp],
        left_exprs: &[Expression<PhysicalRelExpr>],
        right_exprs: &[Expression<PhysicalRelExpr>],
    ) -> Result<(usize, f64), CrustyError> {
        debug_assert_eq!(join_ops.len(), left_exprs.len());
        debug_assert_eq!(join_ops.len(), right_exprs.len());

        let left_exprs = left_exprs
            .iter()
            .map(|p| convert_expr_to_bytecode(p.clone(), None))
            .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()?;
        let right_exprs = right_exprs
            .iter()
            .map(|p| convert_expr_to_bytecode(p.clone(), None))
            .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()?;

        let samples = self.samples.read().unwrap();
        let left_container_samples = samples.get(&left_c_id).ok_or(CrustyError::CrustyError(
            "Container 1 not found".to_string(),
        ))?;
        let right_container_samples = samples.get(&right_c_id).ok_or(CrustyError::CrustyError(
            "Container 2 not found".to_string(),
        ))?;

        if left_container_samples.samples.is_empty() || right_container_samples.samples.is_empty() {
            return Ok((0, 0.0));
        }

        let mut matching_count = 0;

        for left_tuple in &left_container_samples.samples {
            for right_tuple in &right_container_samples.samples {
                if join_ops
                    .iter()
                    .zip(left_exprs.iter().zip(right_exprs.iter()))
                    .all(|(op, (left_expr, right_expr))| {
                        let left_field = left_expr.eval(left_tuple);
                        let right_field = right_expr.eval(right_tuple);
                        compare_fields(*op, &left_field, &right_field)
                    })
                {
                    matching_count += 1;
                }
            }
        }

        let left_sample_size = left_container_samples.samples.len();
        let right_sample_size = right_container_samples.samples.len();
        let selectivity = matching_count as f64 / (left_sample_size * right_sample_size) as f64;
        let count = (selectivity
            * left_container_samples.record_count as f64
            * right_container_samples.record_count as f64) as usize;
        Ok((count, selectivity))
    }

    fn estimate_distinct_prob(
        &self,
        c_id: ContainerId,
        col_id: ColumnId,
    ) -> Result<f64, CrustyError> {
        let distinct_prob;

        {
            let samples = self.samples.read().unwrap();
            let container_samples = samples
                .get(&c_id)
                .ok_or(CrustyError::CrustyError("Container not found".to_string()))?;

            if container_samples.samples.is_empty() {
                return Ok(0.0);
            }

            distinct_prob = container_samples.get_distinct_prob(col_id);
        }

        match distinct_prob {
            Some(prob) => Ok(prob),
            None => {
                let mut samples = self.samples.write().unwrap();
                let container_samples = samples.get_mut(&c_id).unwrap();
                container_samples.update_attr_stats(Some(col_id));
                Ok(container_samples.get_distinct_prob(col_id).unwrap())
            }
        }
    }
}

impl ReservoirStatManager {
    pub fn new_test_stat_manager() -> ReservoirStatManager {
        ReservoirStatManager::new(Box::leak(Box::new(ServerConfig::temporary())), 1000)
    }

    fn get_serializable_stat_manager(&self) -> SerlializedReservoirStatManager {
        let r = self.samples.read().unwrap();
        let samples = r
            .clone()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.get_serializable_container_sample()))
            .collect();
        let r = self.states.read().unwrap();
        let states = r
            .clone()
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        SerlializedReservoirStatManager {
            storage_path: self.storage_path.clone(),
            _mem_budget_mb: self._mem_budget_mb,
            samples,
            states,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::ids::ValueId;
    use common::logical_expr::prelude::Expression;
    use common::physical_expr::physical_rel_expr::PhysicalRelExpr;
    use common::testutil::*;
    use common::traits::stat_manager_trait::StatManagerTrait;
    use common::Tuple;

    fn gen_test_stat_manager() -> ReservoirStatManager {
        let config = Box::leak(Box::new(ServerConfig::temporary()));
        let mem_budget = 1000;
        ReservoirStatManager::new(config, mem_budget)
    }

    #[test]
    fn test_reservoir_stat_manager() {
        let stat_manager = gen_test_stat_manager();
        let value_id = ValueId::new(1);
        let tuple = Tuple::new(vec![]);
        let schema = TableSchema::new(vec![]);
        stat_manager.register_table(1, schema).unwrap();
        let result = stat_manager.new_record(&tuple, value_id);
        assert_eq!(result, Ok(()));

        let err_expected = stat_manager.new_record(&tuple, ValueId::new(2));
        assert_eq!(
            err_expected,
            Err(CrustyError::CrustyError(
                "Container not found/registered".to_string()
            ))
        );
    }

    #[test]
    fn test_small_single_container() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 1000;
        let mut rng = get_rng();
        let (table, mut tuples) = gen_test_table_and_tuples(&mut rng, c_id, tuple_count);
        let mut count = 0;
        stat_manager.register_table(c_id, table.schema).unwrap();
        // Check that the first 1000 records fill the reservoir
        for tuple in &tuples {
            let result = stat_manager.new_record(tuple, value_id);
            count += 1;
            assert_eq!(result, Ok(()));
            assert_eq!(stat_manager.get_container_record_count(c_id), Ok(count));
        }
        {
            let samples = stat_manager.samples.read().unwrap();
            let container_samples = samples.get(&value_id.container_id).unwrap();
            assert_eq!(container_samples.samples.len(), SAMPLE_SIZE);
        }

        // Add new records and check size
        tuples = gen_test_tuples(&mut rng, tuple_count);
        for tuple in &tuples {
            let result = stat_manager.new_record(tuple, value_id);
            count += 1;
            assert_eq!(result, Ok(()));
            assert_eq!(stat_manager.get_container_record_count(c_id), Ok(count));
        }
        {
            let samples = stat_manager.samples.read().unwrap();
            let container_samples = samples.get(&value_id.container_id).unwrap();
            assert_eq!(container_samples.samples.len(), SAMPLE_SIZE);
        }
    }

    #[test]
    fn test_estimated_record_count_simple() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let tuple_count = 1;
        let mut rng = get_rng();
        let (table, tuples) = gen_test_table_and_tuples(&mut rng, c_id, tuple_count);
        stat_manager.register_table(c_id, table.schema).unwrap();
        let tuple = tuples.first().unwrap();
        let f1 = tuple.get_field(0).unwrap();

        let predicate = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expression::<PhysicalRelExpr>::ColRef { id: 1 }), // Referring to "ia1"
            right: Box::new(Expression::<PhysicalRelExpr>::Field { val: f1.to_owned() }),
        };
        let estimated_count_res = stat_manager.estimate_count_and_sel(2, &[predicate.clone()]);
        assert_eq!(
            estimated_count_res,
            Err(CrustyError::CrustyError("Container not found".to_string()))
        );

        let estimated_count_res = stat_manager.estimate_count_and_sel(c_id, &[predicate.clone()]);
        assert_eq!(estimated_count_res, Ok((0, 0.0)));
        stat_manager.new_record(tuple, ValueId::new(c_id)).unwrap();
        let (estimated_count, _) = stat_manager
            .estimate_count_and_sel(c_id, &[predicate])
            .unwrap();
        assert!(estimated_count == 0 || estimated_count == 1);
    }

    #[test]
    fn test_estimated_record_count_predicate() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 10000;
        let mut rng = get_rng();
        let (table, tuples) = gen_test_table_and_tuples(&mut rng, c_id, tuple_count);
        stat_manager.register_table(c_id, table.schema).unwrap();

        // ia1 should be uniformly distributed 0-9
        let predicate = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expression::<PhysicalRelExpr>::ColRef { id: 1 }), // Referring to "ia1"
            right: Box::new(Expression::<PhysicalRelExpr>::Field {
                val: Field::BigInt(1),
            }),
        };
        for tuple in &tuples {
            stat_manager.new_record(tuple, value_id).unwrap();
        }
        let (mut estimated_count, mut est_sel) = stat_manager
            .estimate_count_and_sel(c_id, &[predicate])
            .unwrap();
        info!("Estimated count: {} Est Sel: {}", estimated_count, est_sel);
        // Being rough here, but should be around 8000
        assert!((7000..=9000).contains(&estimated_count));
        assert!((0.7..=0.9).contains(&est_sel));

        let predicate = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expression::<PhysicalRelExpr>::ColRef { id: 1 }), // Referring to "ia1"
            right: Box::new(Expression::<PhysicalRelExpr>::Field {
                val: Field::BigInt(2),
            }),
        };

        // Being rough here, but should be around 7000
        (estimated_count, est_sel) = stat_manager
            .estimate_count_and_sel(c_id, &[predicate])
            .unwrap();
        info!(
            "Estimated count: {} estimate_sel {}",
            estimated_count, est_sel
        );
        assert!((6500..=7500).contains(&estimated_count));
        assert!((0.65..=0.75).contains(&est_sel));
    }

    #[test]
    fn test_estimated_record_count() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 1000;
        let mut rng = get_rng();
        let (table, tuples) = gen_test_table_and_tuples(&mut rng, c_id, tuple_count);
        stat_manager.register_table(c_id, table.schema).unwrap();

        for tuple in &tuples {
            stat_manager.new_record(tuple, value_id).unwrap();
        }

        let predicate = Expression::<PhysicalRelExpr>::Binary {
            op: BinaryOp::Gt,
            left: Box::new(Expression::<PhysicalRelExpr>::ColRef { id: 1 }), // Referring to "ia1"
            right: Box::new(Expression::<PhysicalRelExpr>::Field {
                val: Field::BigInt(1),
            }),
        };

        let (estimated_count, _) = stat_manager
            .estimate_count_and_sel(c_id, &[predicate])
            .unwrap();
        assert!(estimated_count <= tuple_count.try_into().unwrap());
    }

    #[test]
    fn test_estimate_join_selectivity() {
        let stat_manager = gen_test_stat_manager();

        let left_c_id = 1;
        let left_tuple_count = 10000;

        let right_c_id = 2;
        let right_tuple_count = 10000;

        let mut rng = get_rng();
        let (table, left_tuples) = gen_test_table_and_tuples(&mut rng, left_c_id, left_tuple_count);

        stat_manager
            .register_table(left_c_id, table.schema)
            .unwrap();
        for tuple in &left_tuples {
            stat_manager
                .new_record(tuple, ValueId::new(left_c_id))
                .unwrap();
        }

        let (table, right_tuples) =
            gen_test_table_and_tuples(&mut rng, right_c_id, right_tuple_count);
        stat_manager
            .register_table(right_c_id, table.schema)
            .unwrap();
        for tuple in &right_tuples {
            stat_manager
                .new_record(tuple, ValueId::new(right_c_id))
                .unwrap();
        }

        // ia2: 0~999, ia3: 0~99, so the prob for eqs should be 0.001
        let ops = vec![BinaryOp::Eq];
        let left_expr = Expression::<PhysicalRelExpr>::ColRef { id: 3 }; // Referring to "ia3"
        let right_expr = Expression::<PhysicalRelExpr>::ColRef { id: 2 }; // Referring to "ia2"

        // Estimate join selectivity
        let (estimated_count, est_sel) = stat_manager
            .estimate_join_count_and_sel(left_c_id, right_c_id, &ops, &[left_expr], &[right_expr])
            .unwrap();
        info!(
            "Estimated count: {}, Estimate_sel: {}",
            estimated_count, est_sel
        );

        assert!((70000..=130000).contains(&estimated_count));
        assert!((0.0007..=0.0013).contains(&est_sel));
    }
}
