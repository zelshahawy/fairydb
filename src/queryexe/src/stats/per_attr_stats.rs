use common::{Field, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Clone)]
pub struct PerAttrStats {
    pub idx: usize, // index of the attribute in the schema
    pub min_val: Option<Field>,
    pub max_val: Option<Field>,
    pub distinct_count: Option<usize>,
    pub null_count: Option<usize>,
    pub total_count: Option<usize>, // might not be required since we can get this info from record_count of container_samples
                                    // TODO: distribution (can be histogram or other more advanced sketching techniques, like t-digest)
}

impl PerAttrStats {
    pub fn new(idx: usize) -> Self {
        PerAttrStats {
            idx,
            min_val: None,
            max_val: None,
            distinct_count: None,
            null_count: None,
            total_count: None,
        }
    }

    /// update_stats take in a vector of samples and updates the statistics for the attribute
    pub fn update_stats(&mut self, samples: &Vec<Tuple>) {
        let mut min_val = None;
        let mut max_val = None;
        let mut null_count = 0;

        for sample in samples {
            let field = sample.get_field(self.idx).unwrap();
            if let Field::Null = field {
                null_count += 1;
                continue;
            }

            if min_val.is_none() {
                min_val = Some(field.clone());
                max_val = Some(field.clone());
            } else {
                min_val = Some(min_val.unwrap().min(field.clone()));
                max_val = Some(max_val.unwrap().max(field.clone()));
            }
        }

        let distinct_vals: HashSet<&Field> = samples
            .iter()
            .map(|s| s.get_field(self.idx).unwrap())
            .collect();

        self.min_val = min_val;
        self.max_val = max_val;
        self.distinct_count = Some(distinct_vals.len());
        self.null_count = Some(null_count);
        self.total_count = Some(samples.len());
    }

    /// Get the distinct count of the attribute
    pub fn get_distinct_count(&self) -> Option<usize> {
        self.distinct_count
    }
}
