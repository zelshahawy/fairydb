use super::{per_attr_stats::PerAttrStats, SAMPLE_SIZE};
use common::{ids::ValueId, TableSchema, Tuple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;

#[derive(Serialize, Deserialize, Clone)]
pub struct ContainerSamples {
    pub samples: Vec<Tuple>,
    pub record_count: usize, // note that this is different from the number of samples
    pub schema: TableSchema,
    pub id_to_sample: HashMap<ValueId, usize>, // mapping from ValueID to index in samples

    // Attr Stats same order as the attributes in the schema
    pub per_attr_stats: Vec<PerAttrStats>,
    // A key map should be added, but this needs a catalog
}

/// Used only for (de)serialization purposes.
#[derive(Serialize, Deserialize)]
pub struct SerlializedContainerSamples {
    pub samples: Vec<Tuple>,
    pub record_count: usize,
    pub schema: TableSchema,
    pub id_to_sample: HashMap<String, usize>,
    pub per_attr_stats: Vec<PerAttrStats>,
}

impl ContainerSamples {
    /// new creates a new ContainerSamples struct with the given schema
    pub fn new(schema: TableSchema) -> Self {
        let per_attr_stats = schema
            .attributes
            .iter()
            .enumerate()
            .map(|(i, _)| PerAttrStats::new(i))
            .collect();
        Self {
            samples: Vec::with_capacity(SAMPLE_SIZE),
            record_count: 0,
            schema,
            id_to_sample: HashMap::with_capacity(SAMPLE_SIZE),
            per_attr_stats,
        }
    }

    /// add_sample adds a sample tuple to the vector of samples and keeps track of
    /// its value_id. If idx is None, sample will be appended to the end of the
    /// vector. Otherwise, add sample at the specified position.
    /// Finally, don't forget to update record_count
    /// Note: idx should only be supplied if the record count is equal to SAMPLE_SIZE
    /// (i.e., the vector is full and we are replacing a sample)
    pub fn add_sample(&mut self, tuple: Tuple, value_id: ValueId, idx: Option<usize>) {
        match idx {
            Some(i) => {
                self.samples[i] = tuple;
                self.id_to_sample.insert(value_id, i);
            }
            None => {
                self.samples.push(tuple);
                self.id_to_sample.insert(value_id, self.samples.len() - 1);
            }
        }
    }

    /// Increment the record count
    pub fn increment_record_count(&mut self) {
        self.record_count += 1;
    }

    /// Get PerAttrStats
    pub fn get_per_attr_stats(&self) -> &Vec<PerAttrStats> {
        &self.per_attr_stats
    }

    /// Get the number of samples in the container
    pub fn get_num_samples(&self) -> usize {
        self.samples.len()
    }

    /// Get the total number of records in the container
    pub fn get_record_count(&self) -> usize {
        self.record_count
    }

    /// update_attr_stats updates the per_attr_stats for the container samples
    /// If idx is None, all attributes are updated. Otherwise, only the attribute
    /// at the specified index is updated.
    /// In the future, this function may potentially be called by background threads
    /// Currently, this function is called in `estimate_distinct_prob`.
    pub fn update_attr_stats(&mut self, idx: Option<usize>) {
        match idx {
            Some(i) => {
                self.per_attr_stats[i].update_stats(&self.samples);
            }
            None => {
                for attr in self.per_attr_stats.iter_mut() {
                    attr.update_stats(&self.samples);
                }
            }
        }
    }

    /// Get the distinct count of the attribute at the specified index
    pub fn get_distinct_count(&self, idx: usize) -> Option<usize> {
        self.per_attr_stats[idx].get_distinct_count()
    }

    /// Get the distinct probability of the attribute at the specified index
    pub fn get_distinct_prob(&self, idx: usize) -> Option<f64> {
        self.get_distinct_count(idx)
            .map(|d| d as f64 / self.get_record_count() as f64)
    }

    pub fn get_serializable_container_sample(&self) -> SerlializedContainerSamples {
        let id_to_sample: HashMap<String, usize> = self
            .id_to_sample
            .clone()
            .iter()
            .map(|(v, n)| {
                // turn value id to lossless string rep for serialization
                let hex_key = v.to_bytes().iter().fold(String::new(), |mut acc, &byte| {
                    write!(acc, "{:02x}", byte).expect("write failed");
                    acc
                });
                (hex_key, *n)
            })
            .collect();

        SerlializedContainerSamples {
            samples: self.samples.clone(),
            record_count: self.record_count,
            schema: self.schema.clone(),
            id_to_sample,
            per_attr_stats: self.per_attr_stats.clone(),
        }
    }
}

// helper to get back to bytes from hex string we created in get_serializable_container_sample
fn hex_to_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2) // Process 2 characters at a time (each hex byte)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()) // Convert to byte
        .collect()
}

impl SerlializedContainerSamples {
    pub fn get_deserializede_container_sample(&self) -> ContainerSamples {
        let id_to_sample: HashMap<ValueId, usize> = self
            .id_to_sample
            .clone()
            .iter()
            .map(|(v, n)| {
                let value_id = ValueId::from_bytes(&hex_to_bytes(v));
                (value_id, *n)
            })
            .collect();

        ContainerSamples {
            samples: self.samples.clone(),
            record_count: self.record_count,
            schema: self.schema.clone(),
            id_to_sample,
            per_attr_stats: self.per_attr_stats.clone(),
        }
    }
}
