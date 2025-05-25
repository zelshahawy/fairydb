pub mod col_id_generator;
pub mod config;
pub mod small_string;
pub mod tuple_conv0;
pub mod tuple_conv1;
pub mod tuple_conv2;
pub mod tuple_conv3;
pub mod tuple_writer;
use crate::Field;

/// For field changes
pub type TupleAssignments = Vec<(usize, Field)>;
