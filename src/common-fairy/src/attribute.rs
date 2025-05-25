use crate::{ids::ContainerId, DataType};

/// Handle attributes. Pairs the name with the dtype.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Attribute {
    /// Attribute name.
    pub name: String,
    /// Attribute dtype.
    pub dtype: DataType,
    /// Attribute constraint
    pub constraint: Constraint,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Constraint {
    None,
    PrimaryKey,
    Unique,
    NotNull,
    UniqueNotNull,
    ForeignKey(ContainerId), // Points to other table. Infer PK
    NotNullFKey(ContainerId),
}

impl Attribute {
    /// Create a new attribute with the given name and dtype.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute.
    /// * `dtype` - Dtype of the attribute.
    // pub fn new(name: String, dtype: DataType) -> Self { Self { name, dtype, is_pk: false } }
    pub fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::None,
        }
    }

    pub fn new_with_constraint(name: String, dtype: DataType, constraint: Constraint) -> Self {
        Self {
            name,
            dtype,
            constraint,
        }
    }

    pub fn new_pk(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::PrimaryKey,
        }
    }

    /// Returns the name of the attribute.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dtype of the attribute.
    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}
