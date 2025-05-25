use crate::{attribute::Attribute, ids::ContainerId};
use crate::{Constraint, DataType};
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

/// Table implementation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableInfo {
    pub c_id: ContainerId,
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: TableSchema,
}

impl TableInfo {
    pub fn new(c_id: ContainerId, name: String, schema: TableSchema) -> Self {
        TableInfo { c_id, name, schema }
    }
}

/// Handle schemas.
#[derive(Default, PartialEq, Eq, Clone, Debug)]
pub struct TableSchema {
    /// Attributes of the schema.
    pub attributes: Vec<Attribute>,
}

impl Serialize for TableSchema {
    /// Custom serialize to avoid serializing name_map.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.attributes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableSchema {
    /// Custom deserialize to avoid serializing name_map.
    fn deserialize<D>(deserializer: D) -> Result<TableSchema, D::Error>
    where
        D: Deserializer<'de>,
    {
        let attrs = Vec::deserialize(deserializer)?;
        Ok(TableSchema::new(attrs))
    }
}

impl TableSchema {
    /// Create a new schema.
    ///
    /// # Arguments
    ///
    /// * `attributes` - Attributes of the schema in the order that they are in the schema.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self { attributes }
    }

    /// Create a new schema with the given names and dtypes.
    ///
    /// # Arguments
    ///
    /// * `names` - Names of the new schema.
    /// * `dtypes` - Dypes of the new schema.
    pub fn from_vecs(names: Vec<&str>, dtypes: Vec<DataType>) -> Self {
        let mut attrs = Vec::new();
        for (name, dtype) in names.iter().zip(dtypes.iter()) {
            attrs.push(Attribute::new(name.to_string(), dtype.clone()));
        }
        TableSchema::new(attrs)
    }

    /// Get the attribute from the given index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the attribute to look for.
    pub fn get_attribute(&self, i: usize) -> Option<&Attribute> {
        self.attributes.get(i)
    }

    /// Get the index of the attribute.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to get the index for.
    pub fn get_field_index(&self, name: &str) -> Option<usize> {
        // parse the name
        // if it is a table_name.column_name, then use the column_name only
        // otherwise use the name as is
        for (i, attr) in self.attributes.iter().enumerate() {
            if attr.name == name {
                return Some(i);
            }
        }
        None
    }

    /// Returns attribute(s) that are primary keys
    ///
    ///
    pub fn get_pks(&self) -> Vec<Attribute> {
        let mut pk_attributes: Vec<Attribute> = Vec::new();
        for attribute in &self.attributes {
            if attribute.constraint == Constraint::PrimaryKey {
                pk_attributes.push(attribute.clone());
            }
        }
        pk_attributes
    }

    /// Check if the attribute name is in the schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to look for.
    pub fn contains(&self, name: &str) -> bool {
        for attr in &self.attributes {
            if attr.name == name {
                return true;
            }
        }
        false
    }

    /// Get an iterator of the attributes.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    /// Merge two schemas into one.
    ///
    /// The other schema is appended to the current schema.
    ///
    /// # Arguments
    ///
    /// * `other` - Other schema to add to current schema.
    pub fn merge(&self, other: &Self) -> Self {
        let mut attrs = self.attributes.clone();
        attrs.append(&mut other.attributes.clone());
        Self::new(attrs)
    }

    /// Returns the length of the schema.
    pub fn size(&self) -> usize {
        self.attributes.len()
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = String::new();
        for attr in &self.attributes {
            res.push_str(&attr.name);
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}
