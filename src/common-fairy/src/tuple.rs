use crate::{ids::TidType, ids::ValueId, ConversionError, Field};

/// Tuple type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Tuple {
    // Header
    /// The transaction id for concurrency control
    pub tid: TidType,
    #[serde(skip_serializing)]
    #[cfg(feature = "inlinecc")]
    /// Optionally used for read lock or read-ts
    pub read: TidType,

    #[serde(skip_serializing)]
    #[cfg(feature = "mvcc")]
    /// Used for multi-version systems
    pub begin_ts: TidType,

    #[serde(skip_serializing)]
    #[cfg(feature = "mvcc")]
    /// Used for multi-version systems
    pub end_ts: TidType,

    /// Used for multi-version systems, points to next version (older or newer)
    #[cfg(feature = "mvcc")]
    pub tuple_pointer: Option<ValueId>,

    #[serde(skip_serializing)]
    /// Used for query processing to track the source
    pub value_id: Option<ValueId>,

    /// Tuple data.
    pub field_vals: Vec<Field>,
}

impl Tuple {
    /// Create a new tuple with the given data.
    ///
    /// # Arguments
    ///
    /// * `field_vals` - Field values of the tuple.
    pub fn new(field_vals: Vec<Field>) -> Self {
        Self {
            tid: 0,
            value_id: None,
            field_vals,
            #[cfg(feature = "inlinecc")]
            read: 0,
            #[cfg(feature = "mvcc")]
            begin_ts: 0,
            #[cfg(feature = "mvcc")]
            end_ts: 0,
            #[cfg(feature = "mvcc")]
            tuple_pointer: None,
        }
    }

    /// Get the field at index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the field.
    pub fn get_field(&self, i: usize) -> Option<&Field> {
        self.field_vals.get(i)
    }

    /// Update the index at field.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the value to insert.
    /// * `f` - Value to add.
    ///
    /// # Panics
    ///
    /// Panics if the index is out-of-bounds.
    pub fn set_field(&mut self, i: usize, f: Field) {
        self.field_vals[i] = f;
    }

    /// Returns an iterator over the field values.
    pub fn field_vals(&self) -> impl Iterator<Item = &Field> {
        self.field_vals.iter()
    }

    /// Clippy wants this
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the length of the tuple.
    pub fn len(&self) -> usize {
        self.field_vals.len()
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for field in &self.field_vals {
            size += field.size();
        }
        size += std::mem::size_of::<TidType>();
        size += std::mem::size_of::<Option<ValueId>>();
        size
    }

    /// Append another tuple with self.
    ///
    /// # Arguments
    ///
    /// * `other` - Other tuple to append.
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields = self.field_vals.clone();
        fields.append(&mut other.field_vals.clone());
        Self::new(fields)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).unwrap()
    }

    pub fn to_csv(&self) -> String {
        let mut res = Vec::new();
        for field in &self.field_vals {
            res.push(field.to_string());
        }
        res.join(",")
    }
}

impl std::fmt::Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = String::new();
        for field in &self.field_vals {
            res.push_str(&field.to_string());
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}

/// The result of converting tuples for ingestion
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ConvertedResult {
    /// The records that converted succesfully
    pub converted: Vec<Tuple>,
    /// The list of records that did no convert by offset and issues
    pub unconverted: Vec<(usize, Vec<ConversionError>)>,
}

impl ConvertedResult {
    pub fn new() -> Self {
        Self {
            converted: Vec::new(),
            unconverted: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.converted.clear();
        self.unconverted.clear();
    }
}
