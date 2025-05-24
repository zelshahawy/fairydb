use crate::Managers;

use common::{
    datatypes::{default_decimal_precision, default_decimal_scale},
    prelude::*,
    traits::storage_trait::StorageTrait,
    traits::{stat_manager_trait::StatManagerTrait, state_tracker_trait::StateTrackerTrait},
    tuple::ConvertedResult,
    ConversionError,
};
use sqlparser::ast::{Value, Values};

pub(crate) fn insert_validated_tuples(
    table_id: ContainerId,
    tuples: &Vec<Tuple>,
    txn_id: TransactionId,
    managers: &'static Managers,
) -> Result<usize, CrustyError> {
    let mut tuples_bytes = Vec::new();
    for t in tuples {
        tuples_bytes.push(t.to_bytes());
    }
    let inserted = managers.sm.insert_values(table_id, tuples_bytes, txn_id);
    info!("TODO call tm and im for insert_values");
    let insert_count = inserted.len();
    if insert_count == tuples.len() {
        for (t, v) in tuples.iter().zip(inserted.iter()) {
            managers.stats.new_record(t, *v)?;
        }
        managers.stats.set_ts(table_id, txn_id.id());
        Ok(insert_count)
    } else {
        Err(CrustyError::ExecutionError(format!(
            "Attempting to insert {} tuples and only {} were inserted",
            tuples.len(),
            insert_count
        )))
    }
}

/// Check new or updated records to ensure that they do not break any constraints
pub(crate) fn validate_tuples(
    _table_id: &ContainerId,
    schema: &TableSchema,
    col_order: Option<Vec<usize>>,
    mut values: ConvertedResult,
    _txn_id: &TransactionId,
) -> Result<ConvertedResult, CrustyError> {
    if col_order.is_some() {
        return Err(CrustyError::CrustyError(String::from(
            "Col ordering not supported",
        )));
    }
    let mut values_to_remove: Vec<(usize, Vec<ConversionError>)> = Vec::new();
    warn!("PK, FK, Unique constraints not checked");
    for (i, rec) in values.converted.iter().enumerate() {
        for (j, (field, attr)) in (rec.field_vals()).zip(schema.attributes()).enumerate() {
            if let Field::Null = field {
                match attr.constraint {
                    common::Constraint::NotNull
                    | common::Constraint::UniqueNotNull
                    | common::Constraint::PrimaryKey
                    | common::Constraint::NotNullFKey(_) => {
                        values_to_remove.push((i, vec![ConversionError::NullFieldNotAllowed(j)]));
                    }
                    _ => continue, // Null value so nothing to check
                }
            }
            match (&attr.dtype, field) {
                (DataType::BigInt, Field::BigInt(_v)) => {
                    // Nothing for now
                }
                (DataType::String, Field::String(_v)) => {
                    // Nothing for now
                }
                (DataType::Date, Field::Date(_v)) => {
                    // Nothing for now
                }
                (DataType::Decimal(_, _), Field::Decimal(_, _)) => {
                    // Nothing for now
                }
                (DataType::Bool, Field::Bool(_v)) => {
                    // Nothing for now
                }
                _ => {
                    debug!("Wrong field: {} for attr type: {}", field, &attr.dtype);
                    values_to_remove.push((i, vec![ConversionError::WrongType]));
                }
            }
        }
    }
    // Remove in reverse order records that were invalid
    for i in values_to_remove.iter().rev() {
        values.converted.remove(i.0);
    }
    Ok(values)
}

/// Convert data from SQL parser insert and convert to internal representation
pub(crate) fn convert_insert_vals(values: &Values) -> Result<ConvertedResult, CrustyError> {
    let mut res = ConvertedResult {
        converted: Vec::new(),
        unconverted: Vec::new(),
    };

    for (i, val) in values.rows.iter().enumerate() {
        let mut fields = Vec::new();
        for field in val {
            if let sqlparser::ast::Expr::Value(value) = field {
                match value {
                    Value::Number(val, _long) => {
                        let parts = val.split('.').collect::<Vec<&str>>();
                        if parts.len() > 2 {
                            res.unconverted.push((i, vec![ConversionError::ParseError]));
                        } else if parts.len() == 2 {
                            if let Ok(field) = Field::from_str_to_decimal(
                                val,
                                default_decimal_precision(),
                                default_decimal_scale(),
                            ) {
                                fields.push(field);
                            } else {
                                res.unconverted.push((i, vec![ConversionError::ParseError]));
                            }
                        } else if let Ok(i) = Field::parse_int_from_str::<i64>(val) {
                            //TODO merged - add schema and check type
                            fields.push(Field::BigInt(i));
                        } else {
                            res.unconverted.push((i, vec![ConversionError::ParseError]));
                        }
                    }
                    Value::DoubleQuotedString(val) | Value::SingleQuotedString(val) => {
                        if let Ok(field) = Field::from_str_to_string(val) {
                            fields.push(field);
                        } else {
                            res.unconverted.push((i, vec![ConversionError::ParseError]));
                        }
                    }
                    Value::Null => {
                        fields.push(Field::Null);
                    }
                    _ => {
                        res.unconverted
                            .push((i, vec![ConversionError::UnsupportedType]));
                    }
                }
            } else {
                return Err(CrustyError::CrustyError(String::from(
                    "Only values supported in insert",
                )));
            }
        }
        res.converted.push(Tuple::new(fields));
    }
    Ok(res)
}
