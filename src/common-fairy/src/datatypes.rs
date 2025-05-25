use crate::attribute::Attribute;

#[allow(unused_imports)]
use crate::error::{c_err, CrustyError};
use crate::query::bytecode_expr::{And, FromBool, Or};
use crate::BinaryOp;
use chrono::{Duration, NaiveDate};
use std::ops::{Add, Div, Mul, Sub};

pub fn base_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

pub fn null_string() -> String {
    String::from("NULL")
}

pub fn default_decimal_precision() -> u32 {
    10
}

pub fn default_decimal_scale() -> u32 {
    4
}

/// Utilities
pub fn f_int(i: i64) -> Field {
    Field::BigInt(i)
}

pub fn f_str(s: &str) -> Field {
    Field::String(s.to_string())
}

pub fn f_decimal(f: f64) -> Field {
    let s = default_decimal_scale();
    let whole = (f * 10f64.powi(s as i32)) as i64;
    Field::Decimal(whole, s)
}

pub fn f_date(s: &str) -> Field {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    let days = date.signed_duration_since(base_date()).num_days();
    Field::Date(days)
}

/// Enumerate the supported dtypes.
/// When adding a new dtype, make sure to add a corresponding field type.
#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    BigInt,
    Int,
    SmallInt,
    Char(u8), // Length
    String,
    Decimal(u32, u32), // Precision, Scale : Precision is total number of digits, scale is number of digits after decimal
    Date,
    Bool,
    Null,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::BigInt => write!(f, "bigint"),
            DataType::Int => write!(f, "int"),
            DataType::SmallInt => write!(f, "smallint"),
            DataType::Char(n) => write!(f, "char-fixed-{},", n),
            DataType::String => write!(f, "string"),
            DataType::Decimal(p, s) => write!(f, "decimal({},{})", p, s),
            DataType::Date => write!(f, "date"),
            DataType::Bool => write!(f, "bool"),
            DataType::Null => write!(f, "null"),
        }
    }
}

impl From<&Field> for DataType {
    fn from(f: &Field) -> Self {
        match f {
            Field::BigInt(_) => DataType::BigInt,
            Field::Int(_) => DataType::Int,
            Field::SmallInt(_) => DataType::SmallInt,
            Field::Char(i, _) => DataType::Char(*i),
            Field::String(_) => DataType::String,

            // FIXME: I don't think I'm using the right precision and scale here
            // (should get the number of digits in the whole number and the number
            // of digits after the decimal point)
            Field::Decimal(p, s) => DataType::Decimal(*p as u32, *s),
            Field::Date(_) => DataType::Date,
            Field::Bool(_) => DataType::Bool,
            Field::Null => DataType::Null,
        }
    }
}

impl DataType {
    /// Returns the size of the data type in bytes.
    /// Returns None if the size is variable.
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::BigInt => Some(8),
            DataType::Int => Some(4),
            DataType::SmallInt => Some(2),
            DataType::Char(i) => Some(*i as usize),
            DataType::String => None,
            DataType::Decimal(_, _) => Some(12),
            DataType::Date => Some(8),
            DataType::Bool => Some(1),
            DataType::Null => Some(1),
        }
    }
}

/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    BigInt(i64),
    Int(i32),
    SmallInt(i16),
    Char(u8, String), // Length, Value
    String(String),
    Decimal(i64, u32), // Whole, Scale : Whole is the integer part and fractional part combined, scale is number of digits after decimal
    Date(i64),         // Days relative to 1970-01-01
    Bool(bool),
    Null,
}

impl FromBool for Field {
    fn from_bool(b: bool) -> Self {
        Field::Bool(b)
    }
}

impl And for Field {
    fn and(&self, other: &Self) -> Self {
        match (self, other) {
            (Field::Bool(a), Field::Bool(b)) => Field::Bool(*a && *b),
            _ => panic!("Expected bool"),
        }
    }
}

impl Or for Field {
    fn or(&self, other: &Self) -> Self {
        match (self, other) {
            (Field::Bool(a), Field::Bool(b)) => Field::Bool(*a || *b),
            _ => panic!("Expected bool"),
        }
    }
}

impl Add for Field {
    type Output = Result<Self, CrustyError>;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::BigInt(a), Field::BigInt(b)) => Ok(Field::BigInt(a + b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                let res_scale = if s_l > s_r { s_l } else { s_r };
                let adjusted_a = a * 10i64.pow(res_scale - s_l);
                let adjusted_b = b * 10i64.pow(res_scale - s_r);
                Ok(Field::Decimal(adjusted_a + adjusted_b, res_scale))
            }
            (Field::BigInt(a), Field::Decimal(b, s_r)) => {
                let adjusted_a = a * 10i64.pow(s_r);
                Ok(Field::Decimal(adjusted_a + b, s_r))
            }
            (Field::Decimal(a, s_l), Field::BigInt(b)) => {
                let adjusted_b = b * 10i64.pow(s_l);
                Ok(Field::Decimal(a + adjusted_b, s_l))
            }
            _ => panic!("Expected int or decimal"),
        }
    }
}

impl Sub for Field {
    type Output = Result<Self, CrustyError>;

    fn sub(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::BigInt(a), Field::BigInt(b)) => Ok(Field::BigInt(a - b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                let res_scale = if s_l > s_r { s_l } else { s_r };
                let adjusted_a = a * 10i64.pow(res_scale - s_l);
                let adjusted_b = b * 10i64.pow(res_scale - s_r);
                Ok(Field::Decimal(adjusted_a - adjusted_b, res_scale))
            }
            (Field::BigInt(a), Field::Decimal(b, s_r)) => {
                let adjusted_a = a * 10i64.pow(s_r);
                Ok(Field::Decimal(adjusted_a - b, s_r))
            }
            (Field::Decimal(a, s_l), Field::BigInt(b)) => {
                let adjusted_b = b * 10i64.pow(s_l);
                Ok(Field::Decimal(a - adjusted_b, s_l))
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Mul for Field {
    type Output = Result<Self, CrustyError>;

    fn mul(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::BigInt(a), Field::BigInt(b)) => Ok(Field::BigInt(a * b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                // e.g. 123.456 * 2.34
                // 123.456 is stored as 123456 with a scale of 3
                // 2.34 is stored as 234 with a scale of 2
                // We will compute 123456 * 234 = 28846104.
                // We will divide 28846104 by the SMALLER scale 10^2, which gives 288461 with rounding.
                // This result, 288461, represents 288.461 when considering the scale 3.
                let larger_scale = s_l.max(s_r);
                let smaller_scale = s_l.min(s_r);
                let res = (a * b) as f64;
                let num = (res / 10i64.pow(smaller_scale) as f64).round() as i64;
                Ok(Field::Decimal(num, larger_scale))
            }
            (Field::BigInt(a), Field::Decimal(b, s_r)) => {
                // We remain the scale unchanged. We round the result to the nearest integer.
                // e.g. 123 * 2.34
                // 2.34 is stored as 234 with a scale of 2
                // We will compute 123 * 234 = 28782 and store it as 28782 with a scale of 2
                let res = a * b;
                Ok(Field::Decimal(res, s_r))
            }
            (Field::Decimal(a, s_l), Field::BigInt(b)) => {
                let res = a * b;
                Ok(Field::Decimal(res, s_l))
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Div for Field {
    type Output = Result<Self, CrustyError>;

    fn div(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::BigInt(a), Field::BigInt(b)) => {
                if b == 0 {
                    return Err(c_err("Division by zero"));
                }
                Ok(Field::BigInt(a / b))
            }
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                if b == 0 {
                    return Err(c_err("Division by zero"));
                }
                // We adjust to the larger scale
                // e.g. 123.456 / 2.34
                // 123.456 is stored as 123456 with a scale of 3
                // 2.34 is stored as 234 with a scale of 2
                // At the end, we want the division result to have scale of 3
                // Hence, we adjust 123456 to 12345600 by multiplying by the scale 10^2
                // Now, divide 12345600 by 234, which gives 52728 with rounding.
                // This result, 52728, represents 52.728 when considering the scale 3.

                // e.g. 123.45 / 2.345
                // 123.45 is stored as 12345 with a scale of 2
                // 2.345 is stored as 2345 with a scale of 3
                // At the end, we want the division result to have scale of 3
                // Hence, we adjust 12345 to 123450000 by multiplying by the scale 10^4
                // Now, divide 123450000 by 2345, which gives 52628 with rounding.
                // This result, 52628, represents 52.628 when considering the scale 3.

                // Hence, the following is true:
                // s_l + alpha - s_r = max(s_l, s_r)
                // where alpha is the number of digits you need to multiply the numerator by to get the adjusted numerator
                // alpha = max(s_l, s_r) - s_l + s_r
                // In the first example, alpha = 3 - 3 + 2 = 2
                // In the second example, alpha = 3 - 2 + 3 = 4

                let larger_scale = s_l.max(s_r);
                let alpha = larger_scale - s_l + s_r;
                let res = (a * 10i64.pow(alpha)) as f64;
                let num = (res / b as f64).round() as i64;
                Ok(Field::Decimal(num, larger_scale))
            }
            (Field::BigInt(a), Field::Decimal(b, s_r)) => {
                Field::Decimal(a, 0) / Field::Decimal(b, s_r)
            }
            (Field::Decimal(a, s_l), Field::BigInt(b)) => {
                Field::Decimal(a, s_l) / Field::Decimal(b, 0)
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Field {
    pub fn size(&self) -> usize {
        match self {
            Field::BigInt(_) => 8,
            Field::Int(_) => 4,
            Field::SmallInt(_) => 2,
            Field::Char(i, _) => *i as usize,
            Field::String(s) => s.len(),
            Field::Date(_) => 8,
            Field::Decimal(_, _) => 12,
            Field::Bool(_) => 1,
            Field::Null => 1,
        }
    }

    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::BigInt(x) => x.to_le_bytes().to_vec(),
            Field::Int(x) => x.to_le_bytes().to_vec(),
            Field::SmallInt(x) => x.to_le_bytes().to_vec(),
            Field::Char(i, s) => {
                // Each char is 4 bytes and 0000 is \0
                let mut bytes = vec![0; (*i as usize) * 4];
                if s.len() > *i as usize {
                    panic!("String is too long for char field");
                }
                bytes[..s.len()].copy_from_slice(s.as_bytes());
                bytes
            }
            Field::String(s) => {
                let s_len: u32 = s.len() as u32;
                let mut result = s_len.to_le_bytes().to_vec();
                result.extend(s.clone().into_bytes());
                result
            }
            Field::Date(x) => x.to_le_bytes().to_vec(),
            Field::Decimal(whole, scale) => {
                let mut bytes = whole.to_le_bytes().to_vec();
                bytes.extend(scale.to_le_bytes().to_vec());
                bytes
            }
            Field::Bool(b) => {
                if *b {
                    vec![1_u8]
                } else {
                    vec![0_u8]
                }
            }
            Field::Null => b"\0".to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8], dtype: &DataType) -> Result<Self, CrustyError> {
        match dtype {
            DataType::BigInt => {
                let value = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i64.".to_string())
                })?);
                Ok(Field::BigInt(value))
            }
            DataType::Int => {
                let value = i32::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i32.".to_string())
                })?);
                Ok(Field::Int(value))
            }
            DataType::SmallInt => {
                let value = i16::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i16.".to_string())
                })?);
                Ok(Field::SmallInt(value))
            }
            DataType::Char(i) => {
                let value = String::from_utf8(bytes[0..*i as usize].to_vec()).map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to string.".to_string())
                })?;
                Ok(Field::Char(*i, value))
            }
            DataType::String => {
                let s_len = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to get string length.".to_string())
                })?) as usize;
                let value = String::from_utf8(bytes[4..4 + s_len].to_vec()).map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to string.".to_string())
                })?;
                Ok(Field::String(value))
            }
            DataType::Date => {
                let value = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to u32.".to_string())
                })?);
                Ok(Field::Date(value))
            }
            DataType::Decimal(_, _) => {
                let whole = i64::from_le_bytes(bytes[0..8].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i64.".to_string())
                })?);
                let scale = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to u32.".to_string())
                })?);
                Ok(Field::Decimal(whole, scale))
            }
            DataType::Bool => {
                let value = bytes[0] == 1;
                Ok(Field::Bool(value))
            }
            DataType::Null => {
                if bytes[0] == 0 {
                    Ok(Field::Null)
                } else {
                    Err(CrustyError::CrustyError("Invalid null field".to_string()))
                }
            }
        }
    }

    pub fn unwrap_int_field(&self) -> i64 {
        match self {
            Field::BigInt(i) => *i,
            _ => panic!("Expected i64"),
        }
    }

    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::String(s) => s,
            _ => panic!("Expected String"),
        }
    }

    pub fn unwrap_bool_field(&self) -> bool {
        match self {
            Field::Bool(b) => *b,
            _ => panic!("Expected bool"),
        }
    }

    pub fn from_str(field: &str, attr: &Attribute) -> Result<Self, CrustyError> {
        if field == null_string() {
            return Field::from_str_to_null(field);
        }
        match &attr.dtype() {
            DataType::Int => {
                let i = Field::parse_int_from_str::<i32>(field)?;
                Ok(Field::Int(i))
            }
            DataType::BigInt => {
                let i = Field::parse_int_from_str::<i64>(field)?;
                Ok(Field::BigInt(i))
            }
            DataType::SmallInt => {
                let i = Field::parse_int_from_str::<i16>(field)?;
                Ok(Field::SmallInt(i))
            }
            DataType::Char(i) => Field::from_str_to_char(field, *i),
            DataType::String => Field::from_str_to_string(field),
            DataType::Decimal(p, s) => Field::from_str_to_decimal(field, *p, *s),
            DataType::Date => Field::from_str_to_date(field),
            DataType::Bool => Field::from_str_to_bool(field),
            DataType::Null => Field::from_str_to_null(field),
        }
    }

    pub fn parse_int_from_str<T>(input: &str) -> Result<T, CrustyError>
    where
        T: std::str::FromStr + std::fmt::Debug, // T must be convertible from a string and debug-printable
    {
        match input.parse::<T>() {
            Ok(i) => Ok(i),
            Err(_) => Err(CrustyError::ValidationError(format!(
                "Invalid int field {}",
                input
            ))),
        }
    }

    pub fn from_str_to_decimal(field: &str, p: u32, s: u32) -> Result<Self, CrustyError> {
        // Divide the field into integer and fractional parts
        let parts = field.split('.').collect::<Vec<&str>>();
        if parts.len() > 2 {
            return Err(CrustyError::ValidationError(format!(
                "Invalid decimal field {}",
                field
            )));
        }
        let integer = parts[0].parse::<i64>().map_err(|_| {
            CrustyError::ValidationError(format!("Invalid decimal field {}", field))
        })?;

        // Check the integer part
        if integer.abs() >= 10i64.pow(p - s) {
            return Err(CrustyError::ValidationError(format!(
                "Invalid decimal field precision {}. Expected {} found {}",
                field, p, s
            )));
        }
        // if scale is 2, then adjusted_fractional is 0.05 -> 5, 0.5 -> 50
        let adjusted_fractional = if parts.len() == 2 {
            let fractional = parts[1].parse::<i64>().map_err(|_| {
                CrustyError::ValidationError(format!("Invalid decimal field {}", field))
            })?;
            if fractional < 0 || fractional >= 10i64.pow(s) {
                return Err(CrustyError::ValidationError(format!(
                    "Invalid decimal field scale {}. Expected {} found {}",
                    field, p, s
                )));
            }
            fractional * 10i64.pow(s - parts[1].len() as u32)
        } else {
            0
        };
        if integer < 0 {
            Ok(Field::Decimal(
                integer * 10i64.pow(s) - adjusted_fractional,
                s,
            ))
        } else {
            Ok(Field::Decimal(
                integer * 10i64.pow(s) + adjusted_fractional,
                s,
            ))
        }
    }

    pub fn from_str_to_char(field: &str, length: u8) -> Result<Self, CrustyError> {
        if field.len() > length as usize {
            return Err(CrustyError::ValidationError(format!(
                "Invalid char field {}",
                field
            )));
        }
        Ok(Field::Char(length, field.to_string()))
    }

    pub fn from_str_to_string(field: &str) -> Result<Self, CrustyError> {
        let value: String = field.to_string().clone();
        Ok(Field::String(value))
    }

    pub fn from_str_to_date(field: &str) -> Result<Self, CrustyError> {
        let value = NaiveDate::parse_from_str(field, "%Y-%m-%d");
        if let Ok(date) = value {
            let days = date.signed_duration_since(base_date()).num_days();
            Ok(Field::Date(days))
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid date field {}",
                field
            )))
        }
    }

    pub fn from_str_to_bool(field: &str) -> Result<Self, CrustyError> {
        let value = field.parse::<bool>();
        if let Ok(value) = value {
            Ok(Field::Bool(value))
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid bool field {}",
                field
            )))
        }
    }

    pub fn from_str_to_null(field: &str) -> Result<Self, CrustyError> {
        if field == null_string() {
            Ok(Field::Null)
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid null field {}",
                field
            )))
        }
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Field::BigInt(i) => i.to_string(),
            Field::Int(i) => i.to_string(),
            Field::SmallInt(i) => i.to_string(),
            Field::Char(_i, s) => s.to_string(),
            Field::String(s) => s.to_string(),
            Field::Date(i) => {
                let date = base_date() + Duration::days(*i);
                date.format("%Y-%m-%d").to_string()
            }
            Field::Decimal(whole, scale) => {
                let s = whole.to_string();

                // Calculate the number of padding zeros required
                let padding = if s.len() <= *scale as usize {
                    *scale as usize + 1 - s.len() // +1 for the digit before the decimal point
                } else {
                    0
                };

                // Insert the padding zeros at the beginning
                let padded_s = format!("{:0>width$}", s, width = s.len() + padding);

                // Calculate the position to insert the decimal point
                let decimal_pos = padded_s.len() - *scale as usize;

                // Insert the decimal point
                let mut result = padded_s;
                result.insert(decimal_pos, '.');

                result
            }
            Field::Bool(b) => b.to_string(),
            Field::Null => null_string(),
        };
        write!(f, "{}", s)
    }
}

pub fn compare_fields(op: BinaryOp, left: &Field, right: &Field) -> bool {
    match op {
        BinaryOp::Eq => left == right,
        BinaryOp::Neq => left != right,
        BinaryOp::Gt => left > right,
        BinaryOp::Ge => left >= right,
        BinaryOp::Lt => left < right,
        BinaryOp::Le => left <= right,
        BinaryOp::And => left.and(right).unwrap_bool_field(),
        BinaryOp::Or => left.or(right).unwrap_bool_field(),
        _ => panic!("Unsupported comparison operation"),
    }
}
