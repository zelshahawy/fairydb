use serde::{Deserialize, Serialize};

use crate::{
    attribute::Attribute,
    datatypes::{default_decimal_precision, default_decimal_scale},
    DataType,
};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::Neq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::Le => write!(f, "<="),
            BinaryOp::Ge => write!(f, ">="),
            BinaryOp::And => write!(f, "&&"),
            BinaryOp::Or => write!(f, "||"),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl std::fmt::Display for AggOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use AggOp::*;
        match self {
            Avg => write!(f, "AVG"),
            Count => write!(f, "COUNT"),
            Max => write!(f, "MAX"),
            Min => write!(f, "MIN"),
            Sum => write!(f, "SUM"),
        }
    }
}

impl AggOp {
    pub fn to_attr(&self, src_att: &Attribute) -> Attribute {
        let new_name = format!("{}({})", self, src_att.name);
        match self {
            AggOp::Avg => {
                if matches!(&src_att.dtype, DataType::Decimal(_, _)) {
                    Attribute::new(new_name, src_att.dtype.clone())
                } else {
                    Attribute::new(
                        new_name,
                        DataType::Decimal(default_decimal_precision(), default_decimal_scale()),
                    )
                }
            }
            AggOp::Count => Attribute::new(new_name, DataType::BigInt),
            _ => Attribute::new(new_name, src_att.dtype.clone()),
        }
    }
}
