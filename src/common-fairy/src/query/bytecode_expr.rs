use crate::{tuple::Tuple, CrustyError, Field};
use std::ops::{Add, Div, Mul, Sub};

pub trait FromBool {
    fn from_bool(b: bool) -> Self;
}

pub trait And {
    fn and(&self, other: &Self) -> Self;
}

pub trait Or {
    fn or(&self, other: &Self) -> Self;
}

pub enum ByteCodes {
    // CONTROL FLOW
    PushLit,
    PushField,
    // MATH OPERATIONS
    Add,
    Sub,
    Mul,
    Div,
    // COMPARISON OPERATIONS
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // LOGICAL OPERATIONS
    And,
    Or,
}

const STATIC_DISPATCHER: [DispatchFn<Field>; 14] = [
    // CONTROL FLOW
    PUSH_LIT_FN,
    PUSH_FIELD_FN,
    // MATH OPERATIONS
    ADD_FN,
    SUB_FN,
    MUL_FN,
    DIV_FN,
    // COMPARISON OPERATIONS
    EQ_FN,
    NEQ_FN,
    LT_FN,
    GT_FN,
    LTE_FN,
    GTE_FN,
    // LOGICAL OPERATIONS
    AND_FN,
    OR_FN,
];

// Utility functions
pub fn colidx_expr(colidx: usize) -> ByteCodeExpr {
    let mut expr = ByteCodeExpr::new();
    expr.add_code(ByteCodes::PushField as usize);
    expr.add_code(colidx);
    expr
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ByteCodeExpr {
    pub bytecodes: Vec<usize>,
    pub literals: Vec<Field>,
}

impl ByteCodeExpr {
    pub fn new() -> Self {
        ByteCodeExpr {
            bytecodes: Vec::new(),
            literals: Vec::new(),
        }
    }

    pub fn add_code(&mut self, code: usize) {
        self.bytecodes.push(code);
    }

    pub fn add_literal(&mut self, literal: Field) -> usize {
        let i = self.literals.len();
        self.literals.push(literal);
        i
    }

    fn is_empty(&self) -> bool {
        self.bytecodes.is_empty()
    }

    pub fn eval(&self, record: &Tuple) -> Field {
        if self.is_empty() {
            panic!("Cannot evaluate empty expression")
        }
        let mut stack = Vec::new();
        let mut i = 0;
        let record = &record.field_vals;
        let bytecodes = &self.bytecodes;
        let literals = &self.literals;
        while i < bytecodes.len() {
            let opcode = bytecodes[i];
            i += 1;
            STATIC_DISPATCHER[opcode](bytecodes, &mut i, &mut stack, literals, record);
        }
        stack.pop().unwrap()
    }
}

type DispatchFn<T> = fn(&[usize], &mut usize, &mut Vec<T>, &[T], &[T]);
const PUSH_LIT_FN: DispatchFn<Field> = push_lit;
const PUSH_FIELD_FN: DispatchFn<Field> = push_field;
const ADD_FN: DispatchFn<Field> = add;
const SUB_FN: DispatchFn<Field> = sub;
const MUL_FN: DispatchFn<Field> = mul;
const DIV_FN: DispatchFn<Field> = div;
const EQ_FN: DispatchFn<Field> = eq;
const NEQ_FN: DispatchFn<Field> = neq;
const LT_FN: DispatchFn<Field> = lt;
const GT_FN: DispatchFn<Field> = gt;
const LTE_FN: DispatchFn<Field> = lte;
const GTE_FN: DispatchFn<Field> = gte;
const AND_FN: DispatchFn<Field> = and;
const OR_FN: DispatchFn<Field> = or;

fn push_field<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    _literals: &[T],
    record: &[T],
) where
    T: Clone,
{
    stack.push(record[bytecodes[*i]].clone());
    *i += 1;
}

fn push_lit<T>(
    bytecodes: &[usize],
    i: &mut usize,
    stack: &mut Vec<T>,
    literals: &[T],
    _record: &[T],
) where
    T: Clone,
{
    stack.push(literals[bytecodes[*i]].clone());
    *i += 1;
}

fn add<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: Add<Output = Result<T, CrustyError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l + r).unwrap());
}

fn sub<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: Sub<Output = Result<T, CrustyError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l - r).unwrap());
}

fn mul<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: Mul<Output = Result<T, CrustyError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l * r).unwrap());
}

fn div<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: Div<Output = Result<T, CrustyError>> + Clone,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push((l / r).unwrap());
}

fn eq<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialEq + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l == r));
}

fn neq<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialEq + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l != r));
}

fn lt<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l < r));
}

fn gt<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l > r));
}

fn lte<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l <= r));
}

fn gte<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialOrd + Clone + FromBool,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(T::from_bool(l >= r));
}

fn and<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialEq + Clone + And,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.and(&r));
}

fn or<T>(_bytecodes: &[usize], _i: &mut usize, stack: &mut Vec<T>, _literals: &[T], _record: &[T])
where
    T: PartialEq + Clone + Or,
{
    let r = stack.pop().unwrap();
    let l = stack.pop().unwrap();
    stack.push(l.or(&r));
}
