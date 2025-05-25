use crate::prelude::*;
use crate::table::TableInfo;
use crate::{attribute::Attribute, table::TableSchema, tuple::Tuple, DataType, Field};
use itertools::izip;
use rand::distr::{Alphanumeric, Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::seq::IndexedRandom;
use rand::{rng, Rng, SeedableRng};
use std::env;

pub fn get_rng() -> SmallRng {
    match env::var("CRUSTY_SEED") {
        Ok(seed_str) => match seed_str.parse::<u64>() {
            Ok(seed) => {
                log::debug!("Using seed from CRUSTY_SEED: {}", seed);
                SmallRng::seed_from_u64(seed)
            }
            Err(_) => {
                let seed = rng().random::<u64>();
                log::debug!("Failed to parse CRUSTY_SEED, using random seed: {}", seed);
                SmallRng::seed_from_u64(seed)
            }
        },
        Err(_) => {
            let seed = rng().random::<u64>();
            log::debug!("No CRUSTY_SEED provided, using random seed: {}", seed);
            SmallRng::seed_from_u64(seed)
        }
    }
}

pub fn init() {
    // To change the log level for tests change the filter_level
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        //.filter_level(log::LevelFilter::Info)
        .try_init();
}

pub fn gen_random_tuples(
    rng: &mut SmallRng,
    n: usize,
    attributes: &Vec<Attribute>,
    null_prob: f64,
) -> Vec<Tuple> {
    let mut tuples = Vec::new();
    for _ in 0..n {
        let mut tuple_data = Vec::new();
        for attr in attributes {
            if rng.random::<f64>() < null_prob {
                tuple_data.push(Field::Null);
            } else {
                match &attr.dtype {
                    DataType::BigInt => tuple_data.push(Field::BigInt(rng.random::<i64>())),
                    DataType::SmallInt => tuple_data.push(Field::SmallInt(rng.random::<i16>())),
                    DataType::Char(size) => {
                        tuple_data.push(Field::String(gen_rand_string(rng, *size as usize)))
                    }
                    DataType::String => {
                        let size = rng.random_range(2..20);
                        tuple_data.push(Field::String(gen_rand_string(rng, size)))
                    }
                    _ => panic!("Unsupported data type"),
                }
            }
        }
        tuples.push(Tuple::new(tuple_data));
    }
    tuples
}

pub fn gen_random_tuples_fixed(
    rng: &mut SmallRng,
    n: usize,
    attrs_excluding_pk: usize,
) -> (TableSchema, Vec<Tuple>) {
    let mut attributes: Vec<Attribute> = Vec::new();
    attributes.push(Attribute {
        name: String::from("id"),
        dtype: DataType::BigInt,
        constraint: crate::Constraint::PrimaryKey,
    });

    let fixed_dtypes = [
        DataType::BigInt,
        DataType::Char(rng.random_range(2..11)),
        DataType::SmallInt,
    ];
    for i in 0..attrs_excluding_pk {
        let attr = Attribute {
            name: format!("ia{}", i),
            dtype: fixed_dtypes.choose(rng).unwrap().clone(),
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    let table_schema = TableSchema::new(attributes);
    let tuples = gen_random_tuples(rng, n, &table_schema.attributes, 0.0);
    (table_schema, tuples)
}

pub fn gen_random_tuples_var_and_null(
    rng: &mut SmallRng,
    n: usize,
    attrs_excluding_pk: usize,
    null_prob: f64,
) -> (TableSchema, Vec<Tuple>) {
    let mut attributes: Vec<Attribute> = Vec::new();
    attributes.push(Attribute {
        name: String::from("id"),
        dtype: DataType::BigInt,
        constraint: crate::Constraint::PrimaryKey,
    });

    let fixed_dtypes = [
        DataType::BigInt,
        DataType::Char(rng.random_range(2..11)),
        DataType::SmallInt,
        DataType::String,
    ];
    for i in 0..attrs_excluding_pk {
        let attr = Attribute {
            name: format!("ia{}", i),
            dtype: fixed_dtypes.choose(rng).unwrap().clone(),
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    let table_schema = TableSchema::new(attributes);
    let tuples = gen_random_tuples(rng, n, &table_schema.attributes, null_prob);
    (table_schema, tuples)
}

pub fn gen_uniform_strings(
    rng: &mut SmallRng,
    n: u64,
    cardinality: Option<u64>,
    min: usize,
    max: usize,
) -> Vec<Field> {
    let mut ret: Vec<Field> = Vec::new();
    if let Some(card) = cardinality {
        let values: Vec<Field> = (0..card)
            .map(|_| Field::String(gen_rand_string_range(rng, min, max)))
            .collect();
        assert_eq!(card as usize, values.len());
        //ret = values.iter().choose_multiple(&mut rng, n as usize).collect();
        let uniform = Uniform::new(0, values.len()).unwrap();
        for _ in 0..n {
            let idx = uniform.sample(rng);
            assert!(idx < card as usize);
            ret.push(values[idx].clone())
        }
        //ret = rng.sample(values, n);
    } else {
        for _ in 0..n {
            ret.push(Field::String(gen_rand_string_range(rng, min, max)))
        }
    }
    ret
}

pub fn gen_uniform_ints(rng: &mut SmallRng, n: u64, cardinality: Option<u64>) -> Vec<Field> {
    let mut ret = Vec::new();
    if let Some(card) = cardinality {
        if card > i32::MAX as u64 {
            panic!("Cardinality larger than i32 max")
        }
        if n == card {
            // all values distinct
            if n < i32::MAX as u64 / 2 {
                for i in 0..card as i64 {
                    ret.push(Field::BigInt(i));
                }
            } else {
                for i in i64::MIN..i64::MIN + (card as i64) {
                    ret.push(Field::BigInt(i));
                }
            }
            //ret.shuffle(&mut rng);
        } else {
            let mut range = Uniform::new_inclusive(i64::MIN, i64::MIN + (card as i64) - 1).unwrap();
            if card < (i32::MAX / 2) as u64 {
                range = Uniform::new_inclusive(0, card as i64 - 1).unwrap();
            }
            for _ in 0..n {
                ret.push(Field::BigInt(range.sample(rng)));
            }
        }
    } else {
        for _ in 0..n {
            ret.push(Field::BigInt(rng.random::<i64>()));
        }
    }
    ret
}

pub fn gen_test_tuples(rng: &mut SmallRng, n: u64) -> Vec<Tuple> {
    let keys = gen_uniform_ints(rng, n, Some(n));
    let i1 = gen_uniform_ints(rng, n, Some(10));
    let i2 = gen_uniform_ints(rng, n, Some(100));
    let i3 = gen_uniform_ints(rng, n, Some(1000));
    let i4 = gen_uniform_ints(rng, n, Some(10000));
    let s1 = gen_uniform_strings(rng, n, Some(10), 10, 20);
    let s2 = gen_uniform_strings(rng, n, Some(100), 10, 20);
    let s3 = gen_uniform_strings(rng, n, Some(1000), 10, 20);
    let s4 = gen_uniform_strings(rng, n, Some(10000), 10, 30);
    let mut tuples = Vec::new();
    for (k, a, b, c, d, e, f, g, h) in izip!(keys, i1, i2, i3, i4, s1, s2, s3, s4) {
        let vals: Vec<Field> = vec![k, a, b, c, d, e, f, g, h];
        tuples.push(Tuple::new(vals));
    }
    tuples
}

pub fn gen_test_table_and_tuples(
    rng: &mut SmallRng,
    c_id: ContainerId,
    n: u64,
) -> (TableInfo, Vec<Tuple>) {
    let table_name = format!("test_table_{}", c_id);
    let table = gen_table_for_test_tuples(c_id, table_name);
    let tuples = gen_test_tuples(rng, n);
    (table, tuples)
}

/// Generates a table with the given name and schema to match the
/// the gen_test_tuples.
pub fn gen_table_for_test_tuples(c_id: ContainerId, table_name: String) -> TableInfo {
    let mut attributes: Vec<Attribute> = Vec::new();
    let pk_attr = Attribute {
        name: String::from("id"),
        dtype: DataType::BigInt,
        constraint: crate::Constraint::PrimaryKey,
    };
    attributes.push(pk_attr);

    for n in 1..5 {
        let attr = Attribute {
            name: format!("ia{}", n),
            dtype: DataType::BigInt,
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    for n in 1..5 {
        let attr = Attribute {
            name: format!("sa{}", n),
            dtype: DataType::String,
            constraint: crate::Constraint::None,
        };
        attributes.push(attr);
    }
    let table_schema = TableSchema::new(attributes);

    TableInfo::new(c_id, table_name, table_schema)
}

/// Converts an int vector to a Tuple.
///
/// # Argument
///
/// * `data` - Data to put into tuple.
pub fn int_vec_to_tuple(data: Vec<i64>) -> Tuple {
    let mut tuple_data = Vec::new();

    for val in data {
        tuple_data.push(Field::BigInt(val));
    }

    Tuple::new(tuple_data)
}

/// Creates a Vec of tuples containing Ints given a 2D Vec of i32 's
pub fn create_tuple_list(tuple_data: Vec<Vec<i64>>) -> Vec<Tuple> {
    let mut tuples = Vec::new();
    for item in tuple_data.iter() {
        let fields = item.iter().map(|i| Field::BigInt(*i)).collect();
        tuples.push(Tuple::new(fields));
    }
    tuples
}

/// Creates a new table schema for a table with width number of Ints.
pub fn get_int_table_schema(width: usize) -> TableSchema {
    let mut attrs = Vec::new();
    for _ in 0..width {
        attrs.push(Attribute::new(String::new(), DataType::BigInt))
    }
    TableSchema::new(attrs)
}

pub fn get_random_byte_vec(rng: &mut SmallRng, n: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
    random_bytes
}

pub fn gen_rand_string_range(rng: &mut SmallRng, min: usize, max: usize) -> String {
    if min >= max {
        return gen_rand_string(rng, min);
    }
    let size = rng.random_range(min..max);
    rng.sample_iter(Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}

pub fn gen_rand_string(rng: &mut SmallRng, n: usize) -> String {
    rng.sample_iter(Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

pub fn gen_random_int<T>(rng: &mut SmallRng, min: T, max: T) -> T
where
    T: rand::distr::uniform::SampleUniform,
{
    // let mut rng = rng();
    rng.sample(rand::distr::uniform::Uniform::new_inclusive(min, max).unwrap())
}

#[allow(dead_code)]
pub fn get_random_vec_of_byte_vec(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| rng.random::<u8>()).collect());
    }
    res
}

#[allow(dead_code)]
/// get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
/// the value of u8 in Vec<u8> is ascending from 1 to 16 (0x10) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_0x(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        elements += 1;
        if elements >= 16 {
            elements = 1;
        }
    }
    res
}

#[allow(dead_code)]
/// get_ascending_vec_of_byte_vec_0x: this function will create Vec<Vec<u8>>
/// the value of u8 in Vec<u8> is ascending from 1 to 255 (0x100) for each Vec<u8>
pub fn get_ascending_vec_of_byte_vec_02x(
    rng: &mut SmallRng,
    n: usize,
    min_size: usize,
    max_size: usize,
) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    assert!(max_size >= min_size);
    let size_diff = max_size - min_size;
    let mut elements = 1;
    for _ in 0..n {
        let size = if size_diff == 0 {
            min_size
        } else {
            rng.random_range(min_size..size_diff + min_size)
        };
        res.push((0..size).map(|_| elements).collect());
        if elements == 255 {
            elements = 1;
        } else {
            elements += 1;
        }
    }
    res
}

#[allow(dead_code)]
pub fn compare_unordered_byte_vecs(a: &[Vec<u8>], mut b: Vec<Vec<u8>>) -> bool {
    // Quick check
    if a.len() != b.len() {
        trace!("Vecs are different lengths");
        return false;
    }
    // check if they are the same ordered
    let non_match_count = a
        .iter()
        .zip(b.iter())
        .filter(|&(j, k)| j[..] != k[..])
        .count();
    if non_match_count == 0 {
        return true;
    }

    // Now check if they are out of order
    for x in a {
        let pos = b.iter().position(|y| y[..] == x[..]);
        match pos {
            None => {
                //Was not found, not equal
                trace!("Was not able to find value for {:?}", x);
                return false;
            }
            Some(idx) => {
                b.swap_remove(idx);
            }
        }
    }
    if !b.is_empty() {
        trace!("Values in B that did not match a {:?}", b);
    }
    //since they are the same size, b should be empty
    b.is_empty()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_random_vec_bytes() {
        let n = 10_000;
        let mut min = 50;
        let mut max = 75;
        let mut rng = get_rng();
        let mut data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 134;
        max = 134;
        data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(x.len() == min && x.len() == max);
        }

        min = 0;
        max = 14;
        data = get_random_vec_of_byte_vec(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }

    #[test]
    fn test_ascd_random_vec_bytes() {
        let mut rng = get_rng();
        let n = 10000;
        let mut min = 50;
        let mut max = 75;
        let mut data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() < min || x.len() >= max {
                println!("!!!{:?}", x);
            }
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }

        min = 13;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            if x.len() != min || x.len() != max {
                println!("!!!{:?}", x);
                println!("!!!x.len(){:?}", x.len());
                println!("111{}", x.len() == min && x.len() == max);
            }
            assert!(x.len() == min && x.len() == max - 1);
        }

        min = 0;
        max = 14;
        data = get_ascending_vec_of_byte_vec_02x(&mut rng, n, min, max);
        assert_eq!(n, data.len());
        for x in data {
            assert!(
                x.len() >= min && x.len() < max,
                "x's len {} was not withing [{},{}]",
                x.len(),
                min,
                max
            );
        }
    }
}
