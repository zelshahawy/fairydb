use crate::{CrustyError, TableSchema, Tuple};

use super::tuple_writer::TupleConverterTrait;

pub struct TupleConverter2 {
    _table_schema: TableSchema,
}

impl TupleConverterTrait for TupleConverter2 {
    fn new(schema: TableSchema) -> Self {
        TupleConverter2 {
            _table_schema: schema,
        }
    }

    fn write_tuple(&self, tuple: &Tuple, buf: &mut [u8], offset: usize) -> Option<usize> {
        let bytes = tuple.to_bytes();
        if bytes.len() > buf.len() - offset {
            return None;
        }
        buf[offset..offset + bytes.len()].copy_from_slice(&bytes);
        Some(bytes.len())
    }

    fn read_tuple(&self, buf: &[u8], offset: usize, len: usize) -> Result<Tuple, CrustyError> {
        let t = Tuple::from_bytes(buf[offset..offset + len].as_ref());
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::init;

    use super::*;
    use crate::testutil::{gen_random_tuples_fixed, gen_random_tuples_var_and_null, get_rng};

    const BUF_SIZE: usize = 100_000;
    const N: usize = 100;
    const NULL_PROB: f64 = 0.1;

    #[test]
    fn test_conv2() {
        init();
        let mut rng = get_rng();
        let attrs = 10;
        let (schema, tuples) = gen_random_tuples_fixed(&mut rng, N, attrs - 1);
        let mut buf = [0u8; BUF_SIZE];
        let tuple_conv0 = TupleConverter2::new(schema.clone());
        for t in tuples.iter() {
            let len = tuple_conv0.write_tuple(t, &mut buf, 0).unwrap();
            let tuple2 = tuple_conv0.read_tuple(&buf, 0, len).unwrap();
            assert_eq!(t, &tuple2);
        }
        let mut offset = 0;
        for t in tuples.iter() {
            let len = tuple_conv0.write_tuple(t, &mut buf, offset).unwrap();
            let tuple2 = tuple_conv0.read_tuple(&buf, offset, len).unwrap();
            assert_eq!(t, &tuple2);
            offset += len;
        }
    }

    #[test]
    fn test_var_conv2() {
        init();
        let mut rng = get_rng();
        let attrs = 10;
        let (schema, tuples) = gen_random_tuples_var_and_null(&mut rng, N, attrs - 1, NULL_PROB);
        let mut buf = [0u8; BUF_SIZE];
        let tuple_conv2 = TupleConverter2::new(schema.clone());
        for t in tuples.iter() {
            let len = tuple_conv2.write_tuple(t, &mut buf, 0).unwrap();
            let tuple2 = tuple_conv2.read_tuple(&buf, 0, len).unwrap();
            assert_eq!(t, &tuple2);
        }
        let mut offset = 0;
        for t in tuples.iter() {
            let len = tuple_conv2.write_tuple(t, &mut buf, offset).unwrap();
            let tuple2 = tuple_conv2.read_tuple(&buf, offset, len).unwrap();
            assert_eq!(t, &tuple2);
            offset += len;
        }
    }
}
