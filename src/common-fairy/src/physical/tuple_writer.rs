use crate::{CrustyError, TableSchema, Tuple};

pub trait TupleConverterTrait {
    //// Creates a new TupleConverterTrait instance with the given schema.
    fn new(schema: TableSchema) -> Self;

    /// Converts a tuple to bytes and writes it to the buffer at the specified offset.
    /// Returns the number of bytes written.
    /// Returns None if the buffer is not large enough.
    fn write_tuple(&self, tuple: &Tuple, buf: &mut [u8], offset: usize) -> Option<usize>;

    /// Reads a tuple from the buffer at the specified offset and length.
    /// Returns a Result with the tuple if successful, or an error if not.
    /// The length of the tuple's bytes is specified by the len parameter.
    fn read_tuple(&self, buf: &[u8], offset: usize, len: usize) -> Result<Tuple, CrustyError>;
}
