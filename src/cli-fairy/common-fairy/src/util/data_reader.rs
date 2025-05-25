use crate::table::TableSchema;
use crate::CrustyError;
use crate::{tuple::Tuple, Field};
use csv::{Reader, StringRecord};
use std::io::Read;
use std::result::Result;

pub trait DataReader {
    /// Read and return the next record from the data source.
    /// This function returns `None` when there are no more records to read.
    fn read_next(&mut self) -> Result<Option<Tuple>, CrustyError>;
}

pub struct CsvReader<R: Read> {
    schema: TableSchema,
    rdr: Reader<R>,
}

impl<R: Read> CsvReader<R> {
    pub fn new(
        reader: R,
        schema: &TableSchema,
        delimiter: u8,
        has_header: bool,
    ) -> Result<Self, CrustyError> {
        let rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .from_reader(reader);

        Ok(CsvReader {
            schema: schema.clone(),
            rdr,
        })
    }
}

impl<R: Read> DataReader for CsvReader<R> {
    fn read_next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        match self.rdr.records().next() {
            Some(Ok(record)) => {
                let tuple = convert_to_tuple(&record, &self.schema)?;
                Ok(Some(tuple))
            }
            Some(Err(e)) => Err(CrustyError::IOError(e.to_string())),
            None => Ok(None),
        }
    }
}

// Helper function to convert a CSV record to a Tuple
fn convert_to_tuple(record: &StringRecord, schema: &TableSchema) -> Result<Tuple, CrustyError> {
    let mut fields = Vec::new();
    for (str_field, attr) in record.iter().zip(schema.attributes()) {
        fields.push(Field::from_str(str_field, attr)?)
    }
    Ok(Tuple::new(fields))
}
