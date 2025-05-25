use super::OpIterator;
use common::{error::c_err, CrustyError, TableSchema, Tuple};
use serde::{Deserialize, Serialize};

/// Iterator over a Vec of tuples, mainly used for testing.
#[derive(Serialize, Deserialize)]
pub struct TupleIterator {
    // Parameters (No need to reset on close)
    /// Schema of the output.
    schema: TableSchema,
    /// Tuples to iterate over.
    tuples: Vec<Tuple>,
    will_rewind: bool,

    // States (Need to reset on close)
    open: bool,
    /// Current tuple in iteration.
    index: usize,
}

impl TupleIterator {
    /// Create a new tuple iterator over a set of results.
    ///
    /// # Arguments
    ///
    /// * `tuples` - Tuples to iterate over.
    /// * `schema` - Schema of the output results.
    pub fn new(tuples: Vec<Tuple>, schema: TableSchema) -> Self {
        // Set the tuples to reverse order so that we can pop them off
        // the end of the vector. Note that pop() is O(1) for Vec but
        // remove(0) is O(n)
        let tuples: Vec<Tuple> = tuples.into_iter().rev().collect();
        let index = tuples.len();
        Self {
            open: false,
            index,
            tuples,
            schema,
            will_rewind: true,
        }
    }

    pub fn from_json(mut json: serde_json::Value) -> Result<Self, CrustyError> {
        assert!(json["type"] == "TupleIterator");
        let open = serde_json::from_value(json["open"].take())
            .map_err(|e| c_err(&format!("Failed to deserialize open: {}", e)))?;
        let schema = serde_json::from_value(json["schema"].take())
            .map_err(|e| c_err(&format!("Failed to deserialize schema: {}", e)))?;
        let tuples = serde_json::from_value(json["tuples"].take())
            .map_err(|e| c_err(&format!("Failed to deserialize tuples: {}", e)))?;
        let index = serde_json::from_value(json["index"].take())
            .map_err(|e| c_err(&format!("Failed to deserialize index: {}", e)))?;
        let will_rewind = serde_json::from_value(json["will_rewind"].take())
            .map_err(|e| c_err(&format!("Failed to deserialize will_rewind: {}", e)))?;

        Ok(Self {
            open,
            index,
            tuples,
            schema,
            will_rewind,
        })
    }
}

impl OpIterator for TupleIterator {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
    }

    /// Opens the iterator without returning a tuple.
    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.open = true;
        }
        Ok(())
    }

    /// Retrieves the next tuple in the iterator.
    ///
    /// # Panics
    ///
    /// Panics if the TupleIterator has not been opened.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if self.will_rewind {
            // do not consume the iterator
            if self.index == 0 {
                Ok(None)
            } else {
                self.index -= 1;
                Ok(self.tuples.get(self.index).cloned())
            }
        } else {
            // consume the iterator
            Ok(self.tuples.pop())
        }
    }

    /// Closes the tuple iterator.
    fn close(&mut self) -> Result<(), CrustyError> {
        self.index = 0;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        if !self.will_rewind {
            panic!("Cannot rewind a TupleIterator with will_rewind set to false")
        }
        self.index = self.tuples.len();
        Ok(())
    }

    /// Returns the schema of the tuples.
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::OpIterator;
    use crate::testutil::{execute_iter, TestTuples};

    fn get_iter() -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let tuples = setup.tuples;
        let schema = setup.schema;
        let mut iter = Box::new(TupleIterator::new(tuples, schema));
        iter.configure(false);
        iter
    }

    fn run_tuple_iterator() -> Vec<Tuple> {
        let mut iter = get_iter();
        execute_iter(&mut *iter, false).unwrap()
    }

    mod tuple_iterator_test {
        use super::*;

        #[test]
        fn test_tuple_iterator() {
            let tuples = run_tuple_iterator();
            let expected = TestTuples::new("");
            for (t, e) in tuples.iter().zip(expected.tuples.iter()) {
                assert_eq!(t.field_vals, e.field_vals);
            }
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter();
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter();
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter();
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter();
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter();
            iter.configure(true);
            let t_before = execute_iter(&mut *iter, false).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, false).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
