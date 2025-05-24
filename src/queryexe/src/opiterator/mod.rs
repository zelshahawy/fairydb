pub use self::aggregate::Aggregate;
pub use self::cross_join::CrossJoin;
pub use self::filter::Filter;
pub use self::hash_join::HashEqJoin;
pub use self::nested_loop_join::NestedLoopJoin;
pub use self::project::Project;
pub use self::seqscan::SeqScan;
pub use self::sort::Sort;
pub use self::sort_merge_join::SortMergeJoin;
pub use self::tuple_iterator::TupleIterator;
pub use self::update::Update;
use common::{CrustyError, TableSchema, Tuple};

mod aggregate;
mod cross_join;
mod filter;
mod hash_join;
mod nested_loop_join;
mod project;
mod seqscan;
mod sort;
mod sort_merge_join;
mod tuple_iterator;
mod update;

pub trait OpIterator {
    /// conifgure the opiterator
    ///
    /// will_rewind indicates whether we will rewind the operator in the future
    /// This matters because it determines whether the operator can consume
    /// the opiterator state when calling the chain of next() calls.
    /// If will_rewind is false, then
    /// the next() can consume the state of the opiterator. If will_rewind is
    /// true, then the opiterator will be rewound in the future and the
    /// next() cannot consume the state of the opiterator.
    ///
    /// For example, if the plan is:
    ///       (CrossJoin)
    ///      /        \
    ///   (...)       (...)
    /// then, the CrossJoin requires right child to have full state because
    /// the CrossJoin will rewind the right child when next() is called.
    /// In cases where the plan does not contain CrossJoin or NestedLoopJoin,
    /// will_rewind is false for operator's children because the operator
    /// does not rewind its children.
    fn configure(&mut self, will_rewind: bool);

    /// Opens the iterator. This must be called before any of the other methods.
    ///
    /// This initializes the states of the operator.
    /// For example, if the operator is a hash-join, open should create the
    /// hash table used for the join.
    /// If the operator is already open, this function should do nothing.
    /// Therefore a typical implementation of open would be:
    /// ```
    /// fn open(&mut self) -> Result<(), CrustyError> {
    ///    if self.open {
    ///       // initialize the states
    ///       self.open = true;
    ///   }
    ///   Ok(())
    /// }
    /// ```
    fn open(&mut self) -> Result<(), CrustyError>;

    /// Advances the iterator and returns the next tuple from the operator.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError>;

    /// Resets the states of the operator.
    ///
    /// Only operations that can be performed after close is open() and close().
    /// The call to close() should be idempotent.
    /// close() must reset the states of the operator. For example, if the
    /// operator is a hash-join, close should clean up the hash table.
    /// This implies that a call to open() after close() will re-initialize the operator.
    /// However, close() should not reset or modify the operator parameters in order
    /// to allow the operator to be re-opened. For example, if the operator is a
    /// hash-join, close should not change the join predicates because join predicates
    /// are needed when the operator is re-opened.
    fn close(&mut self) -> Result<(), CrustyError>;

    /// Returns the iterator to the start.
    ///
    /// # Panics
    ///
    /// Panics if iterator is not open.
    ///
    /// rewind() should be called after open(). This guarantees that the states
    /// of the operator are initialized (e.g., hash table is created)
    ///
    /// A typical implementation of rewind for a stateless operator would be:
    /// ```ignore
    /// fn rewind(&mut self) -> Result<(), CrustyError> {
    ///     if !self.open {
    ///         panic!("Operator has not been opened")
    ///     }
    ///     self.child.rewind()?;
    ///     Ok(())
    /// }
    /// ```
    /// For a stateful operator, rewind should NOT clean up the states.
    /// For example, if the operator is a hash-join, rewind should not clean up
    /// the hash table. rewind only need to guarantee that call to next()
    /// immediately after rewind() should return the first tuple in the result.
    fn rewind(&mut self) -> Result<(), CrustyError>;

    /// Returns the schema associated with this OpIterator.
    fn get_schema(&self) -> &TableSchema;
}

pub struct DummyOpIterator {}

impl DummyOpIterator {
    pub fn new() -> Self {
        DummyOpIterator {}
    }
}

impl OpIterator for DummyOpIterator {
    fn configure(&mut self, _will_rewind: bool) {
        // do nothing
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        unimplemented!()
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        unimplemented!()
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        unimplemented!()
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        unimplemented!()
    }

    fn get_schema(&self) -> &TableSchema {
        unimplemented!()
    }
}

impl Default for DummyOpIterator {
    fn default() -> Self {
        Self::new()
    }
}
