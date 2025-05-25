use crate::prelude::*;

pub trait IndexTrait {
    /// get a list of values that are equal to a field
    fn equality_get_value_ids(field: Field) -> Vec<ValueId>;
    /// Option set to none means min or max. inclusive set to false means exclusive. Calling on non-range idx will give empty vec
    fn range_get_value_ids(
        min_field: Option<Field>,
        max_field: Option<Field>,
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Vec<ValueId>;

    fn add_values(value_ids: Vec<ValueId>, fields: Vec<Field>) -> Result<(), CrustyError>;
}
