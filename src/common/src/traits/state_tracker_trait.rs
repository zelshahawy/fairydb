use crate::prelude::*;

pub trait StateTrackerTrait {
    fn set_ts(&self, c_id: ContainerId, ts: LogicalTimeStamp);
    fn get_ts(&self, c_id: &ContainerId) -> (LogicalTimeStamp, LogicalTimeStamp);
}
