use std::fmt;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

pub type TidType = u64;
pub type AtomicTidType = AtomicU64;
static TXN_COUNTER: AtomicTidType = AtomicTidType::new(1);

/// A logical timestamp for a record or data. Same a transaction id.
pub type LogicalTimeStamp = TidType;
/// This is used for concurrency control and recovery
pub type AtomicTimeStamp = AtomicTidType;

/// The Id type for a table, index, or other data structure
pub type ContainerId = u16;
/// The concurrency safe container id
pub type AtomicContainerId = AtomicU16;

/// The Id type for a segment or partition
pub type SegmentId = u8;

/// The Id type for a page
pub type PageId = u32;
/// The concurrency safe page id
pub type AtomicPageId = AtomicU32;

/// The slotted id for a record or value within a page
pub type SlotId = u16;

/// The Id type for a column
pub type ColumnId = usize;

/// The Id type for a group, used in the query optimizer.
pub type GroupId = usize;

/// Permissions for locks. Shared is ReadOnly and Exclusive is ReadWrite.
#[derive(PartialEq, Clone, Copy)]
pub enum Permissions {
    ReadOnly,
    ReadWrite,
}

/// Implementation of transaction id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    /// Id of transaction.
    id: TidType,
}

impl TransactionId {
    /// Creates a new transaction id.
    pub fn new() -> Self {
        Self {
            id: TXN_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn system() -> Self {
        Self { id: 0 }
    }

    /// Returns the transaction id.
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        TransactionId::new()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[allow(dead_code)]
/// The things that can be saved and maintained in the database
pub enum StateType {
    HashTable,
    BaseTable,
    MatView,
    Tree,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
/// The metadata for a state. This is used to track the state of a container.
pub struct StateMeta {
    /// The type of state being stored
    pub state_type: StateType,
    /// The ID for storing this container
    pub id: ContainerId,
    /// An optional name
    pub name: Option<String>,
    /// The last time this was updated if at all
    pub last_update: Option<LogicalTimeStamp>,
    /// Containers needed for the query plan to update this state
    pub dependencies: Option<Vec<ContainerId>>,
}

/// Holds information to find a record or value's bytes in a storage manager.
/// Depending on storage manager (SM), various elements may be used.
/// For example a disk-based SM may use pages to store the records, where
/// a main-memory based storage manager may not.
/// It is up to a particular SM to determine how and when to use
#[derive(PartialEq, Clone, Copy, Eq, Hash, Serialize, Deserialize)]
pub struct ValueId {
    /// The source of the value. This could represent a table, index, or other data structure.
    /// All values stored must be associated with a container that is created by the storage manager.
    pub container_id: ContainerId,
    /// An optional segment or partition ID
    pub segment_id: Option<SegmentId>,
    /// An optional page id
    pub page_id: Option<PageId>,
    /// An optional slot id. This could represent a physical or logical ID.
    pub slot_id: Option<SlotId>,
}

pub type VidBytes = [u8; 10];
const PID_SIZE: usize = std::mem::size_of::<PageId>();
const SID_SIZE: usize = std::mem::size_of::<SlotId>();

impl ValueId {
    pub fn new(container_id: ContainerId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: None,
        }
    }

    pub fn new_page(container_id: ContainerId, page_id: PageId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    pub fn new_slot(container_id: ContainerId, page_id: PageId, slot_id: SlotId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: Some(slot_id),
        }
    }

    pub fn to_fixed_bytes(&self) -> VidBytes {
        let mut vb = [0; 10];

        let mut bit_flag = 0b00001000;
        vb[1..3].copy_from_slice(&self.container_id.to_le_bytes());
        let mut offset = 3;
        if self.segment_id.is_some() {
            // bit_flag |= 0b00000100;
            panic!("TODO no segment supported");
        }
        if self.page_id.is_some() {
            bit_flag |= 0b00000010;
            offset += PID_SIZE;
            vb[offset - PID_SIZE..offset].copy_from_slice(&self.page_id.unwrap().to_le_bytes());
        }
        if self.slot_id.is_some() {
            bit_flag |= 0b00000001;
            offset += SID_SIZE;
            vb[offset - SID_SIZE..offset].copy_from_slice(&self.slot_id.unwrap().to_le_bytes());
        }
        vb[0] = bit_flag;
        vb
    }

    /// Utility to convert data into ValueID
    pub fn from_bytes(data: &[u8]) -> Self {
        let bit_flag = data[0];
        let container_id = ContainerId::from_le_bytes(data[1..3].try_into().unwrap());
        let mut offset = 3;
        let segment_id = if bit_flag & 0b00000100 != 0 {
            offset += 1;
            Some(SegmentId::from_le_bytes(
                data[offset - 1..offset].try_into().unwrap(),
            ))
        } else {
            None
        };
        let page_id = if bit_flag & 0b00000010 != 0 {
            offset += PID_SIZE;
            Some(PageId::from_le_bytes(
                data[offset - PID_SIZE..offset].try_into().unwrap(),
            ))
        } else {
            None
        };
        let slot_id = if bit_flag & 0b00000001 != 0 {
            offset += SID_SIZE;
            Some(SlotId::from_le_bytes(
                data[offset - SID_SIZE..offset].try_into().unwrap(),
            ))
        } else {
            None
        };
        ValueId {
            container_id,
            segment_id,
            page_id,
            slot_id,
        }
    }

    pub const CP_BYTES: usize =
        std::mem::size_of::<ContainerId>() + std::mem::size_of::<PageId>() + 1;
    /// Utility to convert ValueID into data/bytes for Container and Value only.
    /// Warning no segment
    pub fn to_cp_bytes(&self) -> [u8; Self::CP_BYTES] {
        let mut bytes = [0; Self::CP_BYTES];
        bytes[0] = 0b00001000;
        if self.page_id.is_some() {
            bytes[0] |= 0b00000010;
        }
        bytes[1..std::mem::size_of::<ContainerId>() + 1]
            .copy_from_slice(&self.container_id.to_le_bytes());
        let page_id = self.page_id.unwrap_or(PageId::MIN);
        bytes[std::mem::size_of::<ContainerId>() + 1..].copy_from_slice(&page_id.to_le_bytes());
        bytes
    }

    /// Utility to convert ValueID into data/bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut bit_flag = 0b00001000;
        if self.segment_id.is_some() {
            bit_flag |= 0b00000100;
        }
        if self.page_id.is_some() {
            bit_flag |= 0b00000010;
        }
        if self.slot_id.is_some() {
            bit_flag |= 0b00000001;
        }
        bytes.push(bit_flag);
        // bytes.push(0);
        bytes.extend_from_slice(&self.container_id.to_le_bytes());
        if self.segment_id.is_some() {
            bytes.extend_from_slice(&self.segment_id.unwrap().to_le_bytes());
        }
        if self.page_id.is_some() {
            bytes.extend_from_slice(&self.page_id.unwrap().to_le_bytes());
        }
        if self.slot_id.is_some() {
            bytes.extend_from_slice(&self.slot_id.unwrap().to_le_bytes());
        }
        bytes
    }
}

impl fmt::Debug for ValueId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut buf: String = format!("<c_id:{}", self.container_id);
        if self.segment_id.is_some() {
            buf.push_str(",seg_id:");
            buf.push_str(&self.segment_id.unwrap().to_string());
        }
        if self.page_id.is_some() {
            buf.push_str(",p_id:");
            buf.push_str(&self.page_id.unwrap().to_string());
        }
        if self.slot_id.is_some() {
            buf.push_str(",slot_id:");
            buf.push_str(&self.slot_id.unwrap().to_string());
        }
        buf.push('>');
        write!(f, "{}", buf)
    }
}

/// Page key is used to determine a specific page in a container in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContainerPageId {
    pub c_id: ContainerId,
    pub page_id: PageId,
}

impl ContainerPageId {
    pub fn new(c_id: ContainerId, page_id: PageId) -> Self {
        ContainerPageId { c_id, page_id }
    }
}

impl std::fmt::Display for ContainerPageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, p:{})", self.c_id, self.page_id)
    }
}

#[derive(PartialEq, PartialOrd)]
/// The Log Sequence Number (LSN) for a record or data.
/// This is used for concurrency control and recovery
/// It is a combination of the page id and slot id used to record the location
/// of the record or data in a log. It is used to track changes to a structure.
pub struct Lsn {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

impl Lsn {
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Lsn { page_id, slot_id }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Lsn<{}.{}>", self.page_id, self.slot_id)
    }
}

/// A hashed code to check the integrity of a record or data
pub type CheckSum = u16;

// State tracking for caching/reuse/indexing/etc.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StateInfo {
    pub c_id: ContainerId,
    pub valid_low: LogicalTimeStamp,
    pub valid_high: LogicalTimeStamp,
    pub incremental: bool,
    //pub cost: f64,
}
impl StateInfo {
    pub fn new(c_id: ContainerId, incremental: bool) -> Self {
        StateInfo {
            c_id,
            valid_low: 0,
            valid_high: 0,
            incremental,
            //cost: 0.0,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_vid_tests() {
        let mut vid = ValueId::new(1);
        assert_eq!(vid.container_id, 1);
        let mut v_bytes = vid.to_bytes();
        println!("{:?}", v_bytes);
        let mut vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId::new_page(1, 2);
        v_bytes = vid.to_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId::new_slot(1, 1, 13);
        v_bytes = vid.to_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId {
            container_id: 1,
            segment_id: Some(1),
            page_id: None,
            slot_id: Some(1),
        };
        v_bytes = vid.to_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);

        let vcp1 = ValueId::new(3);
        let vcp2 = ValueId::new_page(3, 4);
        let vcp3 = ValueId::new_slot(3, 4, 0);
        let vcp4 = ValueId::new_slot(3, 4, 2);
        let vcp5 = ValueId::new_slot(2, 4, 0);
        let vcp6 = ValueId::new_page(3, 1);

        assert_eq!(vcp2.to_cp_bytes(), vcp3.to_cp_bytes());
        assert_eq!(vcp2.to_cp_bytes(), vcp4.to_cp_bytes());
        assert_eq!(vcp3.to_cp_bytes(), vcp4.to_cp_bytes());
        assert_ne!(vcp1.to_cp_bytes(), vcp2.to_cp_bytes());
        assert_ne!(vcp2.to_cp_bytes(), vcp5.to_cp_bytes());
        assert_ne!(vcp3.to_cp_bytes(), vcp5.to_cp_bytes());
        assert_ne!(vcp3.to_cp_bytes(), vcp6.to_cp_bytes());
    }

    #[test]
    fn test_fixed_vid_tests() {
        let mut vid = ValueId::new(1);
        assert_eq!(vid.container_id, 1);
        let mut v_bytes = vid.to_fixed_bytes();
        let mut vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId::new_page(1, 2);
        v_bytes = vid.to_fixed_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId::new_slot(1, 1, 13);
        v_bytes = vid.to_fixed_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
        vid = ValueId {
            container_id: 1,
            segment_id: None,
            page_id: None,
            slot_id: Some(1),
        };
        v_bytes = vid.to_fixed_bytes();
        vid2 = ValueId::from_bytes(&v_bytes);
        assert_eq!(vid, vid2);
    }
}
