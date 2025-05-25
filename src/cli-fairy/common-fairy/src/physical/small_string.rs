use crate::ids::ContainerId;
use crate::CrustyError;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::config::ServerConfig;

// SmallString -  Static, the offset is a static length
// This is an implemenation of the SmallStringOptimization based on this document https://docs.google.com/document/d/1UDeXmxEt6eTU0dDdxg24_-cACyaaNr8dDPYlLg6dsVM/edit
//  with some modification
// A string can either be long or short, this is signaled by the first bit of the first string
// Short String:
// | Header Byte (0 + len of string) | contents of string |
// If a a string is longer than 31 bytes, then the string is split up into prefix and suffix
// Long String:
// | Header Byte : (1 + len of offset) | prefix of string | offset | -> | length of suffix | suffix |
// The suffix is stored instead memory which is managed by a string manager, and the prefix is stored inside the string in the following structure

// Header byte, prefix, offset.
// Header byte: contains metadata about the string if it is long or short, the length of the prefix
// Prefix: contains the prefix of the string, noticably this will not be a fixed length as it will depend on the size of the offset which is dynamic based on its value
// Offset: The offset from the start of the memory managed by the string manager to the location of the start of suffix of the string
// In the memory, first the length of the suffix is stored and then actual suffix is stored

// If a string is smaller than 31 bytes, it can just be stored inside the string object, without reference to the string manager
// The structure of a small string is Header byte, string content
// Header byte: says the string is short, and the length of the string
// Stirng content: the content of the string

const MAX_SHORT_LEN: usize = 32 - 1;
// Maximum length of bytes a string can be
const LENGTH_SIZE: usize = 4;
const OFFSET_LEN: usize = 4;

#[derive(Default, Copy, Clone, Serialize, Deserialize)]
///A struct which is used by the stringmanager to keep track of the locations and size of the freespace
struct FreeRegion {
    size: usize,
    offset: usize,
}

impl FreeRegion {
    fn new(size: usize, offset: usize) -> Self {
        Self { size, offset }
    }
}
#[derive(Clone, Serialize, Deserialize)]
/// StringManager is responsible for managing the suffixes of longer strings.
/// Is composed of a struct memory and then a sorted vec of the free regions for insertion
/// Memory: The total memory
/// Free_regions: A vector of the free regions
/// capacity: a static number which contains the capacity of the stringmanager - if 0 the stringmanager was not provided
pub struct StringManager {
    container_id: ContainerId,
    memory: Arc<RwLock<Vec<u8>>>,
    free_regions: Arc<RwLock<Vec<FreeRegion>>>,
    capacity: usize,
}

impl std::fmt::Debug for StringManager {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("StringManager")
            .field("memory", &"Arc<RwLock<Vec<u8>>>")
            .field("free_regions", &"Arc<RwLock<Vec<FreeRegion>>>")
            .finish()
    }
}

impl Default for StringManager {
    fn default() -> Self {
        Self::new(Box::leak(Box::new(ServerConfig::temporary())), 0, 0)
    }
}

/// Utility function to calculate the number of bytes to store a number
/// 2^16 -> 3, 487549 -> 3
fn calculate_offset_size(offset: usize) -> u8 {
    // Starting from the assumption that at least one byte is needed.
    let mut bytes_needed = 1;

    // Loop through each byte position to find the highest non-zero byte.
    for i in (0..std::mem::size_of::<usize>()).rev() {
        if (offset >> (i * 8)) & 0xFF != 0 {
            bytes_needed = i + 1;
            break;
        }
    }

    bytes_needed as u8
}

impl StringManager {
    /// Create an instance of a string manager
    /// Capacity: The capacity of the string manager (xtx should this be usize)
    pub fn new(_config: &'static ServerConfig, capacity: usize, container_id: ContainerId) -> Self {
        // when we care about persistence, first check a fixed file location to "bring back" the old manager
        //   similar to stat manager's new() methods.
        Self {
            container_id,
            memory: Arc::new(RwLock::new(vec![0; capacity])),
            free_regions: Arc::new(RwLock::new(vec![FreeRegion::new(capacity, 0)])),
            capacity,
        }
    }

    pub fn shutdown(&self) -> Result<(), CrustyError> {
        info!("TODO: string manager shutdown is a stub");
        // TODO: if string manager state is required to be held across shutdown, then write a serialized
        //   struct version to disk and read from it in the new() method. See reservoir_stat_manager to
        //   see how this is done. Ask Kathir questions.
        Ok(())
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        info!("TODO: string manager reset is a stub");
        // TODO: reset properly
        Ok(())
    }

    /// returns the capacity of a storage manager
    #[allow(dead_code)]
    fn capacity(&self) -> usize {
        self.capacity
    }

    /// Allocate bytes in the string manager using binary search, returns the offset that the data was inserted at
    /// Will insert using binary search finding the next smallest region to fit into
    /// bytes: The bytes to insert
    ///
    /// Returns: An optional offset which is where the information was inserted (xtx should this be usize?)
    /// XTX Update usize
    fn allocate(&self, bytes: &[u8]) -> Option<usize> {
        let mut free_regions = self.free_regions.write().unwrap();
        let size = bytes.len();

        let mut suitable_region_index: Option<usize> = None;
        let mut left = 0;
        let mut right = free_regions.len();
        let mut effective_size_needed = 0;
        while left < right {
            let mid = left + (right - left) / 2;
            let region = &free_regions[mid];

            // Effective size needed is the memory size needed to store the suffix and the length of the suffix
            effective_size_needed = size - MAX_SHORT_LEN + OFFSET_LEN + LENGTH_SIZE;

            if region.size >= effective_size_needed {
                suitable_region_index = Some(mid);
                right = mid; // Aim to find the first suitable region.
            } else {
                left = mid + 1;
            }
        }
        suitable_region_index.and_then(|index| {
            let region = &free_regions[index];
            if region.size >= effective_size_needed {
                // Calculate the starting position for data insertion within the region.
                let start_pos = region.offset;

                // Update the region to reflect the allocation.
                if region.size > effective_size_needed {
                    let size_of_insert = region.size - effective_size_needed;
                    let insert_offset = start_pos + effective_size_needed;
                    // Checking if can just modify in place
                    if (index == 0) || (index > 0) && size_of_insert > free_regions[index - 1].size
                    {
                        // If the region has more space than needed, adjust its size and offset.
                        free_regions[index] = FreeRegion {
                            size: size_of_insert,
                            offset: insert_offset,
                        };
                    }
                    // Need to reinsert
                    else {
                        // XTX I don't believe that this else branch is necessary
                        let new_free_region = FreeRegion::new(size_of_insert, insert_offset);
                        // Remove the index
                        free_regions.remove(index);
                        // Find the new insert index
                        let insertion_index = free_regions
                            .binary_search_by(|region| region.size.cmp(&size_of_insert))
                            .unwrap_or_else(|x| x); // Use the Err case to get the insertion point

                        // Insert the new free region into the sorted list
                        free_regions.insert(insertion_index, new_free_region);
                        // Greedily merge free regions
                        self.merge_adjacent_free_regions(insertion_index);
                    }
                } else {
                    // If the region is exactly used up, remove it from the list.
                    free_regions.remove(index);
                }

                // Allocate space for the suffix length and the data.
                let mut memory = self.memory.write().unwrap();

                // First, store the length of the suffix (bytes.len()) in the first LENGTH_SIZE bytes.
                // XTX not sure how to get the u32 to match the LENGTH_SIZE
                let suffix_length = effective_size_needed - LENGTH_SIZE;
                let suffix_length_bytes = (suffix_length as u32).to_le_bytes();
                memory[start_pos..start_pos + LENGTH_SIZE].copy_from_slice(&suffix_length_bytes);

                // Then, store the actual bytes immediately after.
                memory[start_pos + LENGTH_SIZE..start_pos + LENGTH_SIZE + suffix_length]
                    .copy_from_slice(&bytes[bytes.len() - suffix_length..bytes.len()]);

                Some(start_pos)
            } else {
                None
            }
        })
    }

    /// Deallocates values from the memory in the StringManager
    /// Offset: The starting offset of the information to deallocate
    /// Size: The size of the information to deallocate
    #[allow(dead_code)]
    fn deallocate(&self, offset: usize, size: usize) {
        let mut free_regions = self.free_regions.write().unwrap();

        // Create a new free region for the deallocated memory
        let new_free_region = FreeRegion::new(size, offset);

        // Use binary search to find the right insertion point based on offset.
        // The goal is to maintain the list sorted by offset after insertion.
        let insertion_index = free_regions
            .binary_search_by(|region| region.size.cmp(&size))
            .unwrap_or_else(|x| x); // Use the Err case to get the insertion point

        // Insert the new free region into the sorted list
        free_regions.insert(insertion_index, new_free_region);

        // Merge adjacent free regions to minimize fragmentation
        self.merge_adjacent_free_regions(insertion_index);
    }

    /// Helper for deallocate, will merge adjacent free regions
    /// Is greedy so will only need to check for regions that overlap with
    /// index: The index of the region just deleted
    fn merge_adjacent_free_regions(&self, index: usize) {
        let mut free_regions = self.free_regions.write().unwrap();
        let left_edge = free_regions[index].offset;
        let right_edge = free_regions[index].offset + free_regions[index].size;
        let mut i = 0;
        while i < free_regions.len() {
            if i != index {
                let fr_left_edge = free_regions[i].offset;
                let fr_right_edge = free_regions[i].offset + free_regions[i].size;
                // Check if the left edge of the free region that is being currently checking matches the right_edge that was just deleted
                // If can merge them expand the size of the recently deleted one and then deleted the neighbour
                if fr_left_edge == right_edge {
                    let merged_size = free_regions[i].size + free_regions[index].size;
                    free_regions[index].size = merged_size;
                    free_regions.remove(i);
                }
                // Do the same as the other case but also make sure to change the offset of the recently created region
                if fr_right_edge == left_edge {
                    let merged_size = free_regions[i].size + free_regions[index].size;
                    free_regions[index].size = merged_size;
                    free_regions[index].offset = fr_left_edge;
                    free_regions.remove(i);
                }
            }
            i += 1;
        }
    }

    /// Compares the contents of two strings stored in the StringManager, identified by their offsets.
    /// offset1: the offset of the first string
    /// offset2: the offset of the second string
    pub fn compare_strings(&self, offset1: usize, offset2: usize) -> Ordering {
        // Assume that each string's first LENGTH_SIZE bytes in the heap store its length.
        let memory = self.memory.read().unwrap();

        // Read lengths of the strings from their first LENGTH_SIZE bytes.
        let length1 = self.read_length_from_memory(offset1);
        let length2 = self.read_length_from_memory(offset2);

        // Extract the actual string bytes, skipping the first LENGTH_SIZE bytes where the length is stored.
        let string1 = &memory[(offset1 + LENGTH_SIZE)..(offset1 + LENGTH_SIZE + length1)];
        let string2 = &memory[(offset2 + LENGTH_SIZE)..(offset2 + LENGTH_SIZE + length2)];

        // Compare the extracted byte slices.
        string1.cmp(string2)
    }

    /// Utility method to read a string's length stored in the first LENGTH_SIZE bytes at the given offset.
    /// offset: offset of the string to read the length of
    fn read_length_from_memory(&self, offset: usize) -> usize {
        let memory_lock = self.memory.read().unwrap();
        let memory = &*memory_lock;

        // Now you can index into `memory` since it's a reference to Vec<u8>
        let length_bytes = &memory[offset..(offset + LENGTH_SIZE)];

        u32::from_le_bytes(
            length_bytes
                .try_into()
                .expect("Failed to convert length bytes to u32"),
        ) as usize
    }

    /// Reads a string from the managed memory at a given offset.
    /// The first LENGTH_SIZE bytes at the offset indicate the string's length.
    /// The string data follows immediately after these LENGTH_SIZE bytes.
    /// offset: The offset of the string
    ///
    /// Return: the string or an error if was unable to read successfully
    pub fn get_string_at_offset(
        &self,
        offset: usize,
    ) -> Result<String, std::string::FromUtf8Error> {
        let memory = self.memory.read().expect("Failed to lock memory");
        let length_bytes = &memory[offset..offset + LENGTH_SIZE];
        let length = u32::from_le_bytes(
            length_bytes
                .try_into()
                .expect("Failed to convert length bytes to u32"),
        ) as usize;
        // Extracting the string data based on the read length.
        let string_data = &memory[offset + LENGTH_SIZE..offset + LENGTH_SIZE + length];
        // Converting the byte slice to a Rust String.
        String::from_utf8(string_data.to_vec())
    }

    ///Finds the total amount of free space left inside the memory
    /// Returns: the amount of freespace
    pub fn get_free_space(&self) -> usize {
        let free_regions = self.free_regions.read().unwrap();
        free_regions.iter().map(|region| region.size).sum()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SerializedString {
    flag_and_size: u8,
    data: [u8; MAX_SHORT_LEN],
}

#[derive(Debug, Clone)]
/// Implementation of the SSO, more information at the top
/// First byte: flag + size/offset length information
/// If long, the last 4 bits of the first byte indicate the size of the offset needed for the string manager
/// If short, the last 4 bits of the first byte indicate the actual length of the string
pub struct SmallString {
    flag_and_size: u8,
    // The rest is either the string directly (if short) or prefix + offset (if long)
    data: [u8; MAX_SHORT_LEN],
    string_manager: &'static StringManager,
}

impl SmallString {
    /// Takes in a reference to a str and creates a SmallString
    /// s: the str to make a smallstring from
    /// string_manager: the string_manager which manages the potential suffix information
    /// Returns: a instance of a SmallString
    pub fn new(s: &str, string_manager: &'static StringManager) -> Option<Self> {
        let data = s.as_bytes();

        if data.len() <= MAX_SHORT_LEN {
            let mut storage = [0u8; MAX_SHORT_LEN];
            storage[..data.len()].copy_from_slice(data);
            Some(Self {
                flag_and_size: data.len() as u8,
                data: storage,
                string_manager,
            })
        } else if let Some(offset) = string_manager.allocate(data) {
            let prefix_size = MAX_SHORT_LEN - OFFSET_LEN;
            let mut storage = [0u8; MAX_SHORT_LEN];
            storage[..prefix_size].copy_from_slice(&data[..prefix_size]);
            let offset_bytes = (offset as u32).to_le_bytes();
            storage[MAX_SHORT_LEN - OFFSET_LEN..MAX_SHORT_LEN]
                .copy_from_slice(&offset_bytes[..OFFSET_LEN]);
            Some(Self {
                flag_and_size: 0b1000_0000 | (prefix_size as u8 & 0x1F),
                data: storage,
                string_manager,
            })
        } else {
            None
        }
    }

    // XTX did deallocate instead, not sure if this is still necessary
    pub fn delete(self) -> Result<(), CrustyError> {
        // If is short prob don't need to do antyhing
        // If is long then deallocate
        Ok(())
    }

    /// Extract the length of the prefix stored in the object, based on the flag_and_size field.
    fn extract_prefix_len(&self) -> usize {
        (self.flag_and_size & 0b0111_1111) as usize
    }
    /// Extracts the position of the suffix within the string manager
    fn extract_suffix_offset(&self) -> usize {
        let prefix_size = self.extract_prefix_len();
        let offset_bytes_start = prefix_size;
        let mut offset_slice = self.data[offset_bytes_start..MAX_SHORT_LEN].to_vec();
        offset_slice.reverse();
        let mut offset_bytes = [0u8; std::mem::size_of::<usize>()];
        let copy_start = offset_bytes.len().saturating_sub(offset_slice.len());
        offset_bytes[copy_start..].copy_from_slice(&offset_slice);
        usize::from_be_bytes(offset_bytes)
    }
    /// Helper function to dertermine if a string is small
    /// Returns true if small, false if long
    fn is_short(&self) -> bool {
        (self.flag_and_size >> 7) == 0
    }

    /// Checks if the SmallString is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Finds the length of a Smallstring object
    /// Returns: the length of the SmallString
    /// XTX does this need to be LENGTH_SIZE or related to that?
    pub fn len(&self) -> usize {
        let string_manager = &self.string_manager;
        match self.is_short() {
            true => {
                // Short string

                self.extract_prefix_len()
            }
            false => {
                let offset = self.extract_suffix_offset();
                let manager_lock = string_manager;
                let prefix_len = self.extract_prefix_len();
                let suffix_len = manager_lock.read_length_from_memory(offset);
                prefix_len + suffix_len
            }
        }
    }

    /// Compare two SmallString instances.
    /// Will first try to compare them in the memory directly and then if is unable to obtain an order will go to the string manager
    /// other: The other string to compare with
    /// Returns: An ordering between the two strings
    pub fn compare(&self, other: &Self) -> Ordering {
        match (self.is_short(), other.is_short()) {
            // Both strings are short, directly compare their data.
            (true, true) => {
                let self_len = self.extract_prefix_len();
                let other_len = other.extract_prefix_len();
                self.data[..self_len].cmp(&other.data[..other_len])
            }
            // Both strings are long, compare prefixes first then heap contents if necessary.
            (false, false) => {
                // Extract prefix length from the flag_and_size field.
                let self_prefix_len = self.extract_prefix_len();
                let other_prefix_len = other.extract_prefix_len();

                let prefix_cmp = self.data[..self_prefix_len].cmp(&other.data[..other_prefix_len]);
                if prefix_cmp != Ordering::Equal {
                    prefix_cmp
                } else {
                    // Implement actual comparison of the heap contents here.
                    self.compare_heap_contents(other)
                }
            }
            // One string is short, the other is long. Compare the short string to the long string's prefix first.
            (true, false) | (false, true) => {
                let short_bytes = if self.is_short() {
                    &self.data[..self.extract_prefix_len()]
                } else {
                    &other.data[..other.extract_prefix_len()]
                };
                let long_meta = if !self.is_short() { self } else { other };
                let long_prefix_len = long_meta.extract_prefix_len();

                short_bytes.cmp(&long_meta.data[..long_prefix_len]) // Assuming the long content is always greater if prefixes are equal
            }
        }
    }

    /// Compares the content of two strings within the string manager
    /// other: the other string to compare with
    /// Returns: the order between the two strings
    fn compare_heap_contents(&self, other: &Self) -> Ordering {
        let string_manager = &self.string_manager;
        let self_offset = self.extract_suffix_offset();
        let other_offset = other.extract_suffix_offset();
        string_manager.compare_strings(self_offset, other_offset)
    }

    /// Converts the SmallString content to a Rust String.
    /// This method handles both short and long strings and returns a Result
    /// because the conversion from bytes to String can fail if the bytes are not valid UTF-8.
    pub fn to_string(&self) -> Result<String, std::string::FromUtf8Error> {
        let string_manager = &self.string_manager;
        match self.is_short() {
            true => {
                // Short string
                let length: usize = self.extract_prefix_len();
                String::from_utf8(self.data[..length].to_vec())
            }
            false => {
                // Long string
                // Access the StringManager and use it to retrieve the string based on the offset stored in this SmallString.
                let manager_lock = string_manager;
                let manager = manager_lock; // Dereference the MutexGuard to get the manager.
                                            // Extract the prefix length and the actual prefix.
                let prefix_len = self.extract_prefix_len();
                let prefix = std::str::from_utf8(&self.data[..prefix_len])
                    .expect("failed to convert to utf8");

                // Extract the offset where the string is stored in the manager's memory.
                let offset = self.extract_suffix_offset();
                // Use the manager to retrieve the suffix from its managed memory.
                let suffix = manager.get_string_at_offset(offset).unwrap();

                // Combine prefix and suffix into a single String
                Ok(format!("{}{}", prefix, suffix))
            }
        }
    }

    /// Create a SmallString from a byte slice.
    /// This method leverages StringManager to manage long strings.
    /// bytes: the bytes to convert
    /// sm: an optional string manager, which is only required for if the string is long
    /// Returns: an instance of a smallstring which if the Stringmanger wasn't given will just return
    /// a sm
    pub fn from_bytes(bytes: &[u8], sm: &'static StringManager) -> Self {
        let length = bytes.len();

        if length <= MAX_SHORT_LEN {
            let mut data = [0u8; MAX_SHORT_LEN];
            data[..length].copy_from_slice(bytes);
            let flag_and_size = length as u8;
            SmallString {
                flag_and_size,
                data,
                string_manager: sm,
            }
        } else {
            // Long string handling remains the same but uses the static reference
            if sm.capacity == 0 {
                panic!("Did not provide a string manager");
            }

            if let Some(offset) = sm.allocate(bytes) {
                // Rest of your existing long string implementation
                let offset_size = calculate_offset_size(offset) as usize;
                let prefix_size = MAX_SHORT_LEN - offset_size;

                let mut storage = [0u8; MAX_SHORT_LEN];
                storage[..prefix_size].copy_from_slice(&bytes[..prefix_size]);

                let offset_bytes = offset.to_le_bytes();
                storage[MAX_SHORT_LEN - offset_size..MAX_SHORT_LEN]
                    .copy_from_slice(&offset_bytes[..offset_size]);

                Self {
                    flag_and_size: 0b1000_0000 | (prefix_size as u8 & 0x1F),
                    data: storage,
                    string_manager: sm,
                }
            } else {
                panic!("Failed to allocate memory for the long string");
            }
        }
    }

    /// Converts a small string to bytes, will also convert the suffix
    pub fn as_bytes(&self) -> Result<Vec<u8>, CrustyError> {
        let is_long = (self.flag_and_size & 0x80) != 0;
        if is_long {
            // First get the string using to_string()
            let string = self.to_string().map_err(|e| {
                CrustyError::CrustyError(format!("Failed to convert SmallString to String: {}", e))
            })?;

            // Convert the entire string to bytes directly
            Ok(string.into_bytes())
        } else {
            // Short string case
            let prefix_len = self.extract_prefix_len();
            Ok(self.data[0..prefix_len].to_vec())
        }
    }

    /// Writes the contents of the string manager onto a file to be reopened later
    /// file_path: the path of the file to be saved to
    /// Returns: A result to the file path if the shutdown was successful
    #[allow(dead_code)]
    fn shutdown(&self, file_path: PathBuf) -> Result<PathBuf, CrustyError> {
        let mut file = File::create(&file_path)?;
        let serialized = serde_json::to_string(self.string_manager)
            .map_err(|e| CrustyError::CrustyError(format!("Failed serlializing: {}", e)))?;
        file.write_all(serialized.as_bytes())?;
        Ok(file_path)
    }

    // XTX I am slightly confused how to get the strings to re go back othe string manager
    /// Reads the contents of the file to populate a new StringManager
    /// file_path: the path of the file to be read
    #[allow(dead_code)]
    fn startup(file_path: PathBuf, sm: &'static StringManager) -> Result<Self, CrustyError> {
        let mut file = File::open(&file_path)?;
        let mut serialized = String::new();
        file.read_to_string(&mut serialized)?;

        // Create owned StringManager and move it into static storage
        let deserialized_manager: StringManager = serde_json::from_str(&serialized)
            .map_err(|e| CrustyError::CrustyError(format!("Deserialization error: {}", e)))?;

        // Store in static storage
        *sm.memory.write().unwrap() = deserialized_manager.memory.read().unwrap().clone();
        *sm.free_regions.write().unwrap() =
            deserialized_manager.free_regions.read().unwrap().clone();

        Ok(SmallString {
            flag_and_size: 0,
            data: [0; MAX_SHORT_LEN],
            string_manager: sm,
        })
    }
}

// XTX this is making the tests get stuck in a deadlock not sure how to handle, I beleive I am locking in a cosistent matter
// But maybe i should use interior mutability
// impl Drop for SmallString {
//     fn drop(&mut self) {
//         // Check if it's a long string
//         if !self.is_short() {
//             let offset = self.extract_suffix_offset();

//             let length = self.string_manager.read_length_from_memory(offset);

//             // Call the deallocate method on the StringManager
//             self.string_manager.deallocate(offset, length + LENGTH_SIZE);
//         }
//     }
// }

impl Serialize for SmallString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializedString {
            flag_and_size: self.flag_and_size,
            data: self.data,
        }
        .serialize(serializer)
    }
}

// impl<'de> Deserialize<'de> for SmallString {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         let serialized = SerializedString::deserialize(deserializer)?;

//         Ok(SmallString {
//             flag_and_size: serialized.flag_and_size,
//             data: serialized.data,
//             string_manager: ,
//         })
//     }
// }

impl PartialEq for SmallString {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == Ordering::Equal
    }
}

impl Eq for SmallString {}

impl PartialOrd for SmallString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SmallString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl Hash for SmallString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the flag_and_size field
        self.flag_and_size.hash(state);
        self.data.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Utility function to create a string manager instance for testing.
    fn get_test_string_manager() -> &'static StringManager {
        let string_manager = StringManager::new(
            Box::leak(Box::new(ServerConfig::temporary())),
            1024 * 1024,
            0,
        ); // 1MB capacity
        let sm = Box::leak(Box::new(string_manager));
        sm
    }
    #[test]
    fn test_calculate_offset_size() {
        assert_eq!(calculate_offset_size(0), 1);
        assert_eq!(calculate_offset_size(2_u64.pow(16) as usize), 3);
        assert_eq!(calculate_offset_size(487549), 3);
        assert_eq!(calculate_offset_size(2_u64.pow(24) as usize), 4);
        assert_eq!(calculate_offset_size(2_u64.pow(32) as usize), 5);
        assert_eq!(calculate_offset_size(2_u64.pow(40) as usize), 6);
        assert_eq!(calculate_offset_size(2_u64.pow(48) as usize), 7);
        assert_eq!(calculate_offset_size(2_u64.pow(56) as usize), 8);
        assert_eq!(calculate_offset_size(0xFFFF_FFFF_FFFF_FFFF), 8); // Max usize value on a 64-bit system, each F is 4 bits
    }

    #[test]
    fn test_short_string_creation_and_display() {
        let string_manager = get_test_string_manager();
        let content = "hello";
        let my_string = SmallString::new(content, string_manager).unwrap();
        assert_eq!(my_string.to_string().unwrap(), content);
    }
    #[test]
    fn test_short_string_creation_and_display_korean() {
        let string_manager = get_test_string_manager();
        let content = "안녕";
        let my_string = SmallString::new(content, string_manager).unwrap();
        assert_eq!(my_string.to_string().unwrap(), content);
    }

    #[test]
    fn test_long_string_creation_and_display() {
        let string_manager = get_test_string_manager();
        let content = "ahah this is a very long string that exceeds the max short lengt111h";
        let my_string = SmallString::new(content, string_manager).unwrap();
        assert_eq!(my_string.to_string().unwrap(), content);
    }

    #[test]
    fn test_long_string_creation_and_display_korean() {
        let string_manager = get_test_string_manager();
        let content = "안녕하세요! 중요한 것은 꺾이지 않는 많음! 중요한 것은 꺾이지 않는 많음! 중요한 것은 꺾이지 않는 많음! 중요한 것은 꺾이지 않는 많음!";
        let my_string = SmallString::new(content, string_manager).unwrap();
        assert_eq!(my_string.to_string().unwrap(), content);
    }

    #[test]
    fn test_string_comparison_short_long() {
        let string_manager = get_test_string_manager();
        let short_string = SmallString::new("short", string_manager).unwrap();
        let long_string = SmallString::new(
            "this is a very long string that exceeds the max short length",
            string_manager,
        )
        .unwrap();
        assert!(short_string.compare(&long_string) == Ordering::Less);
    }

    #[test]
    fn test_from_bytes_and_as_bytes_short() {
        let string_manager = get_test_string_manager();
        let original_bytes = b"short string";
        let my_string = SmallString::from_bytes(original_bytes, string_manager);
        assert_eq!(my_string.as_bytes().unwrap(), original_bytes);
    }

    #[test]
    fn test_length_short_string() {
        let string_manager = get_test_string_manager();
        let my_string = SmallString::from_bytes(b"hello", string_manager);
        assert_eq!(my_string.extract_prefix_len(), 5);
    }
    fn generate_string(length: usize) -> String {
        let alphabet = "abcdefghijklmnopqrstuvwxyz";
        let mut result = String::with_capacity(length);

        for i in 0..length {
            let char_index = i % 26;
            let c = alphabet.chars().nth(char_index).unwrap();
            result.push(c);
        }

        result
    }

    #[test]
    fn test_bulk_short_strings() {
        let string_manager = get_test_string_manager();
        let mut strings = Vec::new();
        let total_strings = 100;

        // Create and store short strings
        for _i in 1..=total_strings {
            let content = generate_string(5); // Ensure it's short
            let small_string = SmallString::new(content.as_str(), string_manager).unwrap();
            strings.push((content, small_string));
        }

        // Verify each string
        for (expected_content, small_string) in strings {
            assert_eq!(small_string.to_string().unwrap(), expected_content);
        }
    }

    #[test]
    fn test_bulk_long_strings() {
        let string_manager = get_test_string_manager();
        let mut strings = Vec::new();
        let total_strings = 100;

        // Create and store long strings
        for _ in 1..=total_strings {
            let content = generate_string(5000); // Ensure it's long
            let small_string = SmallString::new(content.as_str(), string_manager).unwrap();
            strings.push((content, small_string));
        }

        // Verify each string
        for (expected_content, small_string) in strings {
            assert_eq!(small_string.to_string().unwrap(), expected_content);
        }
    }

    #[test]
    fn test_bulk_long_strings2() {
        let string_manager = get_test_string_manager();
        let mut strings = Vec::new();
        let total_strings = 10000;

        // Create and store long strings
        for _ in 1..=total_strings {
            let content = generate_string(40); // Ensure it's long
            let small_string = SmallString::new(content.as_str(), string_manager).unwrap();
            strings.push((content, small_string));
        }

        // Verify each string
        for (expected_content, small_string) in strings {
            assert_eq!(small_string.to_string().unwrap(), expected_content);
        }
    }

    #[test]
    fn test_mixed_string_types() {
        let string_manager = get_test_string_manager();
        let mut strings = Vec::new();

        // Create a mix of short and long strings
        let short_content = generate_string(5);
        let long_content = generate_string(200);
        strings.push(SmallString::new(short_content.as_str(), string_manager).unwrap());
        strings.push(SmallString::new(long_content.as_str(), string_manager).unwrap());

        // Verify each string
        assert_eq!(strings[0].to_string().unwrap(), short_content);
        assert_eq!(strings[1].to_string().unwrap(), long_content);
    }

    #[test]
    fn test_deallocation_and_reallocation() {
        let string_manager = get_test_string_manager();
        let long_content = generate_string(200);
        let small_string = SmallString::new(long_content.as_str(), string_manager).unwrap();

        // Initially check the long string
        assert_eq!(small_string.to_string().unwrap(), long_content);

        #[allow(clippy::drop_non_drop)]
        drop(small_string);

        // Allocate a new string and ensure the memory can still be used correctly
        let new_content = "new short";
        let new_small_string = SmallString::new(new_content, string_manager).unwrap();
        assert_eq!(new_small_string.to_string().unwrap(), new_content);
    }

    #[test]
    fn test_short_string_length() {
        let string_manager = get_test_string_manager();
        let data = "hello";
        let small_string = SmallString::new(data, string_manager).unwrap();

        assert_eq!(small_string.len(), 5, "Incorrect length for short string");
    }

    #[test]
    fn test_long_string_length() {
        let string_manager = get_test_string_manager();
        // Create a string that is guaranteed to be longer than MAX_SHORT_LEN
        let long_data = "a".repeat(MAX_SHORT_LEN + 1);
        let small_string = SmallString::new(long_data.as_str(), string_manager).unwrap();

        assert_eq!(
            small_string.len(),
            MAX_SHORT_LEN + 1,
            "Incorrect length for long string"
        );
    }

    #[test]
    fn test_empty_string_length() {
        let string_manager = get_test_string_manager();
        let data = "";
        let small_string = SmallString::new(data, string_manager).unwrap();

        assert_eq!(small_string.len(), 0, "Incorrect length for empty string");
    }

    #[test]
    fn test_prefix_and_suffix_length() {
        let string_manager = get_test_string_manager();
        // Ensure the string gets a prefix and a suffix by making it longer than MAX_SHORT_LEN but includes recognizable parts
        let prefix_data = "prefix";
        let suffix_data = "suffix";
        let long_data = format!(
            "{}{}{}",
            prefix_data,
            "a".repeat(MAX_SHORT_LEN - prefix_data.len() - suffix_data.len()),
            suffix_data
        );
        let small_string = SmallString::new(long_data.as_str(), string_manager).unwrap();

        // The length should include both the prefix and the computed suffix length
        assert_eq!(
            small_string.len(),
            long_data.len(),
            "Incorrect combined length for prefix and suffix"
        );
    }

    // Helper function to generate a vector of vectors with increasing string sizes
    // Ensure each string is at least 31 characters long by repeating the number
    fn generate_increasing_size_vectors(total: usize) -> Vec<Vec<String>> {
        (1..=total)
            .map(|n| {
                // Convert n to a string
                let n_str = n.to_string();
                // Determine the minimum length of the string, at least 31 characters or exactly `n`
                let final_length = std::cmp::max(31, n);
                // Calculate how many times to repeat the string to meet or exceed the desired length
                let repeats_needed = final_length.div_ceil(n_str.len());
                // Repeat the string to achieve the necessary length
                vec![n_str.repeat(repeats_needed)]
            })
            .collect()
    }

    #[test]
    fn string_manager_stress_test() {
        let sm = get_test_string_manager();
        let mut stored_strings = Vec::new();

        // Generate a list of strings with increasing size
        let strings = generate_increasing_size_vectors(100);

        // Keep trying to insert strings until there's no more space
        let mut attempts = 0;
        'outer: while attempts < 1000 {
            // Limiting the total number of attempts to avoid infinite loops
            for vec in &strings {
                for s in vec {
                    let string_len = s.len() + LENGTH_SIZE;
                    if string_len <= sm.get_free_space() {
                        let small_string = SmallString::new(s, sm).unwrap();
                        stored_strings.push((s.clone(), small_string.clone()));
                        println!(
                            "Inserted: '{}', Total attempts: {}, Free space: {}",
                            s.len(),
                            attempts,
                            sm.get_free_space()
                        );
                    } else {
                        println!(
                            "Failed to insert: '{}', Not enough space. Free space: {}",
                            s,
                            sm.get_free_space()
                        );
                        break 'outer; // Break out of the loop if no space is available for the current string
                    }
                }
            }
            attempts += 1;
        }

        // Verify all strings are still valid and correct
        for (expected, small_string) in stored_strings.iter() {
            assert_eq!(small_string.to_string().unwrap(), expected.clone());
        }
        // Is hard to do a stress test with dynamic sizing -
    }
}
