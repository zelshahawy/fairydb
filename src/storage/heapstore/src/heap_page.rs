use common::prelude::*;
#[allow(unused_imports)]
use common::PAGE_SIZE;

#[allow(unused_imports)]
use crate::page::{Offset, Page, OFFSET_NUM_BYTES};

use std::mem;

use crate::page::PAGE_FIXED_HEADER_LEN;

#[allow(dead_code)]
/// The size of a slotID
pub(crate) const SLOT_ID_SIZE: usize = mem::size_of::<SlotId>();
#[allow(dead_code)]
/// The allowed metadata size per slot
pub(crate) const SLOT_METADATA_SIZE: usize = 4;
#[allow(dead_code)]
/// The size of the metadata allowed for the heap page, this is in addition to the page header
pub(crate) const HEAP_PAGE_FIXED_METADATA_SIZE: usize = 8;

pub(crate) const SLOT_NUMBER_OFFSET: usize = PAGE_FIXED_HEADER_LEN;
pub(crate) const NEXT_FREE_SLOT_OFFSET: usize = SLOT_NUMBER_OFFSET + 2;
pub(crate) const LOWEST_AVAIL_OFFSET: usize = NEXT_FREE_SLOT_OFFSET + 2;
pub(crate) const REMAINING_SIZE_OFFSET: usize = LOWEST_AVAIL_OFFSET + 2;

pub(crate) const OFFSET_SIZE: usize = mem::size_of::<Offset>();
/// This is trait of a HeapPage for the Page struct.
///
/// The page header size is fixed to `PAGE_FIXED_HEADER_LEN` bytes and you will use
/// additional bytes for the HeapPage metadata
/// Your HeapPage implementation can use a fixed metadata of 8 bytes plus 4 bytes per value/entry/slot stored.
/// For example a page that has stored 3 values, we would assume that the fist
/// `PAGE_FIXED_HEADER_LEN` bytes are used for the page metadata, 8 bytes for the HeapPage metadata
/// and 12 bytes for slot meta data (4 bytes for each of the 3 values).
/// This leave the rest free for storing data (PAGE_SIZE-PAGE_FIXED_HEADER_LEN-8-12).
///
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
pub trait HeapPage {
    // Add any new functions here

    /// Read a little‐endian u16 at byte `offset`.
    fn read_u16_at(&self, offset: usize) -> u16;

    /// Write a little‐endian u16 `value` at byte `offset`.
    fn write_u16_at(&mut self, offset: usize, value: u16);

    /// How many slots have ever been allocated (including freed ones).
    fn slot_count(&self) -> usize;
    fn set_slot_count(&mut self, count: usize);

    /// Byte index of the next free byte in the data region.
    fn next_free(&self) -> usize;
    fn set_next_free(&mut self, ptr: usize);

    /// Lowest‐numbered free slot ID (or `slot_count` if none).
    fn lowest_avail(&self) -> usize;
    fn set_lowest_avail(&mut self, idx: usize);

    /// Total free bytes remaining (including room for directory growth).
    fn remaining_size(&self) -> usize;
    fn set_remaining_size(&mut self, size: usize);

    /// Write offset+length metadata for `slot`.
    fn write_slot_meta(&mut self, slot: SlotId, data_offset: usize, length: usize);

    /// Compact all live records to the end of the page.
    fn compact_page(&mut self);
    // Do not change these functions signatures (only the function bodies)

    /// Initialize the page struct as a heap page.
    #[allow(dead_code)]
    fn init_heap_page(&mut self);

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    #[allow(dead_code)]
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId>;

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    #[allow(dead_code)]
    fn get_value(&self, slot_id: SlotId) -> Option<&[u8]>;

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    #[allow(dead_code)]
    fn delete_value(&mut self, slot_id: SlotId) -> Option<()>;

    /// Update the value for the slotId. If the slotId is not valid or there is not
    /// space on the page return None and leave the old value/slot. If there is space, update the value and return Some(())
    #[allow(dead_code)]
    fn update_value(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<()>;

    /// A utility function to determine the current size of the header for this page
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    fn get_header_size(&self) -> usize;

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    fn get_free_space(&self) -> usize;

    #[allow(dead_code)]
    /// Create an iterator for the page. This should return an iterator that will
    /// return the bytes and the slotId for each value in the page.
    fn iter(&self) -> HeapPageIter<'_>;

    fn iter_from(&self, start_slot_id: SlotId) -> HeapPageIter<'_>;
}

impl HeapPage for Page {
    fn init_heap_page(&mut self) {
        self.data[PAGE_FIXED_HEADER_LEN..PAGE_SIZE].fill(0); // Zero out everything just in case.
                                                             // Basically, I want to store four informations in the metadata:
                                                             // How many slots and The next free slot, the remaining size, lowest avil slot id. All are two bytes.

        //slot_count = 0
        self.write_u16_at(SLOT_NUMBER_OFFSET, 0);

        // 3) next_free = end of page
        self.write_u16_at(NEXT_FREE_SLOT_OFFSET, PAGE_SIZE as u16);

        // 4) lowest_avail = 0
        self.write_u16_at(LOWEST_AVAIL_OFFSET, 0);

        // 5) remaining_size = free bytes available for slotectory + body
        let remaining = (PAGE_SIZE - PAGE_FIXED_HEADER_LEN - HEAP_PAGE_FIXED_METADATA_SIZE) as u16;
        self.write_u16_at(REMAINING_SIZE_OFFSET, remaining);
    }

    /// Read a u16 from the page at the given byte offset.
    fn read_u16_at(&self, off: usize) -> u16 {
        let mut buf = [0u8; OFFSET_SIZE];
        buf.copy_from_slice(&self.data[off..off + OFFSET_SIZE]);
        u16::from_le_bytes(buf)
    }

    /// Write a u16 into the page at the given byte offset.
    fn write_u16_at(&mut self, off: usize, v: u16) {
        self.data[off..off + OFFSET_SIZE].copy_from_slice(&v.to_le_bytes());
    }

    /// Number of slots (including used and immediately-reclaimed) in slotectory.
    fn slot_count(&self) -> usize {
        self.read_u16_at(SLOT_NUMBER_OFFSET) as usize
    }
    fn set_slot_count(&mut self, count: usize) {
        self.write_u16_at(SLOT_NUMBER_OFFSET, count as u16);
    }

    /// Pointer to the first free byte in the body.
    fn next_free(&self) -> usize {
        self.read_u16_at(NEXT_FREE_SLOT_OFFSET) as usize
    }
    fn set_next_free(&mut self, ptr: usize) {
        self.write_u16_at(NEXT_FREE_SLOT_OFFSET, ptr as u16);
    }

    /// Lowest available slot id for reuse (or slot_count if none free).
    fn lowest_avail(&self) -> usize {
        self.read_u16_at(LOWEST_AVAIL_OFFSET) as usize
    }
    fn set_lowest_avail(&mut self, idx: usize) {
        self.write_u16_at(LOWEST_AVAIL_OFFSET, idx as u16);
    }

    /// Total remaining free bytes (incl. slot slotectory growth).
    fn remaining_size(&self) -> usize {
        self.read_u16_at(REMAINING_SIZE_OFFSET) as usize
    }
    fn set_remaining_size(&mut self, size: usize) {
        self.write_u16_at(REMAINING_SIZE_OFFSET, size as u16);
    }
    /// Write the slot metadata (offset and length) for a given slot index.
    fn write_slot_meta(&mut self, slot: SlotId, offset: usize, length: usize) {
        let entry_off = PAGE_FIXED_HEADER_LEN
            + HEAP_PAGE_FIXED_METADATA_SIZE
            + (slot as usize) * SLOT_METADATA_SIZE;
        self.write_u16_at(entry_off, offset as u16);
        self.write_u16_at(entry_off + OFFSET_SIZE, length as u16);
    }

    /// Compact the page by moving all used slots to the end of the page.
    fn compact_page(&mut self) {
        const ENTRY_SZ: usize = SLOT_METADATA_SIZE;
        let slot_start = PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE;
        let slot_count = self.slot_count();

        let mut live = Vec::with_capacity(slot_count);
        for slot in 0..slot_count {
            let meta_off = slot_start + slot * ENTRY_SZ;
            let rec_off = self.read_u16_at(meta_off) as usize;
            let rec_len = self.read_u16_at(meta_off + OFFSET_SIZE) as usize;
            if rec_len > 0
                && rec_off
                    .checked_add(rec_len)
                    .map(|end| end <= PAGE_SIZE)
                    .unwrap_or(false)
            {
                live.push((slot, rec_off, rec_len));
            }
        }

        live.sort_unstable_by_key(|&(_, off, _)| off);

        // Slide everything down from the top of the page
        let mut write_ptr = PAGE_SIZE;
        for (slot, rec_off, rec_len) in live.into_iter().rev() {
            let new_start = write_ptr - rec_len;
            self.data.copy_within(rec_off..rec_off + rec_len, new_start);

            let meta_off = slot_start + slot * ENTRY_SZ;
            self.write_u16_at(meta_off, new_start as u16);
            self.write_u16_at(meta_off + OFFSET_SIZE, rec_len as u16);

            write_ptr = new_start;
        }

        self.set_next_free(write_ptr);
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if inserted or None if there was not enough space.
    fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        const ENTRY_SZ: usize = SLOT_METADATA_SIZE;
        // where the slot directory ends
        let hdr_end = PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE;

        // gather info
        let mut remaining = self.remaining_size();
        let old_count = self.slot_count();
        let slot_id = self.lowest_avail();
        let needs_slot = slot_id == old_count;
        let total_needed = bytes.len() + if needs_slot { ENTRY_SZ } else { 0 };

        // fail early
        if total_needed > remaining {
            return None;
        }

        let dir_end = hdr_end + (old_count + needs_slot as usize) * ENTRY_SZ;
        let mut next_free = self.next_free();

        // if not enough contiguous body space, compact once
        if bytes.len() > next_free.saturating_sub(dir_end) {
            self.compact_page();
            next_free = self.next_free();
        }

        // if we’re appending a new slot, bump slot_count
        if needs_slot {
            self.set_slot_count(old_count + 1);
        }

        // allocate at tail
        let write_end = next_free;
        let write_start = write_end - bytes.len();
        self.data[write_start..write_end].copy_from_slice(bytes);
        self.write_slot_meta(slot_id as SlotId, write_start, bytes.len());
        self.set_next_free(write_start);

        // update remaining_size
        remaining = remaining.saturating_sub(total_needed);
        self.set_remaining_size(remaining);

        // fix up lowest_avail and return
        if needs_slot {
            // we appended a brand‐new slot, so no free ones remain
            self.set_lowest_avail(self.slot_count());
        } else {
            // we reused the old lowest_avail slot, scan forward to the next zero‐length
            let entry_base = PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE;
            let mut next = slot_id + 1;
            while next < self.slot_count() {
                let len_off = entry_base + next * ENTRY_SZ + OFFSET_SIZE;
                if self.read_u16_at(len_off) == 0 {
                    break;
                }
                next += 1;
            }
            self.set_lowest_avail(next);
        }
        Some(slot_id as SlotId)
    }

    fn get_value(&self, slot_id: SlotId) -> Option<&[u8]> {
        let slot_id = slot_id as usize;
        if slot_id >= self.slot_count() {
            return None;
        }
        // Get the offset and length of the slot
        let entry_off =
            PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE + slot_id * SLOT_METADATA_SIZE;
        let off = self.read_u16_at(entry_off) as usize;
        let len = self.read_u16_at(entry_off + OFFSET_SIZE) as usize;
        if len == 0 {
            return None;
        }
        // hopefully a fix
        let end = off.checked_add(len)?;
        Some(&self.data[off..end])
    }

    fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let slot_id = slot_id as usize;
        if slot_id >= self.slot_count() {
            return None;
        }
        let entry_off =
            PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE + slot_id * SLOT_METADATA_SIZE;
        let off = self.read_u16_at(entry_off) as usize;
        let len = self.read_u16_at(entry_off + OFFSET_SIZE) as usize;

        if len == 0 {
            return None;
        }

        self.write_slot_meta(slot_id as SlotId, off, 0);
        self.set_remaining_size(self.remaining_size() + len);
        let curr_low = self.lowest_avail();
        if slot_id < curr_low {
            self.set_lowest_avail(slot_id);
        }
        Some(())
    }

    fn update_value(&mut self, slot_id: SlotId, bytes: &[u8]) -> Option<()> {
        let slot = slot_id as usize;

        self.delete_value(slot_id)?;

        self.set_lowest_avail(slot);

        self.add_value(bytes)?;

        Some(())
    }

    #[allow(dead_code)]
    fn get_header_size(&self) -> usize {
        PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE
    }

    #[allow(dead_code)]
    fn get_free_space(&self) -> usize {
        let body_start = PAGE_FIXED_HEADER_LEN + HEAP_PAGE_FIXED_METADATA_SIZE;
        self.next_free().saturating_sub(body_start)
    }

    fn iter(&self) -> HeapPageIter<'_> {
        HeapPageIter {
            page: self,
            next_slot: 0,
            total_slots: self.slot_count(),
        }
    }

    fn iter_from(&self, start_slot_id: SlotId) -> HeapPageIter<'_> {
        let mut iter = self.iter();
        iter.next_slot = start_slot_id as usize;
        iter
    }
}

pub struct HeapPageIter<'a> {
    page: &'a Page,
    next_slot: usize,
    total_slots: usize,
}

impl<'a> Iterator for HeapPageIter<'a> {
    type Item = (&'a [u8], SlotId);

    /// This function will return the next value in the page. It should return
    /// None if there are no more values in the page.
    /// The iterator should return the bytes reference and the slotId for each value in the page as a tuple.
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_slot >= self.total_slots {
            return None;
        }

        let entry_off = PAGE_FIXED_HEADER_LEN
            + HEAP_PAGE_FIXED_METADATA_SIZE
            + self.next_slot * SLOT_METADATA_SIZE;
        let off = self.page.read_u16_at(entry_off) as usize;
        let len = self.page.read_u16_at(entry_off + OFFSET_SIZE) as usize;

        if len == 0 {
            self.next_slot += 1;
            return self.next();
        }

        let end = off.checked_add(len)?;
        let bytes = &self.page.data[off..end];
        let slot_id = self.next_slot as SlotId;

        self.next_slot += 1;
        Some((bytes, slot_id))
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl<'a> IntoIterator for &'a Page {
    type Item = (&'a [u8], SlotId);
    type IntoIter = HeapPageIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        HeapPageIter {
            page: self,
            next_slot: 0,
            total_slots: self.slot_count(),
        }
    }
}
