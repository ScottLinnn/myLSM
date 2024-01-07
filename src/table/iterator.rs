#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{io::Bytes, sync::Arc};

use anyhow::{Error, Ok, Result};

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    // block: Arc<Block>,
    block_it: BlockIterator,
    idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0).expect("Cannot read block");
        Ok(Self {
            table,
            // block,
            block_it: BlockIterator::create_and_seek_to_first(block),
            idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.idx = 0;
        let block = self.table.read_block(0).expect("Cannot read block");
        self.block_it = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block = table.read_block(0).expect("Cannot read block");
        let mut si = SsTableIterator {
            table,
            // block,
            block_it: BlockIterator::create_and_seek_to_first(block),
            idx: 0,
        };
        let _ = si.seek_to_key(key);
        Ok(si)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let mut left: usize = 0;
        let mut right: usize = self.table.num_of_blocks() - 1;
        let mut mid = (left + right) / 2;
        while left < right {
            let curr_key = &self.table.block_metas[mid].first_key;
            if self.compare_bytes(curr_key, key) {
                right = mid;
                mid = (left + right) / 2;
            } else {
                // If there exists next block
                if mid < self.table.num_of_blocks() - 1 {
                    let next_key = &self.table.block_metas[mid + 1].first_key;
                    // If first key in next block is larger than query key, then this block is the one
                    if self.compare_bytes(next_key, key) {
                        break;
                    } else {
                        // Next block is also smaller, we need to continue searching
                        left = mid + 1;
                        mid = (left + right) / 2;
                    }
                } else {
                    // If not exist, meaning curr block is last block, has to be it.=
                    break;
                }
            }
        }
        let block = self.table.read_block(mid).expect("Cannot read block");
        self.block_it = BlockIterator::create_and_seek_to_first(block);
        self.block_it.seek_to_key(key);
        if !self.block_it.is_valid() && mid < self.table.num_of_blocks() - 1 {
            mid += 1;
            let block = self.table.read_block(mid).expect("Cannot read block");
            self.block_it = BlockIterator::create_and_seek_to_first(block);
            self.block_it.seek_to_key(key);
        }
        self.idx = mid;
        Ok(())
    }

    fn compare_bytes(&self, left: &[u8], right: &[u8]) -> bool {
        return String::from_utf8(left.to_vec()).unwrap()
            > String::from_utf8(right.to_vec()).unwrap();
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_it.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_it.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_it.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_it.next();
        if !self.block_it.is_valid() && self.idx < self.table.num_of_blocks() - 1 {
            self.idx += 1;
            let block: Arc<Block> = self.table.read_block(self.idx).expect("Cannot read block");
            self.block_it = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
