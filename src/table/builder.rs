#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{mem, path::Path};

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::block::BlockIterator;
use crate::{block::BlockBuilder, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    block_builder: BlockBuilder,
    block_size: usize,
    total_size: usize,
    block_bytes: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size: block_size,
            total_size: 0,
            block_bytes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if !self.block_builder.add(key, value) {
            let old_builder: BlockBuilder =
                mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
            let built_block = old_builder.build();
            let bytes = built_block.encode();
            self.block_bytes.append(&mut bytes.to_vec());

            let block_iter = BlockIterator::create_and_seek_to_first(Arc::new(built_block));
            self.meta.push(BlockMeta {
                offset: self.total_size,
                first_key: Bytes::copy_from_slice(block_iter.key()),
            });

            self.total_size += bytes.len();
            let res = self.block_builder.add(key, value);
            assert!(res); // Hopefully a single kv pair won't exceed size
        }
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.total_size
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let fo = super::FileObject::create(path.as_ref(), self.block_bytes);
        Ok(SsTable {
            file: fo.unwrap(),
            block_metas: self.meta,
            block_meta_offset: self.total_size,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
