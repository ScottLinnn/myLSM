#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

// use core::slice::SlicePattern;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        let meta_offset = buf.len();
        for meta in block_meta {
            buf.append(&mut meta.offset.to_be_bytes().to_vec());
            buf.append(&mut meta.first_key.len().to_be_bytes().to_vec());
            buf.append(&mut meta.first_key.to_vec());
        }
        buf.append(&mut meta_offset.to_be_bytes().to_vec());
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let usize_size = std::mem::size_of::<usize>();
        let total_len = buf.remaining();

        let mut block_metas = Vec::new();

        let bytes = buf.copy_to_bytes(total_len).to_vec();

        let mut curr_idx = 0;

        while curr_idx < total_len {
            let offset_bytes: [u8; 8] = bytes[curr_idx..curr_idx + usize_size]
                .try_into()
                .expect("len not match");
            let offset = usize::from_be_bytes(offset_bytes);
            curr_idx += usize_size;

            let keylen_bytes: [u8; 8] = bytes[curr_idx..curr_idx + usize_size]
                .try_into()
                .expect("len not match");
            let keylen = usize::from_be_bytes(keylen_bytes);
            curr_idx += usize_size;

            let key_bytes = bytes[curr_idx..curr_idx + keylen].to_vec();
            curr_idx += keylen;

            block_metas.push(BlockMeta {
                offset: offset,
                first_key: Bytes::from(key_bytes),
            })
        }

        block_metas
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(Bytes::from(data)))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let usize_size = std::mem::size_of::<usize>();
        let total_size: u64 = file.size();
        let bmo_vec = file
            .read(total_size - usize_size as u64, usize_size as u64)
            .expect("cant read bmo vec from file");
        let block_meta_offset = Bytes::from(bmo_vec).get_u64();
        let block_metas_bytes = file
            .read(
                block_meta_offset,
                total_size - usize_size as u64 - block_meta_offset,
            )
            .expect("cant read block_metas_bytes from file");
        let block_metas = BlockMeta::decode_block_meta(Bytes::from(block_metas_bytes));
        Ok(Self {
            file,
            block_metas,
            block_meta_offset: block_meta_offset as usize,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = self.block_metas[block_idx].offset as u64;

        let mut end_offset = 0u64;
        if block_idx >= self.block_metas.len() {
            end_offset = self.block_meta_offset as u64;
        } else {
            end_offset = self.block_metas[block_idx + 1].offset as u64;
        }
        let block = Block::decode(
            self.file
                .read(start_offset, end_offset - start_offset)
                .expect("cant read a block from file")
                .as_slice(),
        );
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        for idx in 0..self.block_metas.len() {
            let curr_key = &self.block_metas[idx].first_key;
            if self.compare_bytes(curr_key, key) {
                return idx;
            }
        }
        usize::MAX
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }

    fn compare_bytes(&self, left: &[u8], right: &[u8]) -> bool {
        return String::from_utf8(left.to_vec()).unwrap()
            >= String::from_utf8(right.to_vec()).unwrap();
    }
}

#[cfg(test)]
mod tests;
