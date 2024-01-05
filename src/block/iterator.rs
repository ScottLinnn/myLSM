use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut bi = BlockIterator::new(block);
        bi.seek_to_first();
        bi
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut bi = BlockIterator::new(block);
        bi.seek_to_key(key);
        bi
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.set_kv();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.set_kv();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        self.idx = 0;

        let data = &self.block.data;
        let mut key_start = self.block.offsets[self.idx];
        let mut key_end = key_start
            + ((data[key_start as usize] as u16) << 8)
            + data[key_start as usize + 1] as u16
            + 2;

        while !self.compare_bytes(&data[key_start as usize + 2..key_end as usize], key) {
            self.idx += 1;
            if self.idx >= self.block.offsets.len() {
                break;
            }
            key_start = self.block.offsets[self.idx];
            key_end = key_start
                + ((data[key_start as usize] as u16) << 8)
                + data[key_start as usize + 1] as u16
                + 2;
        }
        self.set_kv();
    }

    fn compare_bytes(&self, left: &[u8], right: &[u8]) -> bool {
        return String::from_utf8(left.to_vec()).unwrap()
            >= String::from_utf8(right.to_vec()).unwrap();
    }

    // Once index updated, set key and value by accessing data via idx
    fn set_kv(&mut self) {
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let key_start = self.block.offsets[self.idx];
        let data: &Vec<u8> = &self.block.data;
        let key_end = key_start
            + ((data[key_start as usize] as u16) << 8)
            + data[key_start as usize + 1] as u16
            + 2;
        let val_start = key_end;
        let val_end = val_start
            + ((data[val_start as usize] as u16) << 8)
            + data[val_start as usize + 1] as u16
            + 2;
        self.key = data[key_start as usize + 2..key_end as usize].to_vec();
        self.value = data[val_start as usize + 2..val_end as usize].to_vec();
    }
}
