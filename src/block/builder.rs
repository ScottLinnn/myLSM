use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    block: Block,
    curr_size: u16,
    block_size: u16,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            block: Block {
                data: Vec::new(),
                offsets: Vec::new(),
            },
            curr_size: 0u16,
            block_size: block_size as u16,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        if self.curr_size + (key.len() + 2) as u16 + (value.len() + 2) as u16 + 2 > self.block_size
        {
            return false;
        }
        self.block.offsets.push(self.block.data.len() as u16);
        self.block.data.push((key.len() >> 8) as u8);
        self.block.data.push((key.len()) as u8);
        for i in 0..key.len() {
            self.block.data.push(key[i]);
        }
        self.block.data.push((value.len() >> 8) as u8);
        self.block.data.push((value.len()) as u8);
        for i in 0..value.len() {
            self.block.data.push(value[i]);
        }
        self.curr_size += (key.len() + 2) as u16 + (value.len() + 2) as u16 + 2;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        if self.curr_size == 0 {
            return true;
        }
        false
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        self.block
    }
}
