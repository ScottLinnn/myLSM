mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded: Vec<u8> = Vec::new();
        for i in 0..self.data.len() {
            encoded.push(self.data[i]);
        }
        for i in 0..self.offsets.len() {
            encoded.push((self.offsets[i] >> 8) as u8);
            encoded.push((self.offsets[i]) as u8);
        }
        encoded.push((self.offsets.len() >> 8) as u8);

        encoded.push((self.offsets.len()) as u8);
        let encoded_bytes: &[u8] = &encoded;
        // if let Ok(array) = my_array {
        //     // Use array
        // }
        Bytes::copy_from_slice(encoded_bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num = *data.last().unwrap() as usize; // Get the number of elements

        let mut decoded_data = Vec::new();
        let mut decoded_offsets = Vec::new();
        let data_end = data.len() - num * 2 - 2;
        // Extract the data
        for i in 0..data_end {
            decoded_data.push(data[i]);
        }

        // Extract the offsets
        for i in 0..num {
            let offset_index = data_end + i * 2; // Calculate the index for the offsets
            let offset = ((data[offset_index] as u16) << 8) | (data[offset_index + 1] as u16);
            decoded_offsets.push(offset);
        }

        Block {
            data: decoded_data,
            offsets: decoded_offsets,
        }
    }
}

#[cfg(test)]
mod tests;
