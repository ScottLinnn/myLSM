#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.map.get(key) {
            Some(value) => Some(value.value().clone()),
            None => None,
        }
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::from(key.to_vec()), Bytes::from(value.to_vec()));
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let lower_bound = match lower {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(Bytes::from(key.to_vec())),
            Bound::Excluded(key) => Bound::Excluded(Bytes::from(key.to_vec())),
        };
        let upper_bound = match upper {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(Bytes::from(key.to_vec())),
            Bound::Excluded(key) => Bound::Excluded(Bytes::from(key.to_vec())),
        };

        // Create the MemTableIterator
        let it = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map: &Arc<SkipMap<Bytes, Bytes>>| map.range((lower_bound, upper_bound)),
            // You may need to adjust the item field initialization based on your actual use case
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        return it;
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key(), entry.value());
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_item_mut(|x| *x = entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
