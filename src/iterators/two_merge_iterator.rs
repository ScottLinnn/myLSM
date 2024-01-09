#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::f32::consts::E;

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    // currentA: HeapWrapper<I>,
    // currentA: HeapWrapper<I>,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self { a, b })
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.b.is_valid() {
            self.b.key()
        } else {
            self.a.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.b.is_valid() {
            self.b.value()
        } else {
            self.a.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.b.is_valid() || self.a.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.b.is_valid() {
            let _ = self.b.next();
        }
        if self.b.is_valid() {
            return Ok(());
        }
        let _ = self.a.next();
        if self.a.is_valid() {
            return Ok(());
        }
        return Ok(()); // error handling todo
    }
}
