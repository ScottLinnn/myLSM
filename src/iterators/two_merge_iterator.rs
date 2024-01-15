#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::f32::consts::E;

use anyhow::Result;
use bytes::Bytes;

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
        let mut res = Self { a, b };
        if res.a.is_valid() && res.b.is_valid() && res.a.key() == res.b.key() {
            let _ = res.b.next();
        }
        return Ok(res);
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.a.is_valid() && !self.b.is_valid() {
            self.a.key()
        } else if !self.a.is_valid() && self.b.is_valid() {
            self.b.key()
        } else if self.a.is_valid() && self.b.is_valid() {
            if self.a.key() <= self.b.key() {
                self.a.key()
            } else {
                self.b.key()
            }
        } else {
            &[0]
        }
    }

    fn value(&self) -> &[u8] {
        if self.a.is_valid() && !self.b.is_valid() {
            self.a.value()
        } else if !self.a.is_valid() && self.b.is_valid() {
            self.b.value()
        } else if self.a.is_valid() && self.b.is_valid() {
            if self.a.key() <= self.b.key() {
                self.a.value()
            } else {
                self.b.value()
            }
        } else {
            &[0]
        }
    }

    fn is_valid(&self) -> bool {
        self.b.is_valid() || self.a.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && !self.b.is_valid() {
            let _ = self.a.next();
            return Ok(());
        } else if !self.a.is_valid() && self.b.is_valid() {
            let _ = self.b.next();
            return Ok(());
        } else if self.a.is_valid() && self.b.is_valid() {
            if self.a.key() < self.b.key() {
                let _ = self.a.next();
                if self.a.is_valid() && self.a.key() == self.b.key() {
                    let _ = self.b.next();
                }
                return Ok(());
            } else {
                let _ = self.b.next();
                if self.b.is_valid() && self.a.key() == self.b.key() {
                    let _ = self.b.next();
                }
            }
        }

        return Ok(()); // error handling todo
    }
}
