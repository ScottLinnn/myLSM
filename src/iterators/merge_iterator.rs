#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::usize;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: HeapWrapper<I>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut idx: usize = 0 as usize;
        let mut bh: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();

        for iter in iters {
            if !iter.is_valid() {
                continue;
            }
            let hw = HeapWrapper { 0: idx, 1: iter };
            bh.push(hw);
            idx += 1;
        }
        // println!("heap size: {}", bh.len());
        if bh.is_empty() {
            panic!("fail to create");
        }
        let curr: HeapWrapper<I> = bh.pop().unwrap();
        MergeIterator {
            iters: bh,
            current: curr,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.1.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        let _ = self.current.1.next();
        while !self.current.1.is_valid() {
            if self.iters.is_empty() {
                break;
            }
            self.current = self.iters.pop().unwrap();
        }
        Ok(())
    }
}
