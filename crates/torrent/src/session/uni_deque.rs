//! A FIFO queue backed by a [`HashSet`] for O(1) membership queries.
//!
//! Every mutation (push, drain) atomically maintains both structures
//! so [`contains`](UniDeque::contains) is always consistent with the
//! visible queue contents.

use std::collections::{HashSet, VecDeque};

/// A FIFO queue backed by a [`HashSet`] for O(1) membership queries.
///
/// Every mutation (push, drain) atomically maintains both structures
/// so [`contains`](UniDeque::contains) is always consistent with the
/// visible queue contents.
pub(crate) struct UniDeque<T> {
    queue: VecDeque<T>,
    set: HashSet<T>,
}

impl<T: Eq + std::hash::Hash + Clone> UniDeque<T> {
    /// Create an empty [`UniDeque`].
    pub(crate) fn new() -> Self {
        UniDeque {
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    /// Push a value to the back of the queue if it is not already present.
    ///
    /// Returns `true` if the value was newly inserted, `false` if it
    /// was already in the queue (and therefore silently skipped).
    pub(crate) fn push_unique(&mut self, value: T) -> bool {
        let new = self.set.insert(value.clone());
        if new {
            self.queue.push_back(value)
        }
        new
    }

    /// Drain up to `n` items from the front into a `Vec`, removing them
    /// from both the queue and the set atomically.
    ///
    /// If `n` exceeds the current length, all items are drained.
    pub(crate) fn drain_first_n(&mut self, n: usize) -> Vec<T> {
        let n = n.min(self.len());
        let drained = self.queue.drain(..n).inspect(|item| {
            self.set.remove(item);
        });
        drained.collect()
    }

    /// Check whether `value` is present in the queue (O(1)).
    #[allow(dead_code)]
    pub(crate) fn contains(&self, value: &T) -> bool {
        self.set.contains(value)
    }

    /// Return the number of items in the queue.
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty() {
        let d: UniDeque<i32> = UniDeque::new();
        assert_eq!(d.len(), 0);
        assert!(!d.contains(&1));
    }

    #[test]
    fn push_unique_and_contains() {
        let mut d = UniDeque::new();
        d.push_unique(1);
        d.push_unique(2);
        d.push_unique(3);

        assert_eq!(d.len(), 3);
        assert!(d.contains(&1));
        assert!(d.contains(&2));
        assert!(d.contains(&3));
        assert!(!d.contains(&0));
        assert!(!d.contains(&4));
    }

    #[test]
    fn push_unique_duplicate() {
        let mut d = UniDeque::new();
        assert_eq!(d.push_unique(42), true);
        assert_eq!(d.push_unique(42), false);
        assert_eq!(d.push_unique(42), false);

        // Only the first push inserted; duplicates were silently skipped.
        assert!(d.contains(&42));
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn drain_removes_all_items() {
        let mut d = UniDeque::new();
        d.push_unique(10);
        d.push_unique(20);
        d.push_unique(30);

        let drained = d.drain_first_n(3);
        assert_eq!(drained, vec![10, 20, 30]);
        assert_eq!(d.len(), 0);
        assert!(!d.contains(&10));
        assert!(!d.contains(&20));
        assert!(!d.contains(&30));
    }

    #[test]
    fn drain_partial_consumption() {
        let mut d = UniDeque::new();
        d.push_unique(1);
        d.push_unique(2);
        d.push_unique(3);

        let drained = d.drain_first_n(3);
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(d.len(), 0);
        assert!(!d.contains(&1));
        assert!(!d.contains(&2));
        assert!(!d.contains(&3));
    }

    #[test]
    fn drain_more_than_available() {
        let mut d = UniDeque::new();
        d.push_unique(7);

        let drained = d.drain_first_n(100);
        assert_eq!(drained, vec![7]);
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn drain_re_enqueue() {
        let mut d = UniDeque::new();
        d.push_unique(1);
        d.push_unique(2);
        d.push_unique(3);

        // Drain all, then push one back (simulating cooldown retry).
        let drained = d.drain_first_n(3);
        assert_eq!(drained, vec![1, 2, 3]);

        d.push_unique(1);
        assert_eq!(d.len(), 1);
        assert!(d.contains(&1));
    }

    #[test]
    fn len_after_mutations() {
        let mut d = UniDeque::new();
        assert_eq!(d.len(), 0);

        d.push_unique(10);
        assert_eq!(d.len(), 1);

        d.push_unique(20);
        assert_eq!(d.len(), 2);

        d.drain_first_n(1);
        assert_eq!(d.len(), 1);
    }
}
