use std::fs::File;
use std::io;
use std::io::Seek;

/// An arbitrarily ordered vector of key-value pairs.
///
/// Submit this to the [`SortingPipeline`](crate::SortingPipeline) via `submit_unsorted_chunk`.
pub struct Chunk<K, V> {
    pub entries: Vec<(K, V)>,
}

impl<K, V> Chunk<K, V>
where
    K: Ord,
{
    pub fn new(entries: Vec<(K, V)>) -> Self {
        Self { entries }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn sort(&mut self) {
        self.entries.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2))
    }
}

/// The files storing a sorted chunk of key-value pairs.
pub struct SortedChunkFiles {
    pub key_file: File,
    pub value_file: File,
    pub num_entries: usize,
}

impl SortedChunkFiles {
    pub fn new(
        mut key_file: File,
        mut value_file: File,
        num_entries: usize,
    ) -> Result<Self, io::Error> {
        key_file.rewind()?;
        value_file.rewind()?;
        Ok(Self {
            key_file,
            value_file,
            num_entries,
        })
    }
}

// Implement Ord so we can use `SortedChunkFiles` in a priority queue.
impl Ord for SortedChunkFiles {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.num_entries.cmp(&other.num_entries)
    }
}
impl PartialOrd for SortedChunkFiles {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.num_entries.partial_cmp(&other.num_entries)
    }
}
impl Eq for SortedChunkFiles {}
impl PartialEq for SortedChunkFiles {
    fn eq(&self, other: &Self) -> bool {
        self.num_entries.eq(&other.num_entries)
    }
}
