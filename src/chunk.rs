/// An arbitrarily ordered vector of key-value pairs.
///
/// Submit this to the [`SortingPipeline`] via `submit_unsorted_chunk`.
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
        self.entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2))
    }
}
