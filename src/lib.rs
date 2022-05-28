mod chunk;

pub use chunk::Chunk;

use bytemuck::{bytes_of, bytes_of_mut, Pod};
use core::cmp::Reverse;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::thread;
use tempfile::tempfile_in;

const ONE_MIB: usize = 1 << 20;
const MERGE_K: usize = 8;

// PERF: for *large* values, would it be faster to do the entire external sort on (key, value *ID*) pairs first, then use the
// sorted value IDs to swap around the values in one large value file?

/// Accepts an arbitrarily large stream of unordered key-value pairs from the user and streams them into two binary files: one
/// for keys and one for values.
///
/// We denote the input stream as `[(k[0], v[0]), (k[1], v[1]), ...]`. The final key and value files are laid out as `[k[a],
/// k[b], ...]` and `[v[a], v[b], ...]` respectively, such that the key array is sorted. The reason for separate files is to
/// ensure correct data type alignment (for zero-copy reads) without wasting space to padding.
///
/// # Implementation
///
/// To sort an arbitrarily large data set without running out of memory, we must resort to an "external" sorting algorithm that
/// uses the file system for scratch space; we use a parallel merge sort. Each [`Chunk`] is sorted separately, in parallel and
/// streamed to a pair of files. These files are consumed by a merging thread, which (also in parallel) iteratively merges pairs
/// of similarly-sized chunks.
///
/// # File Handles
///
/// **WARNING**: It's possible to exceed your system's limit on open file handles if [`Chunk`]s are too small.
///
/// # Memory Usage
///
/// **WARNING**: If you are running out of memory, make sure you can actually fit `max_sort_concurrency` [`Chunk`]s in memory.
/// Also note that [`std::env::temp_dir`] might actually be an in-memory overlay FS.
pub struct SortingPipeline<K, V> {
    unsorted_chunk_tx: Sender<Chunk<K, V>>,
    merge_initiator_thread_handle: thread::JoinHandle<Result<(), io::Error>>,
}

/// The files storing a sorted chunk of key-value pairs.
struct SortedChunkFiles {
    key_file: File,
    value_file: File,
    num_entries: usize,
}

impl SortedChunkFiles {
    fn new(
        mut key_file: File,
        mut value_file: File,
        num_entries: usize,
    ) -> Result<Self, io::Error> {
        key_file.seek(io::SeekFrom::Start(0))?;
        value_file.seek(io::SeekFrom::Start(0))?;
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

impl<K, V> SortingPipeline<K, V>
where
    K: Ord + Pod + Send,
    V: Pod + Send,
{
    /// - `max_sort_concurrency`: The maximum number of [`Chunk`]s that can be sorted (and persisted) concurrently.
    /// - `max_merge_concurrency`: The maximum number of [`Chunk`] *pairs* that can be merged concurrently.
    pub fn new(
        max_sort_concurrency: usize,
        max_merge_concurrency: usize,
        tmp_dir_path: impl AsRef<Path>,
        output_key_path: impl AsRef<Path>,
        output_value_path: impl AsRef<Path>,
    ) -> Self {
        assert!(max_sort_concurrency > 0);
        assert!(max_merge_concurrency > 0);

        let tmp_dir_path = tmp_dir_path.as_ref().to_owned();
        let output_key_path = output_key_path.as_ref().to_owned();
        let output_value_path = output_value_path.as_ref().to_owned();

        // No need for a long buffer of unsorted chunks, since it just uses more memory, and we don't have any low latency
        // requirements.
        let (unsorted_chunk_tx, unsorted_chunk_rx) = bounded(1);
        // Sorted chunks, however, shouldn't require any memory (they're persisted in temporary files). Since sorting is
        // CPU-bound and merging is IO-bound, we allow this buffer to get quite large so that sorting doesn't get blocked on IO.
        let (sorted_chunk_tx, sorted_chunk_rx) = bounded(1024);

        for _ in 0..max_sort_concurrency {
            let this_tmp_dir_path = tmp_dir_path.clone();
            let this_unsorted_chunk_rx = unsorted_chunk_rx.clone();
            let this_sorted_chunk_tx = sorted_chunk_tx.clone();
            thread::spawn(move || {
                run_sorter(
                    &this_tmp_dir_path,
                    this_unsorted_chunk_rx,
                    this_sorted_chunk_tx,
                )
            });
        }

        // Start a merger task to consume the sorted chunks and merge them in parallel.
        let merge_initiator_thread_handle = thread::spawn(move || {
            let result = run_merge_initiator::<K, V>(
                tmp_dir_path,
                &output_key_path,
                &output_value_path,
                max_merge_concurrency,
                sorted_chunk_rx,
            );
            if result.is_err() {
                log::error!("Merge initiator exited with: {result:?}");
                eprintln!("Merge initiator exited with: {result:?}");
            }
            result
        });

        Self {
            unsorted_chunk_tx,
            merge_initiator_thread_handle,
        }
    }

    /// Enqueues `chunk` for sorting.
    ///
    /// Sorting will occur automatically in the background. But sorting is only gauranteed to complete after calling `finish`.
    pub fn submit_unsorted_chunk(&self, chunk: Chunk<K, V>) {
        if chunk.is_empty() {
            return;
        }
        self.unsorted_chunk_tx.send(chunk).unwrap();
    }

    /// Finishes sorting, which ultimately creates sorted files at `output_key_path` and `output_value_path`.
    pub fn finish(self) -> Result<(), io::Error> {
        // Force disconnect from sorter threads so they can drain.
        drop(self.unsorted_chunk_tx);

        self.merge_initiator_thread_handle.join().unwrap()
    }
}

fn run_sorter<K, V>(
    tmp_dir_path: &Path,
    unsorted_chunk_rx: Receiver<Chunk<K, V>>,
    sorted_chunk_tx: Sender<Result<SortedChunkFiles, io::Error>>,
) where
    K: Ord + Pod,
    V: Pod,
{
    while let Ok(unsorted_chunk) = unsorted_chunk_rx.recv() {
        sorted_chunk_tx
            .send(sort_and_persist_chunk(tmp_dir_path, unsorted_chunk))
            .unwrap();
    }
}

fn sort_and_persist_chunk<K, V>(
    tmp_dir_path: &Path,
    mut chunk: Chunk<K, V>,
) -> Result<SortedChunkFiles, io::Error>
where
    K: Ord + Pod,
    V: Pod,
{
    let sort_span = tracing::info_span!("sort_chunk");
    sort_span.in_scope(|| chunk.sort());

    let num_entries = chunk.len();

    // Write the sorted output to temporary files.
    let persist_span = tracing::info_span!("persist_sorted_chunk");
    let _gaurd = persist_span.enter();
    let mut key_file = tempfile_in(tmp_dir_path)?;
    let mut value_file = tempfile_in(tmp_dir_path)?;
    {
        let mut key_writer = BufWriter::with_capacity(ONE_MIB, &mut key_file);
        let mut value_writer = BufWriter::with_capacity(ONE_MIB, &mut value_file);
        for (k, v) in chunk.entries.into_iter() {
            key_writer.write_all(bytes_of(&k))?;
            value_writer.write_all(bytes_of(&v))?;
        }
    }

    SortedChunkFiles::new(key_file, value_file, num_entries)
}

// This event loop handles incoming sorted chunk files and chooses which pairs of sorted chunks to merge by sending them to a
// "merger" thread. The merger thread also sends the result back to this thread, creating a cycling data path, so we need to be
// careful about liveness here.
fn run_merge_initiator<K, V>(
    tmp_dir_path: PathBuf,
    output_key_path: &Path,
    output_value_path: &Path,
    max_merge_concurrency: usize,
    sorted_chunk_rx: Receiver<Result<SortedChunkFiles, io::Error>>,
) -> Result<(), io::Error>
where
    K: Ord + Pod,
    V: Pod,
{
    // No need for a long buffer here, since we don't have any low latency requirements.
    let (chunk_pair_tx, chunk_pair_rx) = bounded(1);
    // Unbounded to avoid deadlock.
    let (merged_chunk_tx, merged_chunk_rx) = unbounded();

    for _ in 0..max_merge_concurrency {
        let this_tmp_dir_path = tmp_dir_path.clone();
        let this_chunk_pair_rx = chunk_pair_rx.clone();
        let this_merged_chunk_tx = merged_chunk_tx.clone();
        thread::spawn(move || {
            run_merger::<K, V>(&this_tmp_dir_path, this_chunk_pair_rx, this_merged_chunk_tx)
        });
    }

    let mut num_sorted_chunks_received = 0;

    let mut merge_queue = BinaryHeap::new();

    // This thread has to keep track of how many merge "tasks" are pending so it knows when to stop receiving.
    let mut num_merges_started = 0;
    // This is a useful metric to see how much redundant file writing we did.
    let mut num_merges_completed = 0;

    // In my dreams...
    // let num_pending_merges = || num_merges_started - num_merges_completed;

    // PERF: a select! statement might be more optimal, but it's harder to implement. We expect chunk sorting to be faster than
    // merging, so we don't expect it would make a huge difference

    // Handle newly sorted chunks until all sort workers disconnect.
    while let Ok(sorted_chunk_result) = sorted_chunk_rx.recv() {
        num_sorted_chunks_received += 1;
        log::debug!("# sorted chunks received = {num_sorted_chunks_received}");

        // Put it in the queue.
        merge_queue.push(sorted_chunk_result?);

        while num_merges_started - num_merges_completed < max_merge_concurrency
            && merge_queue.len() >= MERGE_K
        {
            let chunks: Vec<_> = (0..MERGE_K).filter_map(|_| merge_queue.pop()).collect();
            chunk_pair_tx.send(chunks).unwrap();
            num_merges_started += 1;
        }

        log::info!(
            "Merge queue length = {}, # pending merges = {}",
            merge_queue.len(),
            num_merges_started - num_merges_completed
        );

        // Check for completed merges without blocking.
        while let Ok(merged_chunk) = merged_chunk_rx.try_recv() {
            num_merges_completed += 1;
            merge_queue.push(merged_chunk?);
        }
    }
    // Sort workers disconnected.
    log::info!("All chunks sorted, only merge work remains");
    log::info!(
        "Merge queue length = {}, # pending merges = {}",
        merge_queue.len(),
        num_merges_started - num_merges_completed
    );

    // Aggressively merge remaining chunks until there are fewer than MERGE_K.
    while merge_queue.len() + num_merges_started - num_merges_completed > MERGE_K {
        // Find groups to merge.
        while merge_queue.len() >= MERGE_K {
            let chunks: Vec<_> = (0..MERGE_K).filter_map(|_| merge_queue.pop()).collect();
            chunk_pair_tx.send(chunks).unwrap();
            num_merges_started += 1;
        }

        log::info!(
            "Merge queue length = {}, # pending merges = {}",
            merge_queue.len(),
            num_merges_started - num_merges_completed
        );

        // Wait for a single merge to finish before checking if we can start more.
        if num_merges_started - num_merges_completed > 0 {
            let merged_chunk_result = merged_chunk_rx.recv().unwrap();
            num_merges_completed += 1;
            merge_queue.push(merged_chunk_result?);
        }
    }

    // Wait for all pending merges to finish.
    while num_merges_started - num_merges_completed > 0 {
        let merged_chunk_result = merged_chunk_rx.recv().unwrap();
        num_merges_completed += 1;
        merge_queue.push(merged_chunk_result?);
    }

    let mut output_key_file = File::create(output_key_path)?;
    let mut output_value_file = File::create(output_value_path)?;

    if merge_queue.is_empty() {
        return Ok(());
    }

    if merge_queue.len() == 1 {
        // Just copy the final chunk into the destination files. This should only happen if we are sorting a very small number
        // of chunks anyway.
        let mut final_chunk = merge_queue.pop().unwrap();
        io::copy(&mut final_chunk.key_file, &mut output_key_file)?;
        io::copy(&mut final_chunk.value_file, &mut output_value_file)?;
        return Ok(());
    }

    // Merge the final chunks into the output file.
    debug_assert!(merge_queue.len() <= MERGE_K);
    let chunks: Vec<_> = (0..MERGE_K).filter_map(|_| merge_queue.pop()).collect();
    let _ = merge_chunks::<K, V>(chunks, output_key_file, output_value_file)?;
    Ok(())
}

fn run_merger<K, V>(
    tmp_dir_path: &Path,
    chunk_pair_rx: Receiver<Vec<SortedChunkFiles>>,
    merged_chunk_tx: Sender<Result<SortedChunkFiles, io::Error>>,
) where
    K: Ord + Pod,
    V: Pod,
{
    while let Ok(chunks) = chunk_pair_rx.recv() {
        merged_chunk_tx
            .send(merge_chunks_into_tempfiles::<K, V>(tmp_dir_path, chunks))
            .unwrap();
    }
}

fn merge_chunks_into_tempfiles<K, V>(
    tmp_dir_path: &Path,
    chunks: Vec<SortedChunkFiles>,
) -> Result<SortedChunkFiles, io::Error>
where
    K: Ord + Pod,
    V: Pod,
{
    merge_chunks::<K, V>(
        chunks,
        tempfile_in(tmp_dir_path)?,
        tempfile_in(tmp_dir_path)?,
    )
}

fn merge_chunks<K, V>(
    chunks: Vec<SortedChunkFiles>,
    mut merged_key_file: File,
    mut merged_value_file: File,
) -> Result<SortedChunkFiles, io::Error>
where
    K: Ord + Pod,
    V: Pod,
{
    let span = tracing::info_span!("merge_two_persisted_chunks");
    let _guard = span.enter();

    let sum_entries = chunks.iter().map(|chunk| chunk.num_entries).sum();

    // Merge the files without reading their entire contents into memory.
    let mut readers: Vec<_> = chunks
        .into_iter()
        .map(|chunk| {
            (
                BufReader::with_capacity(ONE_MIB, chunk.key_file),
                BufReader::with_capacity(ONE_MIB, chunk.value_file),
            )
        })
        .collect();

    let mut key_writer = BufWriter::with_capacity(ONE_MIB, &mut merged_key_file);
    let mut value_writer = BufWriter::with_capacity(ONE_MIB, &mut merged_value_file);

    // Initialize the first key from each file. This asserts that each chunk is not empty.
    let mut key_heap = BinaryHeap::with_capacity(readers.len());
    for (i, reader) in readers.iter_mut().enumerate() {
        let mut key = K::zeroed();
        debug_assert!(read_element(&mut reader.0, &mut key)?);
        key_heap.push((Reverse(key), i));
    }

    while let Some((key, chunk_index)) = key_heap.pop() {
        let reader = &mut readers[chunk_index];
        let mut value = V::zeroed();
        write_element(&mut key_writer, &key.0)?;
        debug_assert!(read_element(&mut reader.1, &mut value)?);
        write_element(&mut value_writer, &value)?;
        let mut next_key = K::zeroed();
        if read_element(&mut reader.0, &mut next_key)? {
            key_heap.push((Reverse(next_key), chunk_index));
        }
    }
    drop(key_writer);
    drop(value_writer);

    SortedChunkFiles::new(merged_key_file, merged_value_file, sum_entries)
}

fn write_element<T>(writer: &mut BufWriter<&mut File>, value: &T) -> Result<(), io::Error>
where
    T: Pod,
{
    writer.write_all(bytes_of(value))
}

fn read_element<T>(reader: &mut BufReader<File>, value: &mut T) -> Result<bool, io::Error>
where
    T: Pod,
{
    if let Err(e) = reader.read_exact(bytes_of_mut(value)) {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Ok(false)
        } else {
            Err(e)
        }
    } else {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use bytemuck::cast_slice;
    use tempfile::tempdir;

    use super::*;

    // For sake of functionality, make sure concurrency is not necessary for liveness. We rely on benchmarks for concurrency
    // testing.
    const MAX_CONCURRENCY: usize = 1;

    fn sorting_pipeline_test<K, V>(
        add_entries: impl FnOnce(&mut SortingPipeline<K, V>),
        expected_keys: &[K],
        expected_values: &[V],
    ) where
        K: Debug + Ord + Pod + Send,
        V: Debug + PartialEq + Pod + Send,
    {
        let dir = tempdir().unwrap();
        let output_key_path = dir.path().join("keys");
        let output_value_path = dir.path().join("values");
        let mut pipeline = SortingPipeline::new(
            MAX_CONCURRENCY,
            MAX_CONCURRENCY,
            std::env::temp_dir(),
            &output_key_path,
            &output_value_path,
        );

        add_entries(&mut pipeline);
        pipeline.finish().unwrap();

        assert_file_elements_eq(output_key_path, expected_keys);
        assert_file_elements_eq(output_value_path, expected_values);
    }

    fn assert_file_elements_eq<T>(path: impl AsRef<Path>, expected: &[T])
    where
        T: Debug + Pod + PartialEq,
    {
        let mut f = File::open(path).unwrap();
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes).unwrap();
        let actual: &[T] = cast_slice(&bytes);
        assert_eq!(actual, expected);
    }

    #[test]
    fn sort_empty() {
        let expected_keys: &[i32] = &[];
        let expected_values: &[i32] = &[];
        sorting_pipeline_test(|_pipeline| {}, expected_keys, expected_values);
    }

    #[test]
    fn sort_single() {
        let expected_keys: &[i32] = &[0];
        let expected_values: &[i32] = &[1];
        sorting_pipeline_test(
            |pipeline| {
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(0, 1)]));
            },
            expected_keys,
            expected_values,
        );
    }

    #[test]
    fn sort_two_in_order_one_chunk() {
        let expected_keys: &[i32] = &[0, 1];
        let expected_values: &[i32] = &[1, 2];
        sorting_pipeline_test(
            |pipeline| {
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(0, 1), (1, 2)]));
            },
            expected_keys,
            expected_values,
        );
    }

    #[test]
    fn sort_two_out_of_order_one_chunk() {
        let expected_keys: &[i32] = &[0, 1];
        let expected_values: &[i32] = &[1, 2];
        sorting_pipeline_test(
            |pipeline| {
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(1, 2), (0, 1)]));
            },
            expected_keys,
            expected_values,
        );
    }

    #[test]
    fn sort_two_singleton_chunks() {
        let expected_keys: &[i32] = &[0, 1];
        let expected_values: &[i32] = &[1, 2];
        sorting_pipeline_test(
            |pipeline| {
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(1, 2)]));
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(0, 1)]));
            },
            expected_keys,
            expected_values,
        );
    }

    #[test]
    fn sort_more_than_two_chunks() {
        let expected_keys: &[i32] = &[0, 1, 2, 3, 4];
        let expected_values: &[i32] = &[1, 2, 4, 8, 16];
        sorting_pipeline_test(
            |pipeline| {
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(1, 2), (0, 1)]));
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(3, 8), (4, 16)]));
                pipeline.submit_unsorted_chunk(Chunk::new(vec![(2, 4)]));
            },
            expected_keys,
            expected_values,
        );
    }
}
