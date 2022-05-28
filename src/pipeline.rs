use crate::chunk::*;
use crate::merge::*;

use bytemuck::{bytes_of, Pod};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::thread;
use tempfile::tempfile_in;

const ONE_MIB: usize = 1 << 20;

// PERF: for *large* values, would it be faster to do the entire external sort on (key, value *ID*) pairs first, then use the
// sorted value IDs to swap around the values in one large value file?

/// Accepts an arbitrarily large stream of unordered key-value pairs (AKA entries) from the user and streams them into two
/// binary files: one for keys and one for values.
pub struct SortingPipeline<K, V> {
    unsorted_chunk_tx: Sender<Chunk<K, V>>,
    merge_initiator_thread_handle: thread::JoinHandle<Result<(), io::Error>>,
}

/// Performance-tuning parameters for [`SortingPipeline`].
pub struct Config {
    /// The maximum number of [`Chunk`]s that can be sorted (and persisted) concurrently.
    pub max_sort_concurrency: usize,
    /// The maximum number of merge operations that can occur concurrently.
    pub max_merge_concurrency: usize,
    /// The maximum number of sorted chunks that can participate in a merge operation.
    pub merge_k: usize,
}

impl<K, V> SortingPipeline<K, V>
where
    K: Ord + Pod + Send,
    V: Pod + Send,
{
    pub fn new(
        config: Config,
        tmp_dir_path: impl AsRef<Path>,
        output_key_path: impl AsRef<Path>,
        output_value_path: impl AsRef<Path>,
    ) -> Self {
        assert!(config.max_sort_concurrency > 0);
        assert!(config.max_merge_concurrency > 0);
        assert!(config.merge_k > 1);

        let tmp_dir_path = tmp_dir_path.as_ref().to_owned();
        let output_key_path = output_key_path.as_ref().to_owned();
        let output_value_path = output_value_path.as_ref().to_owned();

        // No need for a long buffer of unsorted chunks, since it just uses more memory, and we don't have any low latency
        // requirements.
        let (unsorted_chunk_tx, unsorted_chunk_rx) = bounded(1);
        // Sorted chunks, however, shouldn't require any memory (they're persisted in temporary files). Since sorting is
        // CPU-bound and merging is IO-bound, we allow this buffer to get quite large so that sorting doesn't get blocked on IO.
        let (sorted_chunk_tx, sorted_chunk_rx) = bounded(1024);

        for _ in 0..config.max_sort_concurrency {
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
                config.max_merge_concurrency,
                config.merge_k,
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
    /// Sorting will occur automatically in the background. But sorting is only guaranteed to complete after calling `finish`.
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
            .send(merge_chunks_into_tempfiles::<K, V>(chunks, tmp_dir_path))
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
    let mut key_writer = BufWriter::with_capacity(ONE_MIB, tempfile_in(tmp_dir_path)?);
    let mut value_writer = BufWriter::with_capacity(ONE_MIB, tempfile_in(tmp_dir_path)?);
    for (k, v) in chunk.entries.into_iter() {
        key_writer.write_all(bytes_of(&k))?;
        value_writer.write_all(bytes_of(&v))?;
    }

    SortedChunkFiles::new(
        key_writer.into_inner()?,
        value_writer.into_inner()?,
        num_entries,
    )
}

// This event loop handles incoming sorted chunk files and chooses which groups of sorted chunks to merge by sending them to a
// "merger" thread. The merger thread also sends the result back to this thread, creating a cycling data path, so we need to be
// careful about liveness here.
fn run_merge_initiator<K, V>(
    tmp_dir_path: PathBuf,
    output_key_path: &Path,
    output_value_path: &Path,
    max_merge_concurrency: usize,
    merge_k: usize,
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

    let mut merge_queue = BinaryHeap::new();

    let mut num_sorted_chunks_received = 0;
    // This thread has to keep track of how many merge "tasks" are pending so it knows when to stop receiving.
    let mut num_merges_started = 0;
    // This is a useful metric to see how much redundant file writing we did.
    let mut num_merges_completed = 0;
    macro_rules! num_pending_merges {
        () => {
            num_merges_started - num_merges_completed
        };
    }

    // Handle newly sorted chunks until all sort workers disconnect.
    while let Ok(sorted_chunk_result) = sorted_chunk_rx.recv() {
        num_sorted_chunks_received += 1;
        log::debug!("# sorted chunks received = {num_sorted_chunks_received}");

        // Put it in the queue.
        merge_queue.push(sorted_chunk_result?);

        while num_pending_merges!() < max_merge_concurrency && merge_queue.len() >= merge_k {
            let chunks: Vec<_> = (0..merge_k).filter_map(|_| merge_queue.pop()).collect();
            chunk_pair_tx.send(chunks).unwrap();
            num_merges_started += 1;
        }

        log::info!(
            "Merge queue length = {}, # pending merges = {}",
            merge_queue.len(),
            num_pending_merges!()
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
        num_pending_merges!()
    );

    // Aggressively merge remaining chunks until there are at most merge_k.
    while merge_queue.len() + num_pending_merges!() > merge_k {
        // Find groups to merge.
        while merge_queue.len() >= merge_k {
            let chunks: Vec<_> = (0..merge_k).filter_map(|_| merge_queue.pop()).collect();
            chunk_pair_tx.send(chunks).unwrap();
            num_merges_started += 1;
        }

        log::info!(
            "Merge queue length = {}, # pending merges = {}",
            merge_queue.len(),
            num_pending_merges!()
        );

        // Wait for a single merge to finish before checking if we can start more.
        if num_pending_merges!() > 0 {
            let merged_chunk_result = merged_chunk_rx.recv().unwrap();
            num_merges_completed += 1;
            merge_queue.push(merged_chunk_result?);
        }
    }

    // Wait for all pending merges to finish.
    while num_pending_merges!() > 0 {
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
        log::info!("Only one chunk: just copying to output location");
        let mut final_chunk = merge_queue.pop().unwrap();
        io::copy(&mut final_chunk.key_file, &mut output_key_file)?;
        io::copy(&mut final_chunk.value_file, &mut output_value_file)?;
        return Ok(());
    }

    // Merge the final chunks into the output file.
    assert!(merge_queue.len() <= merge_k);
    let chunks: Vec<_> = (0..merge_k).filter_map(|_| merge_queue.pop()).collect();
    let _ = merge_chunks::<K, V>(chunks, output_key_file, output_value_file)?;
    num_merges_completed += 1;

    log::info!("Done merging! Performed {num_merges_completed} merge(s) total");
    Ok(())
}
