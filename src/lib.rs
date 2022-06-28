//! # Key-Value Parallel Merge Sort
//!
//! Sort [`Pod`](bytemuck::Pod) (key, value) data sets that don't fit in memory.
//!
//! This crate provides the `kv_par_merge_sort` library, which enables the user to sort chunks of (key, value) pairs (AKA
//! entries) via a [`SortingPipeline`]. The sorted output lands in two files: one for keys and one for values. The keys file is
//! sorted, while the values file is "parallel" to the key file.
//!
//! More precisely, we denote the input stream as `[(k[0], v[0]), (k[1], v[1]), ...]`. The final key and value files are laid
//! out as `[k[a], k[b], ...]` and `[v[a], v[b], ...]` respectively, such that the key array is sorted. The reason for separate
//! files is to ensure correct data type alignment (for zero-copy reads) without wasting space to padding.
//!
//! ## Sorting ~17 GB data set (half a billion entries)
//!
//! ```sh
//! $ time RUST_LOG=debug cargo run --release --example large_data_set -- -o /ssd_data/bench_data/ -t /ssd_data/tmp/
//! [2022-05-28T08:24:56Z INFO  large_data_set] Random input data set will contain 18 unsorted chunks of at most 28071681 entries each
//! [2022-05-28T08:25:36Z INFO  large_data_set] Done generating random chunks
//! [2022-05-28T08:26:00Z INFO  kv_par_merge_sort] Running merge of 16 persisted chunks
//! [2022-05-28T08:26:01Z INFO  kv_par_merge_sort] All chunks sorted, only merge work remains
//! [2022-05-28T08:27:02Z INFO  kv_par_merge_sort] Running merge of 3 persisted chunks
//! [2022-05-28T08:28:30Z INFO  kv_par_merge_sort] Done merging! Performed 2 merge(s) total
//!
//! real    3m33.830s
//! user    3m31.733s
//! sys     0m42.923s
//! ```
//!
//! ## Implementation
//!
//! To sort an arbitrarily large data set without running out of memory, we must resort to an "external" sorting algorithm that
//! uses the file system for scratch space; we use a parallel merge sort. Each chunk is sorted separately, in parallel and
//! streamed to a pair of files. These files are consumed by a merging thread, which (also in parallel) iteratively merges
//! groups of up to `merge_k` similarly-sized chunks.
//!
//! ## File Handles
//!
//! **WARNING**: It's possible to exceed your system's limit on open file handles if chunks are too small.
//!
//! ## Memory Usage
//!
//! **WARNING**: If you are running out of memory, make sure you can actually fit `max_sort_concurrency` chunks in memory. Also
//! note that [`std::env::temp_dir`] might actually be an in-memory `tmpfs`.
//!
//! ## File System Usage
//!
//! This algorithm requires twice the size of the input data in free file system space in order to perform the final merge.

mod chunk;
mod merge;
mod pipeline;

pub use pipeline::*;

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use bytemuck::cast_slice;
    use bytemuck::Pod;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;
    use tempfile::tempdir;

    use super::*;

    // For sake of functionality, make sure concurrency is not necessary for liveness. We rely on benchmarks for concurrency
    // testing.
    const CONFIG: Config = Config {
        max_sort_concurrency: 1,
        max_merge_concurrency: 1,
        merge_k: 2,
    };

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
            CONFIG,
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
                pipeline.submit_unsorted_chunk(vec![(0, 1)]);
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
                pipeline.submit_unsorted_chunk(vec![(0, 1), (1, 2)]);
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
                pipeline.submit_unsorted_chunk(vec![(1, 2), (0, 1)]);
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
                pipeline.submit_unsorted_chunk(vec![(1, 2)]);
                pipeline.submit_unsorted_chunk(vec![(0, 1)]);
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
                pipeline.submit_unsorted_chunk(vec![(1, 2), (0, 1)]);
                pipeline.submit_unsorted_chunk(vec![(3, 8), (4, 16)]);
                pipeline.submit_unsorted_chunk(vec![(2, 4)]);
            },
            expected_keys,
            expected_values,
        );
    }
}
