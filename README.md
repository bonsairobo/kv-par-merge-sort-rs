# kv-par-merge-sort

## Key-Value Parallel Merge Sort

Sort [`Pod`] (key, value) data sets that don't fit in memory.

This crate provides the `kv_par_merge_sort` library, which enables the user to sort [`Chunk`]s of (key, value) pairs (AKA
entries) via a [`SortingPipeline`]. The sorted output lands in two files: one for keys and one for values. The keys file is
sorted, while the values file is "parallel" to the key file.

More precisely, we denote the input stream as `[(k[0], v[0]), (k[1], v[1]), ...]`. The final key and value files are laid
out as `[k[a], k[b], ...]` and `[v[a], v[b], ...]` respectively, such that the key array is sorted. The reason for separate
files is to ensure correct data type alignment (for zero-copy reads) without wasting space to padding.

### Sorting ~17 GB data set (half a billion entries)

```sh
$ time RUST_LOG=debug cargo run --release --example large_data_set -- -o /run/media/duncan/ssd_data/bench_data/ -t /run/media/duncan/ssd_data/tmp/
[2022-05-28T08:24:56Z INFO  large_data_set] Random input data set will contain 18 unsorted chunks of at most 28071681 entries each
[2022-05-28T08:25:36Z INFO  large_data_set] Done generating random chunks
[2022-05-28T08:26:00Z INFO  kv_par_merge_sort] Running merge of 16 persisted chunks
[2022-05-28T08:26:01Z INFO  kv_par_merge_sort] All chunks sorted, only merge work remains
[2022-05-28T08:27:02Z INFO  kv_par_merge_sort] Running merge of 3 persisted chunks
[2022-05-28T08:28:30Z INFO  kv_par_merge_sort] Done merging! Performed 2 merge(s) total

real    3m33.830s
user    3m31.733s
sys     0m42.923s
```

### Implementation

To sort an arbitrarily large data set without running out of memory, we must resort to an "external" sorting algorithm that
uses the file system for scratch space; we use a parallel merge sort. Each [`Chunk`] is sorted separately, in parallel and
streamed to a pair of files. These files are consumed by a merging thread, which (also in parallel) iteratively merges
groups of up to `merge_k` similarly-sized chunks.

### File Handles

**WARNING**: It's possible to exceed your system's limit on open file handles if [`Chunk`]s are too small.

### Memory Usage

**WARNING**: If you are running out of memory, make sure you can actually fit `max_sort_concurrency` [`Chunk`]s in memory.
Also note that [`std::env::temp_dir`] might actually be an in-memory overlay FS.

### File System Usage

This algorithm requires twice the size of the input data in free file system space in order to perform the final merge.

License: MIT OR Apache-2.0
