use crate::chunk::SortedChunkFiles;

use bytemuck::{bytes_of, bytes_of_mut, Pod};
use core::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter};
use std::path::Path;
use tempfile::tempfile_in;

const ONE_MIB: usize = 1 << 20;

pub fn merge_chunks_into_tempfiles<K, V>(
    chunks: Vec<SortedChunkFiles>,
    tmp_dir_path: &Path,
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

/// Merges any number of [`SortedChunkFiles`]
pub fn merge_chunks<K, V>(
    chunks: Vec<SortedChunkFiles>,
    merged_key_file: File,
    merged_value_file: File,
) -> Result<SortedChunkFiles, io::Error>
where
    K: Ord + Pod,
    V: Pod,
{
    log::info!("Running merge of {} persisted chunks", chunks.len());

    let span = tracing::info_span!("merge_persisted_chunks");
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

    let mut key_writer = BufWriter::with_capacity(ONE_MIB, merged_key_file);
    let mut value_writer = BufWriter::with_capacity(ONE_MIB, merged_value_file);

    // Initialize the first key from each file. This asserts that each chunk is not empty.
    let mut key_heap = BinaryHeap::with_capacity(readers.len());
    for (i, (key_reader, _)) in readers.iter_mut().enumerate() {
        let mut key = K::zeroed();
        assert!(read_element(key_reader, &mut key)?);
        key_heap.push((Reverse(key), i));
    }

    while let Some((key, chunk_index)) = key_heap.pop() {
        let (key_reader, value_reader) = &mut readers[chunk_index];
        write_element(&mut key_writer, &key.0)?;
        let mut value = V::zeroed();
        assert!(read_element(value_reader, &mut value)?);
        write_element(&mut value_writer, &value)?;
        let mut next_key = K::zeroed();
        if read_element(key_reader, &mut next_key)? {
            key_heap.push((Reverse(next_key), chunk_index));
        }
    }

    SortedChunkFiles::new(
        key_writer.into_inner()?,
        value_writer.into_inner()?,
        sum_entries,
    )
}

fn write_element<T>(mut writer: impl io::Write, value: &T) -> Result<(), io::Error>
where
    T: Pod,
{
    writer.write_all(bytes_of(value))
}

fn read_element<T>(mut reader: impl io::Read, value: &mut T) -> Result<bool, io::Error>
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
