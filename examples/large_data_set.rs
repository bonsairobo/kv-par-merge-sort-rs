use kv_par_merge_sort::{Chunk, SortingPipeline};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::path::PathBuf;
// use tracing_chrome::ChromeLayerBuilder;
// use tracing_subscriber::prelude::*;

const MAX_SORT_CONCURRENCY: usize = 16;
const MAX_MERGE_CONCURRENCY: usize = 8;
const MAX_ENTRIES_PER_CHUNK: usize = 100_663_296;

fn main() {
    // Make sure the system has enough memory to support these parameters.
    const SIXTEEN_GB: usize = 16 * (1 << 30);
    type K = u32;
    type V = u32;
    let entry_size = std::mem::size_of::<K>() + std::mem::size_of::<V>();
    let chunk_size = MAX_ENTRIES_PER_CHUNK * entry_size;
    let total_size = chunk_size * (MAX_SORT_CONCURRENCY + 1);
    assert!(total_size < SIXTEEN_GB, "{total_size} > {SIXTEEN_GB}");

    env_logger::init();
    // let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
    // tracing_subscriber::registry().with(chrome_layer).init();

    let num_entries = 5_000_000_000;

    let temp_dir = "/run/media/duncan/ssd_data/tmp";
    let dir = PathBuf::from("/run/media/duncan/ssd_data/bench_data");
    // let temp_dir = std::env::temp_dir();
    // let temp_dir = "/home/duncan/tmp";
    // let dir = PathBuf::from("/home/duncan/bench_data");
    let output_key_path = dir.join("keys");
    let output_value_path = dir.join("values");

    let pipeline = SortingPipeline::<K, V>::new(
        MAX_SORT_CONCURRENCY,
        MAX_MERGE_CONCURRENCY,
        temp_dir,
        &output_key_path,
        &output_value_path,
    );

    let mut rng = SmallRng::from_entropy();
    let num_chunks = num_entries / MAX_ENTRIES_PER_CHUNK;
    log::info!("Program will generate {num_chunks} unsorted chunks");
    let mut num_submitted = 0;
    for _ in 0..num_chunks {
        let chunk_data = (0..MAX_ENTRIES_PER_CHUNK).map(|_| rng.gen()).collect();
        pipeline.submit_unsorted_chunk(Chunk::new(chunk_data));
        num_submitted += 1;
        log::debug!("# unsorted chunks submitted = {num_submitted}");
    }
    log::info!("Done generating random chunks");

    pipeline.finish().unwrap();
}
