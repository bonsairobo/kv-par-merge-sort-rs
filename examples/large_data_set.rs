use clap::Parser;
use kv_par_merge_sort::{Chunk, SortingPipeline};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::path::PathBuf;
// use tracing_chrome::ChromeLayerBuilder;
// use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short = 'o', long)]
    output_dir: PathBuf,
    #[clap(short = 't', long)]
    temp_dir: PathBuf,
}

const MAX_SORT_CONCURRENCY: usize = 16;
const MAX_MERGE_CONCURRENCY: usize = 8;
const MERGE_K: usize = 16;
const MAX_ENTRIES_PER_CHUNK: usize = 100_663_296;

fn main() {
    let args = Args::parse();

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

    let num_entries = 1_000_000_000;

    let output_key_path = args.output_dir.join("keys.bin");
    let output_value_path = args.output_dir.join("values.bin");

    let pipeline = SortingPipeline::<K, V>::new(
        MAX_SORT_CONCURRENCY,
        MAX_MERGE_CONCURRENCY,
        MERGE_K,
        args.temp_dir,
        &output_key_path,
        &output_value_path,
    );

    let mut rng = SmallRng::from_entropy();
    let num_chunks = (num_entries + MAX_ENTRIES_PER_CHUNK - 1) / MAX_ENTRIES_PER_CHUNK;
    log::info!(
        "Random input data set will contain {num_chunks} unsorted chunks \
        of at most {MAX_ENTRIES_PER_CHUNK} entries each"
    );
    let mut num_submitted = 0;
    let mut num_entries_remaining = num_entries;
    while num_entries_remaining > 0 {
        let chunk_size = MAX_ENTRIES_PER_CHUNK.min(num_entries_remaining);
        let chunk_data = (0..chunk_size).map(|_| rng.gen()).collect();
        pipeline.submit_unsorted_chunk(Chunk::new(chunk_data));
        num_entries_remaining -= chunk_size;
        num_submitted += 1;
        log::debug!("# unsorted chunks submitted = {num_submitted}");
    }
    log::info!("Done generating random chunks");

    pipeline.finish().unwrap();
}
