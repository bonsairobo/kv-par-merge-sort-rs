use clap::Parser;
use kv_par_merge_sort::{chunk_max_entries_from_memory_limit, Config, SortingPipeline};
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
    #[clap(short = 'n', long, default_value = "500000000")]
    num_entries: usize,
}

const CONFIG: Config = Config {
    max_sort_concurrency: 16,
    max_merge_concurrency: 8,
    merge_k: 16,
};
type K = [u8; 12];
type V = [u8; 24];

// Make sure to use at most 16 GiB of memory.
const SIXTEEN_GIB: usize = 16 * (1 << 30);

fn main() {
    let args = Args::parse();

    env_logger::init();
    // let (chrome_layer, _guard) = ChromeLayerBuilder::new().build();
    // tracing_subscriber::registry().with(chrome_layer).init();

    let output_key_path = args.output_dir.join("keys.bin");
    let output_value_path = args.output_dir.join("values.bin");

    let pipeline =
        SortingPipeline::<K, V>::new(CONFIG, args.temp_dir, &output_key_path, &output_value_path);

    let max_entries_per_chunk =
        chunk_max_entries_from_memory_limit::<K, V>(SIXTEEN_GIB, CONFIG.max_sort_concurrency);

    let mut rng = SmallRng::from_entropy();
    let num_chunks = (args.num_entries + max_entries_per_chunk - 1) / max_entries_per_chunk;
    log::info!(
        "Random input data set will contain {num_chunks} unsorted chunks \
        of at most {max_entries_per_chunk} entries each"
    );
    let mut num_submitted = 0;
    let mut num_entries_remaining = args.num_entries;
    while num_entries_remaining > 0 {
        let chunk_size = max_entries_per_chunk.min(num_entries_remaining);
        let chunk = (0..chunk_size).map(|_| rng.gen()).collect();
        pipeline.submit_unsorted_chunk(chunk);
        num_entries_remaining -= chunk_size;
        num_submitted += 1;
        log::debug!("# unsorted chunks submitted = {num_submitted}");
    }
    log::info!("Done generating random chunks");

    pipeline.finish().unwrap();
}
