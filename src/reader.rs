use std::{
    path::{Path, PathBuf},
    sync::{Arc, mpsc::Sender},
};

use indicatif::ProgressBar;
use log::trace;
use tokio_uring::fs::File;

use crate::buffer::Buffer;

// a context contains all what is necessary to apply a specific pattern
// when reading blocks using multiple threads
#[derive(Debug)]
pub struct Context {
    // number of threads
    pub nb_threads: usize,

    // thread ID
    pub thread_id: usize,

    // block size passed from arguments
    pub block_size: usize,

    // progress bar shared between threads
    pub pbar: Arc<ProgressBar>,

    // send part of the channel
    pub tx: Sender<(usize, Vec<u8>)>,

    // function giving the block to read
    pub pattern_func: fn(nb_threads: usize, thread_id: usize, k: usize) -> usize,
}

// reader is called by each thread
pub fn read_par(ctx: Context, path: PathBuf) -> anyhow::Result<()> {
    tokio_uring::start(async {
        // Open a file
        let src = File::open(path).await?;
        //let dst = File::create(&args.of).await?;

        // let devsize = get_dev_size(&src)?;
        // let bar = ProgressBar::new(devsize);

        // index of block
        let mut k = 0usize;

        // Buffers
        let mut read_offset = 0u64;
        let mut write_offset = 0u64;

        loop {
            let buf = Buffer::with_capacity(ctx.block_size);
            let buffer = vec![0; ctx.block_size];

            // offset is given by the pattern func
            let block_index = (ctx.pattern_func)(ctx.nb_threads, ctx.thread_id, k);
            let offset = block_index * ctx.block_size;

            // Asynchronously read a chunk
            let (res, buffer) = src.read_at(buffer, offset as u64).await;
            let bytes_read = res?;

            // EOF ?
            if bytes_read == 0 {
                break;
            }

            // send data we read
            ctx.tx.send((block_index, buffer[..bytes_read].to_vec()))?;

            trace!(
                "thread ID: {} read {bytes_read} bytes at offset {offset}",
                ctx.thread_id
            );

            k += 1;

            ctx.pbar.inc(bytes_read as u64);

            // // create the chunk metadata with bytes we just read
            // let bytes = &buf[..n];
            // let chunk = Chunk::try_from((bytes, args.dd, args.compress))?;
            // trace!("chunk: {:?}", chunk);

            // // Optional: compute SHA256 hash of this chunk
            // // let mut hasher = Sha256::new();
            // // hasher.update(chunk);
            // // let hash = hasher.finalize();
            // // println!("Chunk at offset {} hash: {}", offset, hex::encode(hash));

            // // asynchronously write chunk to destination

            // let res = chunk.write_at(&dst, &mut write_offset, args.dd).await;
            // res?;

            // read_offset += n as u64;
            // bar.inc(n as u64);

            // trace!("read offset: {read_offset} write offset: {write_offset}")
        }

        Ok(())
    })
}

// // the indicates how block are read: round-robin, contiguously, etc
// struct ReadPattern;

// impl ReadPattern {
//     // round robin is best suited for NVMe
//     //
//     // Thread 0 → B0, B4, B8, ...
//     // Thread 1 → B1, B5, B9, ...
//     // Thread 2 → B2, B6, B10...
//     // Thread 3 → B3, B7, B11...
//     round_robin
// }
