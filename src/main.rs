mod args;
use std::time::Instant;
use std::{collections::HashMap, os::unix::fs::OpenOptionsExt};

use crate::{args::get_args, block::Chunk};

mod device;
use device::get_dev_size;

mod block;

use indicatif::ProgressBar;
use log::{info, trace};
use tokio_uring::fs::{File, OpenOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let now = Instant::now();

    // get arguments
    let args = get_args()?;

    let mut dedup = HashMap::<u64, u64>::new();

    tokio_uring::start(async {
        // Open a file
        let src: File = File::open(&args.dev).await?;
        // let src = OpenOptions::new()
        //     .read(true)
        //     .custom_flags(libc::O_DIRECT)
        //     .open(&args.dev)
        //     .await?;

        let dst = File::create(&args.output).await?;

        let devsize = get_dev_size(&src)?;
        let bar = ProgressBar::new(devsize);

        // Buffers
        let mut read_offset = 0u64;
        let mut write_offset = 0u64;

        loop {
            let buf = Vec::with_capacity(args.block_size());

            // Asynchronously read a chunk
            let (res, buf) = src.read_at(buf, read_offset).await;
            let n = res?;

            // EOF ?
            if n == 0 {
                break;
            }

            // create the chunk metadata with bytes we just read
            let bytes = &buf[..n];
            let chunk = Chunk::from(bytes);
            trace!("chunk: {:?}", chunk);

            if dedup.contains_key(&chunk.hash) {
                info!("hash {} is already in dedup", chunk.hash);
            } else {
                dedup.insert(chunk.hash, read_offset);
            }

            // Optional: compute SHA256 hash of this chunk
            // let mut hasher = Sha256::new();
            // hasher.update(chunk);
            // let hash = hasher.finalize();
            // println!("Chunk at offset {} hash: {}", offset, hex::encode(hash));

            // asynchronously write chunk to destination
            let res = chunk.write_at(&dst, &mut write_offset, args.dd).await;
            //let (res, _) = dst.write_all_at(bytes.to_vec(), read_offset).await;
            res?;

            read_offset += n as u64;
            bar.inc(n as u64);

            trace!("read offset: {read_offset} write offset: {write_offset}")
        }

        bar.finish();

        //───────────────────────────────────────────────────────────────────────────────────
        // elapsed time
        //───────────────────────────────────────────────────────────────────────────────────
        let elapsed = now.elapsed();
        println!("took {} millis", elapsed.as_millis());

        println!("keys={}", dedup.len());

        Ok(())
    })
}
