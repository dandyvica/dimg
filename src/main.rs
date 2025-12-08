mod args;
use std::time::Instant;
use std::{collections::HashMap, os::unix::fs::OpenOptionsExt};

use crate::buffer::{Buffer, is_zero};
use crate::{args::get_args, chunk::Chunk};

mod device;
use anyhow::Ok;
use device::get_dev_size;

mod chunk;

mod buffer;
mod hash;

use indicatif::ProgressBar;
use log::{info, trace};
use tokio_uring::fs::{File, OpenOptions};

fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    // get arguments
    let args = get_args()?;
    info!("args: {:?}", args);

    // if this option is set, try to deduplicate only
    // this will keep which blocks are only 0
    let mut zero_dup = Vec::<u64>::new();

    // key is hash, value is offset
    let mut dedup = HashMap::<u128, u64>::new();
    let mut dont_write = false;

    tokio_uring::start(async {
        // Open a file
        let src: File = File::open(&args.r#if).await?;
        let dst = File::create(&args.of).await?;

        let devsize = get_dev_size(&src)?;
        let bar = ProgressBar::new(devsize);

        // Buffers
        let mut read_offset = 0u64;
        let mut write_offset = 0u64;

        loop {
            //let buf = Vec::with_capacity(args.block_size());
            let buf = Buffer::with_capacity(args.block_size());

            // Asynchronously read a chunk
            let (res, buf) = src.read_at(Into::<Vec<u8>>::into(buf), read_offset).await;
            let n = res?;

            // EOF ?
            if n == 0 {
                break;
            }

            // create the chunk metadata with bytes we just read
            let bytes = &buf[..n];
            let chunk = Chunk::try_from((bytes, args.dd, args.compress))?;
            trace!("chunk: {:?}", chunk);

            // Optional: compute SHA256 hash of this chunk
            // let mut hasher = Sha256::new();
            // hasher.update(chunk);
            // let hash = hasher.finalize();
            // println!("Chunk at offset {} hash: {}", offset, hex::encode(hash));

            // asynchronously write chunk to destination

            let res = chunk.write_at(&dst, &mut write_offset, args.dd).await;
            res?;

            read_offset += n as u64;
            bar.inc(n as u64);

            trace!("read offset: {read_offset} write offset: {write_offset}")
        }

        bar.finish();

        src.close().await?;
        dst.close().await?;

        //───────────────────────────────────────────────────────────────────────────────────
        // elapsed time
        //───────────────────────────────────────────────────────────────────────────────────
        let elapsed = now.elapsed();
        println!("took {} millis", elapsed.as_millis());

        println!("keys={}", dedup.len());

        Ok(())
    })?;

    println!("#v = {}", zero_dup.len());
    Ok(())
}
