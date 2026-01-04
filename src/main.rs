mod args;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;
use std::{collections::HashMap, os::unix::fs::OpenOptionsExt};

use crate::buffer::{Buffer, is_zero};
use crate::reader::{Context, read_par};
use crate::writer::writer_thread;
use crate::{args::get_args, chunk::Chunk};

mod device;
use anyhow::Ok;
use device::input_size;

mod buffer;
mod chunk;
mod hash;
mod reader;
mod writer;

use indicatif::ProgressBar;
use log::{info, trace};
use tokio_uring::fs::{File, OpenOptions};

fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    // get arguments
    let args = get_args()?;
    info!("args: {:?}", args);

    // get device size
    let devsize = input_size(&args.r#if)?;
    let pbar = Arc::new(ProgressBar::new(devsize));

    // if this option is set, try to deduplicate only
    // this will keep which blocks are only 0
    let mut zero_dup = Vec::<u64>::new();

    // key is hash, value is offset
    let mut dedup = HashMap::<u128, u64>::new();
    let mut dont_write = false;

    // we'll keep thred handles here
    let mut handles = Vec::new();

    // this is for our writer/hasher thread
    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>)>();

    // start our writer/hasher thread
    let hasher_handle = writer_thread(rx);

    // start args.threads number of threads
    /*

                    FILE (blocks on disk)
    +--------------------------------------------------+
    |  B0  |  B1  |  B2  |  B3  |  B4  |  B5  |  B6  |  B7 |
    +--------------------------------------------------+
        ^      ^      ^      ^
        |      |      |      |

    Thread 0        Thread 1        Thread 2        Thread 3
    ---------       ---------       ---------       ---------
    io_uring 0     io_uring 1     io_uring 2     io_uring 3
        |               |               |               |
        | read_at       | read_at       | read_at       | read_at
        | offset=0      | offset=1*K    | offset=2*K    | offset=3*K
        v               v               v               v
       B0              B1              B2              B3

    General rule:
      Thread i reads block k:

        offset = k * BLOCK_SIZE

    */
    for i in 0..args.nb_threads() {
        let tx = tx.clone();

        // build context
        let ctx = Context {
            nb_threads: args.nb_threads(),
            thread_id: i,
            block_size: args.block_size(),
            pbar: Arc::clone(&pbar),
            tx: tx,
            pattern_func: |n, i, k| n * k + i,
        };
        println!("{:?}", ctx);

        let path = args.r#if.clone();

        info!("starting thread {i}");

        handles.push(thread::spawn(move || {
            read_par(ctx, path).unwrap();
        }));
    }

    // Drop the original sender so that writer/hasher thread can exit
    drop(tx);    

    for handle in handles {
        handle.join().unwrap();
    }

    // tokio_uring::start(async {
    //     // Open a file
    //     let src: File = File::open(&args.r#if).await?;
    //     let dst = File::create(&args.of).await?;

    //     let devsize = get_dev_size(&src)?;
    //     let bar = ProgressBar::new(devsize);

    //     // Buffers
    //     let mut read_offset = 0u64;
    //     let mut write_offset = 0u64;

    //     loop {
    //         //let buf = Vec::with_capacity(args.block_size());
    //         let buf = Buffer::with_capacity(args.block_size());

    //         // Asynchronously read a chunk
    //         let (res, buf) = src.read_at(Into::<Vec<u8>>::into(buf), read_offset).await;
    //         let n = res?;

    //         // EOF ?
    //         if n == 0 {
    //             break;
    //         }

    //         // create the chunk metadata with bytes we just read
    //         let bytes = &buf[..n];
    //         let chunk = Chunk::try_from((bytes, args.dd, args.compress))?;
    //         trace!("chunk: {:?}", chunk);

    //         // Optional: compute SHA256 hash of this chunk
    //         // let mut hasher = Sha256::new();
    //         // hasher.update(chunk);
    //         // let hash = hasher.finalize();
    //         // println!("Chunk at offset {} hash: {}", offset, hex::encode(hash));

    //         // asynchronously write chunk to destination

    //         let res = chunk.write_at(&dst, &mut write_offset, args.dd).await;
    //         res?;

    //         read_offset += n as u64;
    //         bar.inc(n as u64);

    //         trace!("read offset: {read_offset} write offset: {write_offset}")
    //     }

    //     bar.finish();

    //     src.close().await?;
    //     dst.close().await?;

    //     //───────────────────────────────────────────────────────────────────────────────────
    //     // elapsed time
    //     //───────────────────────────────────────────────────────────────────────────────────
    //     let elapsed = now.elapsed();
    //     println!("took {} millis", elapsed.as_millis());

    //     println!("keys={}", dedup.len());

    //     Ok(())
    // })?;

    hasher_handle.join().unwrap();

    //───────────────────────────────────────────────────────────────────────────────────
    // elapsed time
    //───────────────────────────────────────────────────────────────────────────────────
    pbar.finish();
    let elapsed = now.elapsed();
    println!("took {} millis", elapsed.as_millis());

    Ok(())
}
