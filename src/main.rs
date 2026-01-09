mod args;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;

use crate::args::get_args;
use crate::reader::{RunContext, read_par};
use crate::writer::{WriterParams, writer_thread};

mod device;
use anyhow::Ok;
use device::Device;

mod chunk;
mod hash;
mod reader;
mod writer;

use human_bytes::human_bytes;
use humantime::format_duration;
use indicatif::ProgressBar;
use log::{debug, info, trace};

fn main() -> anyhow::Result<()> {
    let start = Instant::now();

    // get arguments
    let args = get_args()?;
    debug!("args: {:?}", args);

    // get device size
    let devsize = Device::size(&args.r#if)?;
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
    let (tx, rx) = mpsc::channel::<(u64, Vec<u8>)>();

    // start our writer/hasher thread
    let writer_params = WriterParams::from(&args);
    let hasher_handle = writer_thread(rx, writer_params);

    info!(
        "input:{} pid:{} threads:{} block_size:{} buffers:{} target_size:{devsize}",
        args.r#if.display(),
        std::process::id(),
        args.nb_threads(),
        args.block_size(),
        args.buffers,
    );

    // new to synchronize access to offset for multi-threaded access
    let shared_offset = Arc::new(AtomicU64::new(0));
    let block_index = Arc::new(AtomicU64::new(0));

    // start args.threads number of threads
    for i in 0..args.nb_threads() {
        let tx = tx.clone();

        // build context
        let ctx = RunContext {
            nb_threads: args.nb_threads(),
            thread_id: i,
            block_size: args.block_size(),
            pbar: Arc::clone(&pbar),
            tx: tx,
            num_buffers: args.buffers,
            shared_offset: Arc::clone(&shared_offset),
            block_index: Arc::clone(&block_index),
            pattern_func: |n, i, k| n * k + i,
        };
        trace!("{:?}", ctx);

        let path = args.r#if.clone();

        debug!("starting thread {i}");
        let thread_id = thread::spawn(move || read_par(ctx, path));
        handles.push(thread_id);
    }

    // Drop the original sender so that writer/hasher thread can exit
    drop(tx);

    for handle in handles {
        let _ = handle
            .join()
            .map_err(|e| anyhow::anyhow!("thread panicked: {:?}", e))?;
    }

    // print out hash if any
    let hash = hasher_handle
        .join()
        .map_err(|e| anyhow::anyhow!("thread panicked: {:?}", e))?;

    if let Some(hash) = hash {
        println!("{hash}");
    }

    //───────────────────────────────────────────────────────────────────────────────────
    // elapsed time
    //───────────────────────────────────────────────────────────────────────────────────
    pbar.finish();
    
    let elapsed = start.elapsed();
    let rate = devsize as f64 / elapsed.as_secs_f64();
    info!(
        "took: {} millis, rate: {}/s",
        format_duration(elapsed),
        human_bytes(rate)
    );

    Ok(())
}
