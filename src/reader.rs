use std::iter;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    path::PathBuf,
    sync::{Arc, mpsc::Sender},
};

use aligned_vec::{AVec, ConstAlign, avec};
use anyhow::Context;
use futures::StreamExt;
use indicatif::ProgressBar;
use libc::{O_DIRECT, O_SYNC};
use log::{debug, info, trace};
use tokio_uring::buf::fixed::FixedBufRegistry;
use tokio_uring::buf::{IoBuf, IoBufMut};
use tokio_uring::fs::OpenOptions;

type AlignedVector = AVec<u8, ConstAlign<4096>>;
struct AlignedWrapper(AlignedVector);
impl AlignedWrapper {
    pub fn init(len: usize) -> Self {
        let v = avec![[4096] | 0u8; len];
        assert_eq!(v.len(), len);
        assert_eq!(v.capacity(), len);
        Self(v)
    }
}

// implement IOBuf & IoBufMut for our speciliazed buffer
unsafe impl IoBuf for AlignedWrapper {
    fn stable_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.0.len()
    }
    fn bytes_total(&self) -> usize {
        self.0.capacity()
    }
}
unsafe impl IoBufMut for AlignedWrapper {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
    unsafe fn set_init(&mut self, init_len: usize) {
        if self.0.len() < init_len {
            unsafe {
                self.0.set_len(init_len);
            }
        }
    }
}

// a context contains all what is necessary to apply a specific pattern
// when reading blocks using multiple threads
#[derive(Debug)]
pub struct RunContext {
    // number of threads
    pub nb_threads: usize,

    // thread ID
    pub thread_id: usize,

    // block size passed from arguments
    pub block_size: usize,

    // progress bar shared between threads
    pub pbar: Arc<ProgressBar>,

    // send part of the channel
    pub tx: Sender<(u64, Vec<u8>)>,

    // buffer registry size = number of buckets to use when issuing read_fixed_at()
    pub num_buffers: usize,

    // this will be incremented by all threads
    pub shared_offset: Arc<AtomicU64>,

    // used to sync block
    pub block_index: Arc<AtomicU64>,

    // function giving the block to read
    pub pattern_func: fn(nb_threads: usize, thread_id: usize, k: usize) -> usize,
}

// reader is called by each thread
pub fn read_par(ctx: RunContext, path: PathBuf) -> anyhow::Result<()> {
    debug!("tokio-uring runtime started");

    tokio_uring::start(async {
        // build our aligned buffer registry abd register to the kernel
        let registry = FixedBufRegistry::new(
            iter::repeat_with(|| AlignedWrapper::init(ctx.block_size)).take(ctx.num_buffers),
        );
        registry.register()?;

        // Open input file or device
        let src = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(O_DIRECT | O_SYNC)
            .open(path)
            .await?;

        // We use FuturesUnordered to track our 4 concurrent reads
        //let mut offset = 0;
        let mut active_reads = futures::stream::FuturesUnordered::new();

        // we need to associate each block read to an index which is used to serialize data sent to writer/hasher
        // thread
        // let mut block_index = 0u64;

        // initial submission to start filling buckets
        for i in 0..ctx.num_buffers {
            let buf = registry
                .check_out(i)
                .context("error checking out buffer from registry")?;

            // adds to the current value, returning the previous value.
            let offset = ctx
                .shared_offset
                .fetch_add(ctx.block_size as u64, Ordering::Relaxed);

            active_reads.push(src.read_fixed_at(buf, offset));

            //offset += ctx.block_size as u64;
        }

        while let Some((res, buf)) = active_reads.next().await {
            let bytes_read = res?;

            // continue but not break: outstanding buffers might contain data
            if bytes_read == 0 {
                continue;
            }

            // actual reads happen here
            // send data to our writer/hasher thread
            ctx.pbar.inc(bytes_read as u64);

            let index = ctx.block_index.fetch_add(1, Ordering::Relaxed);
            // println!("threadID:{} thread_offset={thread_offset}", ctx.thread_id);
            ctx.tx.send((index, buf[..bytes_read].to_vec()))?;

            if bytes_read == ctx.block_size {
                let offset = ctx
                    .shared_offset
                    .fetch_add(ctx.block_size as u64, Ordering::Relaxed);
                active_reads.push(src.read_fixed_at(buf, offset));
                //offset += ctx.block_size as u64;
            } else {
                break;
            }
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
