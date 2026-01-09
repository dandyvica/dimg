// writer/hasher module

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::mpsc::Receiver,
    thread::{self, JoinHandle},
};

use log::{debug, trace};
use sha2::{Digest, Sha256};

use crate::{args::Args, chunk::Chunk};

// what is given to the writer thread to process incoming data blocks
#[derive(Debug, Default)]
pub struct WriterParams {
    // true if user wants dd-like copy
    pub dd: bool,

    // true id user wants data to be compressed using LZ4
    pub compress: bool,

    // true if user wants to calculate sha256 sum
    pub sha256: bool,

    // true if user wants to calculate blake3 sum
    pub blake3: bool,

    // output file to write to
    pub output_file: Option<PathBuf>,
}

impl From<&Args> for WriterParams {
    fn from(args: &Args) -> Self {
        Self {
            dd: args.dd,
            compress: args.compress,
            sha256: args.sha256,
            blake3: args.blake3,
            output_file: args.of.clone(),
        }
    }
}

pub fn writer_thread(
    rx: Receiver<(u64, Vec<u8>)>,
    params: WriterParams,
) -> JoinHandle<Option<String>> {
    thread::spawn(move || {
        // start initiating hashes
        let mut hasher_sha = if params.sha256 {
            Some(Sha256::new())
        } else {
            None
        };
        let mut hasher_blake3 = if params.blake3 {
            Some(blake3::Hasher::new())
        } else {
            None
        };

        // this will help to serialize data coming from reader threads
        let mut pending = BTreeMap::<u64, Vec<u8>>::new();
        let mut next_block = 0;

        // open output file for writing
        let mut writer = if let Some(output_file) = &params.output_file {
            let of = File::create(output_file).unwrap();
            Some(BufWriter::new(of))
        } else {
            None
        };

        let mut i = 0;

        while let Ok((block_index, buf)) = rx.recv() {
            // println!("i:{i} block={block_index}");
            i += 1;

            // Store received block
            pending.insert(block_index, buf);
            trace!("block index={block_index}");

            // Hash any contiguous blocks in order
            while let Some(buf) = pending.remove(&next_block) {
                // calculate hash on this block if asked for
                if let Some(ref mut h_sha256) = hasher_sha {
                    h_sha256.update(&buf);
                }

                if let Some(ref mut h_blake3) = hasher_blake3 {
                    h_blake3.update(&buf);
                }

                // the chunk is depending on writer params
                // TODO
                let chunk = Chunk::try_from((buf.as_slice(), &params)).unwrap();
                debug!("chunk size: {} type: {:?}", chunk.len, chunk.chunk_type);

                // write chunk
                if let Some(ref mut w) = writer {
                    chunk.write(w).ok();
                }
                next_block += 1;
            }
        }

        if let Some(ref mut w) = writer {
            w.flush().unwrap();
        }

        // Final SHA-256 hash if any
        if let Some(h_sha256) = hasher_sha {
            Some(format!("{:x}", h_sha256.finalize()))
        } else if let Some(h_blake3) = hasher_blake3 {
            Some(h_blake3.finalize().to_string())
        } else {
            None
        }
    })
}
