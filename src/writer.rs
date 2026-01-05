// writer/hasher module

use std::{
    collections::BTreeMap, fs::File, io::BufWriter, path::PathBuf, sync::mpsc::Receiver, thread::{self, JoinHandle}
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

    // output file to write to
    pub output_file: PathBuf,
}

impl From<&Args> for WriterParams {
    fn from(args: &Args) -> Self {
        Self {
            dd: args.dd,
            compress: args.compress,
            sha256: args.sha256,
            output_file: args.of.clone(),
        }
    }
}

pub fn writer_thread(
    rx: Receiver<(usize, Vec<u8>)>,
    params: WriterParams,
) -> JoinHandle<Option<String>> {
    thread::spawn(move || {
        let mut hasher = Sha256::new();
        let mut pending = BTreeMap::<usize, Vec<u8>>::new();
        let mut next_block = 0;

        // open output file for writing
        let mut of = File::create(&params.output_file).unwrap();
        let mut writer = BufWriter::new(of);

        while let Ok((block_index, buf)) = rx.recv() {
            // Store received block
            pending.insert(block_index, buf);
            trace!("block index={block_index}");

            // Hash any contiguous blocks in order
            while let Some(buf) = pending.remove(&next_block) {
                // calculate hash on this block if asked for
                if params.dd {
                    hasher.update(&buf);
                }

                // the chunk is depending on writer params
                // TODO
                let chunk = Chunk::try_from((buf.as_slice(), &params)).unwrap();
                debug!("chunk size: {} type: {:?}", chunk.len, chunk.chunk_type);

                // write chunk
                chunk.write(&mut writer);
                next_block += 1;
            }
        }

        // Final SHA-256 hash if any
        if params.dd {
            Some(format!("{:x}", hasher.finalize()))
        } else {
            None
        }
    })
}
