// writer/hasher module

use std::{
    collections::BTreeMap,
    sync::mpsc::Receiver,
    thread::{self, JoinHandle},
};

use sha2::{Digest, Sha256};

pub fn writer_thread(rx: Receiver<(usize, Vec<u8>)>) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut hasher = Sha256::new();
        let mut pending = BTreeMap::<usize, Vec<u8>>::new();
        let mut next_block = 0;

        while let Ok((block_index, buf)) = rx.recv() {
            // Store received block
            pending.insert(block_index, buf);
            println!("block index={block_index}");

            // Hash any contiguous blocks in order
            while let Some(buf) = pending.remove(&next_block) {
                hasher.update(&buf);
                next_block += 1;
            }
        }

        // Final SHA-256 hash
        let hash = hasher.finalize();
        println!("SHA256: {:x}", hash);
    })
}
