// all functions for xxhash3 or blake3

use xxhash_rust::xxh3::xxh3_128;

// compute the xxhash3-128 of zeroed block of data
pub fn zeroed_hash(block_size: usize) -> u128 {
    let bytes = vec![0u8; block_size];
    xxh3_128(&bytes)
}
