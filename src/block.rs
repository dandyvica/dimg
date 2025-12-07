use tokio_uring::{buf::BoundedBuf, fs::File};
use xxhash_rust::xxh3::xxh3_64;

// define the block structure save to image file
#[derive(Debug, Default)]
pub struct Chunk<'a> {
    // xxhash3 to implement deduplication
    pub hash: u64,

    // data length
    len: usize,

    // data from what was read
    data: &'a [u8],
}

impl<'a> Chunk<'a> {
    // write chunk into output file
    pub async fn write_at(&self, dst: &File, offset: &mut u64, dd: bool) -> anyhow::Result<()> {
        // write raw data without any other metadata
        if dd {
            let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;
            res?;
            *offset += self.data.len() as u64;
        }
        // orherwise, add metadata
        else {
            // write hash
            let hash = self.hash.to_le_bytes();
            let (res, _) = dst.write_all_at(hash.to_vec(), *offset).await;

            // write length
            let len = self.len.to_le_bytes();
            let (res, _) = dst.write_all_at(len.to_vec(), *offset).await;

            // finally write data
            let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;

            // move offset
            *offset += self.len() as u64;
        }

        Ok(())
    }

    // length of the whole chunk
    pub fn len(&self) -> usize {
        16 + self.len
    }
}

impl<'a> From<&'a [u8]> for Chunk<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self {
            hash: xxh3_64(bytes),
            len: bytes.len(),
            data: bytes,
        }
    }
}
