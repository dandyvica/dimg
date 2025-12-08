use std::borrow::Cow;

use anyhow::anyhow;
use lz4::block::compress;
use tokio_uring::{buf::BoundedBuf, fs::File};
use xxhash_rust::xxh3::xxh3_128;

// we can have different types of chunks:
// - "regular" ones with raw data, optionally compressed
// - zero chunk meaning we read a block of 0's from the source, so we know what is it
// - compressed with LZ4
#[derive(Debug, Default, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum ChunkType {
    Zero = 0,

    #[default]
    Raw = 1,

    // chunk is compressed with LZ4
    Compressed = 2,

    // chunk is built for direct mode (dd)
    Direct = 3,
}

// define the block structure save to image file
#[derive(Debug, Default)]
pub struct Chunk<'a> {
    // chunk type to know exactly what is in the chunk
    pub chunk_type: ChunkType,

    // xxhash3 to implement deduplication
    // optional is case of pure dd-like imaging
    pub hash: Option<u128>,

    // data length is mandatory if chunk is compressed
    len: Option<usize>,

    // data from what was read
    data: Cow<'a, [u8]>,
}

impl<'a> Chunk<'a> {
    // write chunk into output file
    pub async fn write_at(&self, dst: &File, offset: &mut u64, dd: bool) -> anyhow::Result<()> {
        // our write is dependant on type
        match self.chunk_type {
            ChunkType::Raw | ChunkType::Compressed => {
                // write raw data without any other metadata

                // write chunk type
                let (res, _) = dst.write_all_at(vec![self.chunk_type as u8], *offset).await;

                // write hash
                let hash = self.hash.unwrap().to_le_bytes();
                let (res, _) = dst.write_all_at(hash.to_vec(), *offset).await;

                // write length
                let len = self.len.unwrap().to_le_bytes();
                let (res, _) = dst.write_all_at(len.to_vec(), *offset).await;

                // finally write data
                let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;

                // move offset
                *offset += self.len() as u64;
            }
            ChunkType::Zero => {
                // with this method, we don't write full block
                // so dd is not an option
                if dd {
                    return Err(anyhow!("dd mode is not possible"));
                }
                let (res, _) = dst.write_all_at(vec![self.chunk_type as u8], *offset).await;
            }
            ChunkType::Direct => {
                let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;
                res?;
                *offset += self.data.len() as u64;
            }
        }

        Ok(())
    }

    // length of the whole chunk
    pub fn len(&self) -> usize {
        16 + self.len.unwrap_or_default()
    }
}

impl<'a> TryFrom<(&'a [u8], bool, bool)> for Chunk<'a> {
    type Error = anyhow::Error;

    fn try_from(value: (&'a [u8], bool, bool)) -> Result<Self, Self::Error> {
        let (data, dd_mode, compression_on) = value;

        // if dd mode, we want raw data
        if dd_mode {
            Ok(Self {
                chunk_type: ChunkType::Direct,
                hash: None,
                len: None,
                data: Cow::Borrowed(data),
            })
        } else if compression_on {
            let compressed = compress(&data, None, false)?;
            Ok(Self {
                chunk_type: ChunkType::Compressed,
                hash: Some(xxh3_128(&compressed)),
                len: Some(compressed.len()),
                data: Cow::Owned(compressed),
            })
        } else {
            Ok(Self {
                chunk_type: ChunkType::Raw,
                hash: Some(xxh3_128(data)),
                len: Some(data.len()),
                data: Cow::Borrowed(data),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from() -> anyhow::Result<()> {
        // dd mode
        let bytes = vec![0xFF; 10];
        let chunk = Chunk::try_from((bytes.as_slice(), true, false))?;

        assert_eq!(chunk.chunk_type, ChunkType::Direct);
        assert!(chunk.hash.is_none());
        assert!(chunk.len.is_none());
        assert_eq!(chunk.data, bytes);

        // compressed mode
        let bytes = b"hello world".to_vec();
        let chunk = Chunk::try_from((bytes.as_slice(), false, true))?;

        assert_eq!(chunk.chunk_type, ChunkType::Compressed);
        assert_eq!(
            chunk.hash.unwrap(),
            196948728345034857295869450085786825685u128
        );
        assert_eq!(chunk.len.unwrap(), 12);
        assert_eq!(
            chunk.data,
            vec![
                0xb0, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64
            ]
        );

        Ok(())
    }
}
