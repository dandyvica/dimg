use std::{borrow::Cow, fs::File, io::{BufWriter, Write}};

use anyhow::anyhow;
use lz4::block::compress;
use xxhash_rust::xxh3::xxh3_128;

use crate::writer::WriterParams;

// we can have different types of chunks:
// - "regular" ones with raw data, optionally compressed
// - zero chunk meaning we read a block of 0's from the source, so we know what is it
// - compressed with LZ4
#[derive(Debug, Default, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum ChunkType {
    // chunk is made full of zeros
    FullOfZeros = 0,

    #[default]
    Raw = 1,

    // chunk is compressed with LZ4
    Compressed = 2,

    // chunk is built for direct mode (dd)
    DDMode = 3,
}

// define the block structure save to image file
#[derive(Debug, Default)]
pub struct Chunk<'a> {
    // data length doesn't include itself, nor chunk type
    pub len: usize,

    // chunk type to know exactly what is in the chunk
    pub chunk_type: ChunkType,

    // xxhash3 to implement deduplication
    // optional is case of pure dd-like imaging
    //pub hash: Option<u128>,

    // data from what was read. When full of zeros, it's None
    data: Option<Cow<'a, [u8]>>,
}

impl<'a> Chunk<'a> {
    // write chunk into output file
    pub fn write(&self, dst: &mut BufWriter<File>) -> anyhow::Result<()> {
        // our write is dependant on type
        match self.chunk_type {
            ChunkType::Raw | ChunkType::Compressed => {
                // write first length
                dst.write_all(&self.len.to_be_bytes())?;

                // write chunk type
                dst.write_all(&[self.chunk_type as u8])?;

                // then
                dst.write_all(&self.data.as_ref().unwrap())?;
            }
            ChunkType::FullOfZeros => {
                dst.write_all(&self.len.to_be_bytes())?;

                // write chunk type
                dst.write_all(&[self.chunk_type as u8])?;
            }
            ChunkType::DDMode => dst.write_all(&self.data.as_ref().unwrap())?,
        }

        Ok(())
    }
    // pub fn write(&self, dst: &File, offset: &mut u64, dd: bool) -> anyhow::Result<()> {
    //     // our write is dependant on type
    //     match self.chunk_type {
    //         ChunkType::Raw | ChunkType::Compressed => {
    //             // write raw data without any other metadata

    //             // write chunk type
    //             let (res, _) = dst.write_all_at(vec![self.chunk_type as u8], *offset).await;

    //             // write hash
    //             let hash = self.hash.unwrap().to_le_bytes();
    //             let (res, _) = dst.write_all_at(hash.to_vec(), *offset).await;

    //             // write length
    //             let len = self.len.unwrap().to_le_bytes();
    //             let (res, _) = dst.write_all_at(len.to_vec(), *offset).await;

    //             // finally write data
    //             let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;

    //             // move offset
    //             *offset += self.len() as u64;
    //         }
    //         ChunkType::Zero => {
    //             // with this method, we don't write full block
    //             // so dd is not an option
    //             if dd {
    //                 return Err(anyhow!("dd mode is not possible"));
    //             }
    //             let (res, _) = dst.write_all_at(vec![self.chunk_type as u8], *offset).await;
    //         }
    //         ChunkType::Direct => {
    //             let (res, _) = dst.write_all_at(self.data.to_vec(), *offset).await;
    //             res?;
    //             *offset += self.data.len() as u64;
    //         }
    //     }

    //     Ok(())
    // }

    // length of the whole chunk
    // pub fn len(&self) -> usize {
    //     16 + self.len.unwrap_or_default()
    // }
}

impl<'a> TryFrom<(&'a [u8], &WriterParams)> for Chunk<'a> {
    type Error = anyhow::Error;

    fn try_from(value: (&'a [u8], &WriterParams)) -> Result<Self, Self::Error> {
        let (data, params) = value;

        // if dd mode, we want raw data
        if params.dd {
            Ok(Self {
                len: 0,
                chunk_type: ChunkType::DDMode,
                // hash: None,
                data: Some(Cow::Borrowed(data)),
            })
        } else if params.compress {
            let compressed = compress(&data, None, false)?;
            Ok(Self {
                len: compressed.len(),
                chunk_type: ChunkType::Compressed,
                // hash: Some(xxh3_128(&compressed)),
                data: Some(Cow::Owned(compressed)),
            })
        } else {
            // test for zeros
            if is_zeros(data) {
                // chunk contains only the chunk_type here
                Ok(Self {
                    len: 0,
                    chunk_type: ChunkType::FullOfZeros,
                    // hash: None,
                    data: None,
                })
            } else {
                Ok(Self {
                    len: data.len(),
                    chunk_type: ChunkType::Raw,
                    // hash: Some(xxh3_128(data)),
                    data: Some(Cow::Borrowed(data)),
                })
            }
        }
    }
}

// test if data slice is full of zeros
fn is_zeros(data: &[u8]) -> bool {
    // all() is short circuit => will stop at the first non-0 byte
    data.iter().all(|b| *b == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from() -> anyhow::Result<()> {
        // dd mode
        let bytes = vec![0xFF; 10];
        let chunk = Chunk::try_from((bytes.as_slice(), true, false))?;

        assert_eq!(chunk.chunk_type, ChunkType::DDMode);
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
