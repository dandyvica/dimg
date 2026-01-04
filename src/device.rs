use std::{fs::File, os::fd::AsRawFd, os::unix::fs::FileTypeExt, path::Path};

use anyhow::anyhow;

// funcs specific to device
pub fn input_size(path: &Path) -> anyhow::Result<u64> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;

    if metadata.file_type().is_block_device() {
        let mut size: u64 = 0;
        unsafe {
            // BLKGETSIZE64 ioctl number
            const BLKGETSIZE64: u64 = 0x80081272;
            let ret = libc::ioctl(file.as_raw_fd(), BLKGETSIZE64, &mut size);
            if ret < 0 {
                return Err(anyhow!(std::io::Error::last_os_error()));
            }
        }
        Ok(size)
    } else if metadata.file_type().is_file() {
        Ok(metadata.len())
    } else {
        Err(anyhow!("Unsupported file type"))
    }
}
