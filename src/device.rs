use std::os::fd::AsRawFd;

use anyhow::anyhow;
use tokio_uring::fs::File;

// funcs specific to device
pub fn get_dev_size(dev: &File) -> anyhow::Result<u64> {
    let fd = dev.as_raw_fd();

    let mut size: u64 = 0;

    unsafe {
        // BLKGETSIZE64 ioctl number
        const BLKGETSIZE64: u64 = 0x80081272;
        let ret = libc::ioctl(fd, BLKGETSIZE64, &mut size);
        if ret < 0 {
            return Err(anyhow!(std::io::Error::last_os_error()));
        }
    }

    Ok(size)
}
