use std::{
    fs::{self, File},
    os::{fd::AsRawFd, unix::fs::FileTypeExt},
    path::Path,
};

use anyhow::anyhow;

pub struct Device;

pub enum DeviceType {
    HDD,
    SSD,
    NVMe,
    SD,
    Unknown,
}

// funcs specific to device
impl Device {
    // device or file size in bytes
    pub fn size(path: &Path) -> anyhow::Result<u64> {
        let file: File = File::open(path)?;
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

    // try to detect device type
    pub fn r#type(name: &str) -> DeviceType {
        let base = format!("/sys/block/{}", name);

        // 1. Check if rotational
        let is_rotational = fs::read_to_string(format!("{}/queue/rotational", base))
            .map(|s| s.trim() == "1")
            .unwrap_or(false);

        if is_rotational {
            return DeviceType::HDD;
        }

        // 2. Check for NVMe in path
        if name.starts_with("nvme") {
            return DeviceType::NVMe;
        }

        // 3. Check for SD/MMC
        if name.starts_with("mmcblk") {
            return DeviceType::SD;
        }

        // 4. Default to SSD for non-rotational sdX
        if name.starts_with("sd") {
            return DeviceType::SSD;
        }

        DeviceType::Unknown
    }
}
