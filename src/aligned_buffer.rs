use std::alloc::{Layout, alloc, dealloc};
use tokio_uring::buf::IoBuf;

pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedBuffer {
    pub fn new(size: usize) -> Self {
        // Use 4096 alignment even if sector is 512 for better flash performance
        let layout = Layout::from_size_align(size, 4096).expect("Invalid layout");
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            panic!("Memory allocation failed");
        }
        Self { ptr, layout }
    }
}

// This trait allows tokio-uring to use our custom struct
unsafe impl IoBuf for AlignedBuffer {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr
    }
    fn bytes_init(&self) -> usize {
        self.layout.size()
    }
    fn bytes_total(&self) -> usize {
        self.layout.size()
    }
}

// Critical: Prevent memory leaks
impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}
