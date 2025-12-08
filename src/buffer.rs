use std::ops::Deref;

// an encapsulation of a byte data
pub struct Buffer(Vec<u8>);

impl Buffer {
    // new buffer
    pub fn with_capacity(capa: usize) -> Self {
        Self(Vec::with_capacity(capa))
    }

    // test if buffer is only composed with 0u8
    pub fn is_zero(&self) -> bool {
        self.iter().all(|b| *b == 0)
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/* impl AsRef<Vec<u8>> for Buffer
{
    fn as_ref(&self) -> &Vec<u8> {
        &self.0
    }
} */

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl Into<Vec<u8>> for Buffer {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

// test if buffer is only composed with 0u8
pub fn is_zero(buf: &[u8]) -> bool {
    buf.iter().all(|b| *b == 0)
}
