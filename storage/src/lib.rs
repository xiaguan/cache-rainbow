use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub use value::Value;

mod value;

type PageVersion = AtomicU64;
type PageID = u64;
type PageOffest = u64;

pub struct FifoFileCache {
    // The version of each page, which is incremented by 1 after each write
    // After reading a page, the version of the page should be checked
    pages: Arc<[PageVersion]>,
    // The size of each page, it is fixed
    page_size: usize,
    // The path of the file
    path: PathBuf,
    write_page_id: u64,
    write_offset: u64,
}

#[derive(Debug, Clone)]
pub struct WriteReponse {
    pub page_id: PageID,
    pub page_offset: PageOffest,
    pub version: u64,
    pub length: usize,
}

pub trait MockRequest<V>
where
    V: Value,
{
    // Read a value from the storage
    // Return none if the page_version is not the same as the version of the page
    // Otherwise return the value deserialized from the page directly
    fn read(&self, request: &WriteReponse) -> Option<V>;
    // Write a value to the storage
    // Return the page_id, page_offset, version, and length of the written value
    // The page_version should be incremented by 1
    fn write(&mut self, value: V) -> WriteReponse;
}

impl FifoFileCache {
    pub fn new(path: PathBuf, page_size: usize, capacity: usize) -> Self {
        debug_assert!(page_size > 0);
        // The capacity should be a multiple of the page size
        debug_assert!(capacity % page_size == 0);
        debug_assert!(capacity > page_size);
        let page_num = capacity / page_size;

        // All pages are initialized to 0
        let mut pages = Vec::with_capacity(page_num);
        for _ in 0..page_num {
            pages.push(AtomicU64::new(0));
        }
        let write_page_id = 0;
        let write_offset = 0;
        Self {
            pages: pages.into(),
            page_size,
            path,
            write_page_id,
            write_offset,
        }
    }

    fn write_move(&mut self, value_size: u64) {
        if self.write_offset + value_size > self.page_size as u64 {
            // Increment the next page version
            let next_page_id = (self.write_page_id + 1) % (self.pages.len() as u64);
            self.pages[next_page_id as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Switch to the next page
            self.write_page_id = next_page_id;
            self.write_offset = 0;
        }
    }

    fn write_data(&mut self, data: Vec<u8>) -> WriteReponse {
        let offset = self.write_page_id * self.page_size as u64 + self.write_offset;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)
            .expect("Failed to open file");
        file.seek(SeekFrom::Start(offset))
            .expect("Failed to seek file");
        file.write_all(&data).expect("Failed to write file");

        let response = WriteReponse {
            page_id: self.write_page_id,
            page_offset: self.write_offset,
            version: self.pages[self.write_page_id as usize]
                .load(std::sync::atomic::Ordering::Relaxed),
            length: data.len(),
        };
        self.write_offset += data.len() as u64;
        response
    }
}

impl<V> MockRequest<V> for FifoFileCache
where
    V: Value,
{
    fn read(&self, request: &WriteReponse) -> Option<V> {
        let offset = request.page_id * self.page_size as u64 + request.page_offset;
        let mut file = File::open(&self.path).expect("Failed to open file");
        file.seek(SeekFrom::Start(offset))
            .expect("Failed to seek file");

        let mut buffer = vec![0; request.length];
        file.read_exact(&mut buffer).expect("Failed to read file");

        // Each page's version is incremented by 1 after each write
        // Check the version after read, if it's not the same as the request version, return None
        let page_version =
            self.pages[request.page_id as usize].load(std::sync::atomic::Ordering::Relaxed);
        if page_version != request.version {
            return None;
        }

        let value = bincode::deserialize(&buffer).expect("Failed to deserialize value");
        Some(value)
    }

    fn write(&mut self, value: V) -> WriteReponse {
        let serialized = bincode::serialize(&value).expect("Failed to serialize value");
        let length = serialized.len();
        debug_assert!(length <= self.page_size);
        self.write_move(length as u64);
        self.write_data(serialized)
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, fs};

    use rand::Rng;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestValue {
        value: u64,
    }

    impl From<u64> for TestValue {
        fn from(value: u64) -> Self {
            Self { value }
        }
    }

    impl Value for TestValue {}

    struct CacheItem {
        value: TestValue,
        // Write reponse
        response: WriteReponse,
    }

    #[test]
    fn test_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_read_write");
        let page_size = 8;
        let capacity = 8 * 2;
        let mut cache = FifoFileCache::new(path.clone(), page_size, capacity);

        let value = TestValue::from(123);
        let response = cache.write(value);
        debug_assert!(response.page_id == 0);
        debug_assert!(response.page_offset == 0);
        debug_assert!(response.version == 0);

        let read_request = WriteReponse {
            page_id: response.page_id,
            page_offset: response.page_offset,
            version: response.version,
            length: response.length,
        };
        let read_value: TestValue = cache.read(&read_request).unwrap();
        assert_eq!(read_value.value, 123);

        cache.write(TestValue::from(456));
        // The cache only has 2 pages, so the third write should move to the next page
        let reponse = cache.write(TestValue::from(789));

        debug_assert!(reponse.page_id == 0);
        debug_assert!(reponse.page_offset == 0);
        debug_assert!(reponse.version == 1);

        // Try read the old value, should return None
        let read_value: Option<TestValue> = cache.read(&read_request);
        assert!(read_value.is_none());
    }
}
