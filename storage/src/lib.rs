use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

pub use value::Value;

mod value;

type PageVersion = AtomicU64;
type PageID = u64;
type PageOffset = u64;

pub struct FifoFileCache {
    // The version of each page, which is incremented by 1 after each write
    // After reading a page, the version of the page should be checked
    pages: Arc<[PageVersion]>,
    // The size of each page, it is fixed
    page_size: usize,
    // The path of the file
    path: PathBuf,
    manager: Mutex<WriteManger>,
}

struct WriteManger {
    pages: Arc<[PageVersion]>,
    write_page_id: u64,
    write_offset: u64,
    page_size: usize,
    file: File,
}

impl WriteManger {
    fn write_move(&mut self, value_size: u64) {
        if self.write_offset + value_size > self.page_size as u64 {
            // Increment the next page version
            let next_page_id = (self.write_page_id + 1) % (self.pages.len() as u64);
            self.pages[next_page_id as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Switch to the next page
            self.write_page_id = next_page_id;
            self.write_offset = 0;
            self.file
                .seek(SeekFrom::Start(self.write_page_id * self.page_size as u64))
                .expect("Failed to seek file");
            self.file.flush().expect("Failed to flush file");
        }
    }

    fn write_data(&mut self, data: Vec<u8>) -> WriteResponse {
        let data_len = data.len();
        self.file.write_all(&data).expect("Failed to write file");
        let response = WriteResponse {
            page_id: self.write_page_id,
            page_offset: self.write_offset,
            version: self.pages[self.write_page_id as usize]
                .load(std::sync::atomic::Ordering::Relaxed),
            length: data_len,
        };
        self.write_offset += data_len as u64;
        response
    }
}

#[derive(Debug, Clone)]
pub struct WriteResponse {
    pub page_id: PageID,
    pub page_offset: PageOffset,
    pub version: u64,
    pub length: usize,
}

pub trait MockRequest<V>
where V: Value
{
    // Read a value from the storage
    // Return none if the page_version is not the same as the version of the page
    // Otherwise return the value deserialized from the page directly
    fn read(&self, request: &WriteResponse) -> Option<V>;
    // Write a value to the storage
    // Return the page_id, page_offset, version, and length of the written value
    // The page_version should be incremented by 1
    fn write(&self, value: V) -> WriteResponse;
}

impl FifoFileCache {
    pub fn new(path: PathBuf, page_size: usize, capacity: usize) -> Self {
        assert!(page_size > 0);
        // The capacity should be a multiple of the page size
        assert!(capacity % page_size == 0);
        assert!(capacity > page_size);
        let page_num = capacity / page_size;

        // All pages are initialized to 0
        let mut pages = Vec::with_capacity(page_num);
        for _ in 0..page_num {
            pages.push(AtomicU64::new(0));
        }
        let pages: Arc<[PageVersion]> = pages.into();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .expect("Failed to open file");
        let manager = Mutex::new(WriteManger {
            pages: pages.clone(),
            write_page_id: 0,
            write_offset: 0,
            page_size,
            file,
        });
        Self {
            pages,
            page_size,
            path,
            manager,
        }
    }
}

impl<V> MockRequest<V> for FifoFileCache
where V: Value
{
    fn read(&self, request: &WriteResponse) -> Option<V> {
        assert!(request.length <= self.page_size);
        assert!(request.page_id < self.pages.len() as u64);
        assert!(request.page_offset + request.length as u64 <= self.page_size as u64);
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

    fn write(&self, value: V) -> WriteResponse {
        let serialized = bincode::serialize(&value).expect("Failed to serialize value");
        let length = serialized.len();
        assert!(length <= self.page_size);
        let mut manager = self.manager.lock().unwrap();
        manager.write_move(length as u64);
        manager.write_data(serialized)
    }
}

#[cfg(test)]
mod tests {

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

    #[test]
    fn test_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_read_write");
        let page_size = 8;
        let capacity = 8 * 2;
        let cache = FifoFileCache::new(path.clone(), page_size, capacity);

        let value = TestValue::from(123);
        let response = cache.write(value);
        assert!(response.page_id == 0);
        assert!(response.page_offset == 0);
        assert!(response.version == 0);

        let read_request = WriteResponse {
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

        assert!(reponse.page_id == 0);
        assert!(reponse.page_offset == 0);
        assert!(reponse.version == 1);

        // Try read the old value, should return None
        let read_value: Option<TestValue> = cache.read(&read_request);
        assert!(read_value.is_none());
    }
}
