use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rand::Rng;
use serde::{Deserialize, Serialize};
use storage::{FifoFileCache, MockRequest, WriteResponse};

/// It's mock the kv workload for storage bench.
/// First it generates a lot of random key,value pairs.
/// The value is the key's hash
/// Then it start write and read threads to do the kv workload
/// The write thread will random pick a key,value pair and write it to the storage
/// The read thread will random pick a key follow zipf distribution and read it from the storage

const CACHE_SIZE: usize = 10_000;
const READER_COUNT: usize = 8;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestValue {
    check_sum: u32,
    value: Vec<u8>,
}

impl TestValue {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        // 280 bytes value is the most common value size in real cache workload
        let value: Vec<u8> = (0..280).map(|_| rng.gen()).collect();
        let check_sum = crc32fast::hash(&value);
        Self { check_sum, value }
    }

    fn validate(&self) {
        let check_sum = crc32fast::hash(&self.value);
        assert_eq!(check_sum, self.check_sum);
    }
}
impl storage::Value for TestValue {}

enum CacheItenInner {
    Memory(TestValue),
    File(WriteResponse),
    Invalid,
}

struct CacheItem {
    inner: RwLock<CacheItenInner>,
}

impl Default for CacheItem {
    fn default() -> Self {
        Self {
            inner: RwLock::new(CacheItenInner::Invalid),
        }
    }
}

impl CacheItem {
    fn update_file(&self, reponse: WriteResponse) {
        let mut inner = self.inner.write().unwrap();
        *inner = CacheItenInner::File(reponse);
    }

    fn read(&self, file_cache: &FifoFileCache) -> Option<(TestValue, WriteResponse)> {
        let inner = self.inner.read().unwrap();
        match &*inner {
            CacheItenInner::Memory(_) => None,
            CacheItenInner::File(reponse) => {
                let value = file_cache.read(&reponse)?;
                Some((value, reponse.clone()))
            }
            CacheItenInner::Invalid => None,
        }
    }
}

struct Cache {
    items: HashMap<u64, CacheItem>,
}

fn generate_cache() -> Cache {
    let mut items = HashMap::new();
    for i in 0..CACHE_SIZE {
        items.insert(i as u64, CacheItem::default());
    }
    Cache { items }
}

enum OperationTrace {
    Read(WriteResponse, Duration),
    Write(WriteResponse, Duration),
    Finish,
}

#[derive(Serialize)]
struct Trace {
    operation_type: String,
    page_id: u64,
    page_offset: u64,
    version: u64,
    duration: String,
}

fn write_thread(
    cache: Arc<FifoFileCache>,
    cache_map: Arc<Cache>,
    write_count: u64,
    trace_sender: std::sync::mpsc::Sender<OperationTrace>,
) {
    let mut rng = rand::thread_rng();
    for _ in 0..write_count {
        let key = rng.gen_range(0..CACHE_SIZE as u64);
        let value = TestValue::new();
        value.validate();
        let start = std::time::Instant::now();
        let response = cache.write(value);
        let elapsed = start.elapsed();
        trace_sender
            .send(OperationTrace::Write(response.clone(), elapsed))
            .unwrap();
        cache_map.items.get(&key).unwrap().update_file(response);
    }
}

fn read_thread(
    cache: Arc<FifoFileCache>,
    cache_map: Arc<Cache>,
    read_count: u64,
    trace_sender: std::sync::mpsc::Sender<OperationTrace>,
) {
    let mut rng = rand::thread_rng();
    for _ in 0..read_count {
        let key = rng.gen_range(0..CACHE_SIZE as u64);
        let start = std::time::Instant::now();
        let item = cache_map.items.get(&key).unwrap();
        let value = item.read(&cache);
        if let Some((value, reponse)) = value {
            let elapsed = start.elapsed();
            trace_sender
                .send(OperationTrace::Read(reponse, elapsed))
                .unwrap();
            value.validate();
        }
    }
}

// A csv writer that recieves the operation trace and write it to a file
// The file can be used to analyze the performance of the storage
// The csv file has the following columns:
// operation_type, page_id, page_offset, version, duration
fn write_trace(receiver: std::sync::mpsc::Receiver<OperationTrace>) {
    let mut writer = csv::Writer::from_path("trace.csv").unwrap();
    for trace in receiver {
        match trace {
            OperationTrace::Read(reponse, duration) => {
                writer
                    .serialize(Trace {
                        operation_type: "read".to_string(),
                        page_id: reponse.page_id,
                        page_offset: reponse.page_offset,
                        version: reponse.version,
                        duration: duration.as_micros().to_string(),
                    })
                    .unwrap();
            }
            OperationTrace::Write(reponse, duration) => {
                writer
                    .serialize(Trace {
                        operation_type: "write".to_string(),
                        page_id: reponse.page_id,
                        page_offset: reponse.page_offset,
                        version: reponse.version,
                        duration: duration.as_micros().to_string(),
                    })
                    .unwrap();
            }
            OperationTrace::Finish => break,
        }
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_read_write");
    let page_size = 4096;
    let capacity = page_size * 1024;
    let cache = Arc::new(FifoFileCache::new(path.clone(), page_size, capacity));
    let cache_map = Arc::new(generate_cache());

    let (trace_sender, trace_receiver) = std::sync::mpsc::channel();

    let trace_handle = std::thread::spawn(move || {
        write_trace(trace_receiver);
    });

    let write_count = CACHE_SIZE as u64 * 10;
    let read_count = CACHE_SIZE as u64 * 200;

    let write_handle = {
        let cache = cache.clone();
        let cache_map = cache_map.clone();
        let trace_sender = trace_sender.clone();
        std::thread::spawn(move || {
            write_thread(cache, cache_map, write_count, trace_sender);
        })
    };

    let read_cache = cache.clone();
    let read_cache_map = cache_map.clone();
    let read_handles = (0..READER_COUNT)
        .map(|_| {
            let cache = read_cache.clone();
            let cache_map = read_cache_map.clone();
            let trace_sender = trace_sender.clone();
            std::thread::spawn(move || {
                read_thread(cache, cache_map, read_count, trace_sender);
            })
        })
        .collect::<Vec<_>>();

    write_handle.join().unwrap();
    println!("write thread finished");
    for handle in read_handles {
        println!("read thread finished");
        handle.join().unwrap();
    }
    trace_sender.send(OperationTrace::Finish).unwrap();
    trace_handle.join().unwrap();
}
