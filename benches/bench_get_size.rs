use std::time::Instant;

use get_size::GetSize;

const ITERATIONS : usize = 5_000_000_000;

#[derive(GetSize)]
pub struct Level1 {
    data : Vec<u8>,
}

#[derive(GetSize)]
pub struct Level2 {
    level1 : Level1,
    msg : String,
}

fn main() {
    let level1 = Level1 { data : vec![1, 2, 3] };
    let level2 = Level2 { level1 : level1, msg : "Hello".to_string() };

    println!("Bench get_size");

    let before = Instant::now();
    for i in 0..ITERATIONS {
        let size = level2.get_size();
        if i% 1_000_000_000 == 0 {
            println!("Size: {}", size);
        }
    }
    let elapsed = before.elapsed();

    println!("Elapsed time: {} ms", elapsed.as_millis());
    println!("get_size per iteration: {} ns", elapsed.as_nanos() / ITERATIONS as u128);
}