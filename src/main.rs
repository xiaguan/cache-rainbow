use get_size::GetSize;


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

    let size = level1.get_size();
    let static_size = std::mem::size_of::<Level1>();
    println!("Size of level1 struct: {} (static size: {})", size, static_size);

    let mut level2 = Level2 { level1 : level1, msg : "Hello".to_string() };
    
    let size = level2.get_size();
    println!("Size of level2 struct: {}", size);

    // Append the size of the level2 struct to the level1 data
    level2.level1.data.push(size as u8);
    level2.msg.push_str(" World");

    let size = level2.get_size();
    println!("Size of level2 struct: {}", size);
}