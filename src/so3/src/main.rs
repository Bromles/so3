#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    println!("lib stub: {}", so3_accord::add(1, 2))
}
