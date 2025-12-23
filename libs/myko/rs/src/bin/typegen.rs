//! Binary to generate TypeScript types without running tests
//!
//! Usage: cargo run --bin typegen -p myko-rs

fn main() {
    myko_rs::codegen::generate_item_types().expect("Failed to generate TypeScript types");
}
