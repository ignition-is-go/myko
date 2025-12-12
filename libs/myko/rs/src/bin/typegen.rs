//! Binary to generate TypeScript types without running tests
//!
//! Usage: cargo run --bin typegen -p myko-rs

fn main() {
    myko_rs::type_gen::generate_item_types().expect("Failed to generate TypeScript types");
}
