//! Binary to generate TypeScript types without running tests
//!
//! Usage: cargo run --bin typegen -p myko

fn main() {
    #[cfg(not(target_arch = "wasm32"))]
    {
        myko::codegen::generate_item_types().expect("Failed to generate TypeScript types");
    }

    #[cfg(target_arch = "wasm32")]
    {
        panic!("typegen is not supported on wasm32");
    }
}
