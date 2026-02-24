//! Binary to generate docs JSON from TypeScript bindings.
//!
//! Usage:
//!   cargo run --bin docgen -p myko-rs
//! Optional env overrides:
//!   MYKO_DOCGEN_BINDINGS_DIR (default: libs/myko/rs/bindings)
//!   MYKO_DOCGEN_OUTPUT (default: libs/myko/rs/docs/myko.json)

fn main() {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let bindings_dir = std::env::var("MYKO_DOCGEN_BINDINGS_DIR")
            .unwrap_or_else(|_| "libs/myko/rs/bindings".to_string());
        let output = std::env::var("MYKO_DOCGEN_OUTPUT")
            .unwrap_or_else(|_| "libs/myko/rs/docs/myko.json".to_string());

        myko_rs::codegen::generate_docs_json_from_bindings(&bindings_dir, &output)
            .expect("Failed to generate docs JSON");
    }

    #[cfg(target_arch = "wasm32")]
    {
        panic!("docgen is not supported on wasm32");
    }
}
