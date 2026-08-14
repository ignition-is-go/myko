//! Binary to generate docs JSON from TypeScript bindings.
//!
//! Usage:
//!   cargo run --bin docgen -p myko
//! Optional env overrides:
//!   `MYKO_DOCGEN_BINDINGS_DIR` (default: libs/myko/rs/bindings)
//!   `MYKO_DOCGEN_OUTPUT` (default: libs/myko/rs/docs/myko.json)

fn main() -> anyhow::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let bindings_dir = std::env::var("MYKO_DOCGEN_BINDINGS_DIR")
            .unwrap_or_else(|_| "libs/myko/rs/bindings".to_string());
        let output = std::env::var("MYKO_DOCGEN_OUTPUT")
            .unwrap_or_else(|_| "libs/myko/rs/docs/myko.json".to_string());

        myko::codegen::typescript::generate_docs_json_from_bindings(&bindings_dir, &output)?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    {
        Err(anyhow::anyhow!("docgen is not supported on wasm32"))
    }
}
