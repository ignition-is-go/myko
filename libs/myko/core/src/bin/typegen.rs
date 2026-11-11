//! Binary to generate TypeScript types without running tests
//!
//! Usage: cargo run --bin typegen -p myko [-- <output-dir>]

fn main() -> anyhow::Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let output_dir = std::env::args()
            .nth(1)
            .unwrap_or_else(|| "bindings".to_string());
        // NOTE(ts): ts-rs reads TS_RS_EXPORT_DIR for individual type file output
        // SAFETY: typegen is single-threaded at this point (before any library init)
        unsafe { std::env::set_var("TS_RS_EXPORT_DIR", &output_dir) };
        myko::codegen::typescript::generate_item_types(&output_dir)?;
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    {
        Err(anyhow::anyhow!("typegen is not supported on wasm32"))
    }
}
