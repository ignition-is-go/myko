//! Shared downcast helper for the registration `Factory` blanket impls.

use std::any::Any;

/// Downcast a type-erased request payload to its concrete `XRequest<_>` type
/// and clone it out, producing a uniform error string on mismatch.
///
/// `label` is the trailing text of the error message, so the message reads
/// `Failed to downcast {label}` (matching the per-site wording, e.g.
/// `"query payload"` or `"report to ReportRequest<Foo>"`).
///
// NOTE(ts): only reached through the registration `Factory` blanket impls,
// which are monomorphized server-side only — on wasm the generic callers are
// never instantiated, so the fn reads as dead there.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
pub(crate) fn downcast_request<T: Clone + 'static>(
    any: &dyn Any,
    label: &str,
) -> Result<T, String> {
    any.downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| format!("Failed to downcast {label}"))
}
