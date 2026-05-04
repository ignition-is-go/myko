use smallvec::SmallVec;

use crate::core::common::with_id::WithTypedId;

/// Field separator written after each pushed field. U+001F (Information
/// Separator One) is reserved for this use and must never appear in a
/// user-supplied query.
pub(super) const FIELD_SEP: char = '\u{001F}';

/// Wrapper passed to `Searchable::extract_searchable`. Hides `SmallVec` (and
/// the offsets bookkeeping) from the macro-generated impl so entity crates
/// don't need to depend on `smallvec` directly.
pub struct SearchableExtractor<'a> {
    pub(super) text: &'a mut String,
    pub(super) offsets: &'a mut SmallVec<[u32; 4]>,
}

impl<'a> SearchableExtractor<'a> {
    /// Append a field value to the buffer. Pushes the field's start offset
    /// before the value, then appends the value followed by the field
    /// separator. Call once per `#[searchable]` field, in declaration order.
    pub fn push_field(&mut self, value: &str) {
        self.offsets.push(self.text.len() as u32);
        self.text.push_str(value);
        self.text.push(FIELD_SEP);
    }
}

/// Implemented by every entity that has at least one `#[searchable]` field.
///
/// The `#[myko_item]` macro emits this impl when applicable; the method body
/// calls `extractor.push_field(...)` once per `#[searchable]` field in
/// declaration order.
pub trait Searchable: WithTypedId + Send + Sync + 'static {
    fn extract_searchable(&self, extractor: &mut SearchableExtractor<'_>);

    /// Field names parallel to the order of `push_field` calls.
    /// UI affordance for "which field matched?"; never read in hot paths.
    fn searchable_field_names() -> &'static [&'static str];
}
