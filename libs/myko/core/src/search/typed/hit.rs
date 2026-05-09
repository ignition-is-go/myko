use super::Score;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hit<Id> {
    pub id: Id,
    pub score: Score,
    /// Index into the entity's `searchable_field_names()` slice.
    pub matched_field: usize,
}
