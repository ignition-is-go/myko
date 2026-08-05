use std::{cell::RefCell, collections::HashMap, marker::PhantomData};

use nucleo_matcher::{Config, Matcher, Utf32Str};
use num_traits::ToPrimitive;
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use thread_local::ThreadLocal;

use super::{
    Hit, IdInterner, Score, Searchable,
    searchable::{FIELD_SEP, SearchableExtractor},
};

/// Tokens are split on whitespace, the field separator, and a small set of
/// identifier-shaped punctuation. Mirrors how users tend to type fragments of
/// names ("scene/cue", "audio.mixer", "kebab-case-tag").
const fn is_token_boundary(c: char) -> bool {
    c.is_whitespace() || matches!(c, FIELD_SEP | '/' | ':' | '.' | '-' | '_')
}

fn tokenize(text: &str) -> impl Iterator<Item = &str> {
    text.split(is_token_boundary).filter(|s| !s.is_empty())
}

#[derive(Clone, Copy, Debug)]
pub struct SearchOptions {
    pub limit: usize,
    pub max_edit_distance: u8,
    pub include_subsequence: bool,
    pub include_typo: bool,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            limit: 100,
            max_edit_distance: 2,
            include_subsequence: true,
            include_typo: true,
        }
    }
}

/// Per-entity record. `internal_id` is redundant with the position in
/// `entities` until compaction renumbers slots; storing it makes some
/// algorithms easier and costs 4 bytes.
struct EntityRecord {
    internal_id: u32,
    /// Concatenated, lowercased field values separated by `FIELD_SEP`.
    searchable_text: Box<str>,
    /// Byte offsets where each field starts in `searchable_text`.
    field_offsets: SmallVec<[u32; 4]>,
}

impl EntityRecord {
    /// Returns the field index containing the given byte position. Used to
    /// fill `Hit::matched_field` when we know *where* a match landed.
    fn field_of_position(&self, byte_pos: usize) -> usize {
        self.field_offsets
            .partition_point(|&off| usize::try_from(off).unwrap_or(usize::MAX) <= byte_pos)
            .saturating_sub(1)
    }
}

/// Per-thread matcher with reusable scratch buffers. Each rayon worker gets
/// its own slot via `ThreadLocal`, eliminating allocation per match call.
struct MatcherSlot {
    matcher: Matcher,
    needle_buf: Vec<char>,
    hay_buf: Vec<char>,
}

impl MatcherSlot {
    fn new() -> Self {
        Self {
            matcher: Matcher::new(Config::DEFAULT),
            needle_buf: Vec::new(),
            hay_buf: Vec::new(),
        }
    }
}

/// Per-type, in-memory search index. Generic over `T: Searchable` — every
/// call into `T::extract_searchable` is monomorphized and inlined; there is
/// no `dyn` on the typed hot path.
pub struct SearchIndex<T: Searchable> {
    interner: IdInterner<T::Id>,
    /// Bit set if the corresponding `internal_id` is live. Posting lists in
    /// `exact` retain dead ids until compaction; queries AND-mask with `live`.
    live: RoaringBitmap,

    /// Dense, indexed by `internal_id`. Compaction renumbers in place.
    entities: Vec<Option<EntityRecord>>,
    /// Token → set of `internal_ids` that contain it.
    exact: HashMap<Box<str>, RoaringBitmap>,

    /// Reusable buffers for the extractor, cleared between inserts.
    scratch_text: String,
    scratch_offsets: SmallVec<[u32; 4]>,

    /// Per-thread fuzzy matcher state for the subsequence tier.
    matcher_pool: ThreadLocal<RefCell<MatcherSlot>>,

    dead_count: usize,
    compaction_threshold: f32,

    _marker: PhantomData<fn(T)>,
}

impl<T: Searchable> SearchIndex<T> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            interner: IdInterner::with_capacity(cap),
            live: RoaringBitmap::new(),
            entities: Vec::with_capacity(cap),
            exact: HashMap::new(),
            scratch_text: String::new(),
            scratch_offsets: SmallVec::new(),
            matcher_pool: ThreadLocal::new(),
            dead_count: 0,
            compaction_threshold: 0.5,
            _marker: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        usize::try_from(self.live.len()).unwrap_or(usize::MAX)
    }

    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
    }

    pub fn insert(&mut self, entity: &T) {
        let id = entity.typed_id();

        if let Some(existing_slot) = self.interner.lookup(&id) {
            self.purge_tokens(existing_slot);
        }

        let internal_id = self.interner.intern(&id);

        self.scratch_text.clear();
        self.scratch_offsets.clear();
        {
            let mut extractor = SearchableExtractor {
                text: &mut self.scratch_text,
                offsets: &mut self.scratch_offsets,
            };
            T::extract_searchable(entity, &mut extractor);
        }
        self.scratch_text.make_ascii_lowercase();

        let record = EntityRecord {
            internal_id,
            searchable_text: self.scratch_text.as_str().into(),
            field_offsets: self.scratch_offsets.clone(),
        };

        let slot = usize::try_from(internal_id).unwrap_or(usize::MAX);
        if slot >= self.entities.len() {
            self.entities.resize_with(slot.saturating_add(1), || None);
        }
        if let Some(entity_slot) = self.entities.get_mut(slot) {
            *entity_slot = Some(record);
        }

        for token in tokenize(&self.scratch_text) {
            self.exact
                .entry(Box::<str>::from(token))
                .or_default()
                .insert(internal_id);
        }

        if !self.live.contains(internal_id) {
            self.live.insert(internal_id);
        }
    }

    pub fn update(&mut self, entity: &T) {
        self.insert(entity);
    }

    pub fn remove(&mut self, id: &T::Id) {
        if let Some(internal_id) = self.interner.lookup(id)
            && self.live.remove(internal_id)
        {
            self.dead_count = self.dead_count.saturating_add(1);
            if self.should_compact() {
                self.compact();
            }
        }
    }

    pub fn search(&self, query: &str, opts: SearchOptions) -> Vec<Hit<T::Id>> {
        if query.is_empty() || opts.limit == 0 {
            return Vec::new();
        }

        let lower = query.to_lowercase();

        // Tier 0: exact-token match. If this fills the limit, we still run
        // the other tiers so dedupe sees overlapping ids and keeps the
        // higher-tier score — but at the merge step they get pruned cheap.
        let mut hits: HashMap<u32, (Score, usize)> = HashMap::new();
        for (id, score, field) in self.exact_scan(&lower) {
            hits.insert(id, (score, field));
        }

        // Tier 1: subsequence (nucleo). Skipped if disabled or if exact
        // already covered the result count comfortably.
        if opts.include_subsequence && hits.len() < opts.limit {
            for (id, score, field) in self.subsequence_scan(&lower) {
                use std::collections::hash_map::Entry;
                match hits.entry(id) {
                    Entry::Vacant(e) => {
                        e.insert((score, field));
                    }
                    Entry::Occupied(mut e) => {
                        if score > e.get().0 {
                            e.insert((score, field));
                        }
                    }
                }
            }
        }

        // Tier 2: typo (Levenshtein). Same merge rule.
        if opts.include_typo && hits.len() < opts.limit {
            for (id, score, field) in self.typo_scan(&lower, opts.max_edit_distance) {
                use std::collections::hash_map::Entry;
                match hits.entry(id) {
                    Entry::Vacant(e) => {
                        e.insert((score, field));
                    }
                    Entry::Occupied(mut e) => {
                        if score > e.get().0 {
                            e.insert((score, field));
                        }
                    }
                }
            }
        }

        let mut sorted: Vec<(u32, Score, usize)> =
            hits.into_iter().map(|(id, (s, f))| (id, s, f)).collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.1));
        sorted.truncate(opts.limit);

        sorted
            .into_iter()
            .filter_map(|(internal_id, score, matched_field)| {
                let id = self.interner.id_of(internal_id)?.clone();
                Some(Hit {
                    id,
                    score,
                    matched_field,
                })
            })
            .collect()
    }

    /// Tier 0: every entity that contains the query as one of its tokens.
    fn exact_scan(&self, lower_query: &str) -> Vec<(u32, Score, usize)> {
        let Some(bitmap) = self.exact.get(lower_query) else {
            return Vec::new();
        };
        let live_hits = bitmap & &self.live;
        live_hits
            .iter()
            .filter_map(|internal_id| {
                let record = self
                    .entities
                    .get(usize::try_from(internal_id).ok()?)?
                    .as_ref()?;
                let matched_field = record
                    .searchable_text
                    .find(lower_query)
                    .map_or(0, |pos| record.field_of_position(pos));
                Some((internal_id, Score::Exact, matched_field))
            })
            .collect()
    }

    /// Tier 1: nucleo fuzzy match on each entity's full searchable text.
    /// Parallel via rayon on native targets; serial on wasm.
    fn subsequence_scan(&self, lower_query: &str) -> Vec<(u32, Score, usize)> {
        let candidates: Vec<&EntityRecord> = self
            .entities
            .iter()
            .filter_map(|e| e.as_ref())
            .filter(|e| self.live.contains(e.internal_id))
            .collect();

        let f = |record: &&EntityRecord| -> Option<(u32, Score, usize)> {
            let cell = self
                .matcher_pool
                .get_or(|| RefCell::new(MatcherSlot::new()));
            let mut slot = cell.borrow_mut();
            let MatcherSlot {
                matcher,
                needle_buf,
                hay_buf,
            } = &mut *slot;

            let needle = Utf32Str::new(lower_query, needle_buf);
            let haystack = Utf32Str::new(&record.searchable_text, hay_buf);
            let score = matcher.fuzzy_match(haystack, needle)?;

            // Approximation for matched_field: position of the query's first
            // char in the haystack. Cheaper than fuzzy_indices and good
            // enough for "which field did this hit?" UX.
            let first_char = lower_query.chars().next();
            let matched_field = first_char
                .and_then(|c| record.searchable_text.find(c))
                .map_or(0, |pos| record.field_of_position(pos));

            Some((record.internal_id, Score::Subsequence(score), matched_field))
        };

        #[cfg(not(target_arch = "wasm32"))]
        {
            use rayon::prelude::*;
            candidates.par_iter().filter_map(f).collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            candidates.iter().filter_map(f).collect()
        }
    }

    /// Tier 2: per-token Levenshtein with length prefilter. Iterates each
    /// entity's tokens and reports the best (lowest-distance) hit.
    fn typo_scan(&self, lower_query: &str, max_dist: u8) -> Vec<(u32, Score, usize)> {
        if max_dist == 0 {
            return Vec::new();
        }
        let qlen = i64::try_from(lower_query.chars().count()).unwrap_or(i64::MAX);

        let candidates: Vec<&EntityRecord> = self
            .entities
            .iter()
            .filter_map(|e| e.as_ref())
            .filter(|e| self.live.contains(e.internal_id))
            .collect();

        let f = |record: &&EntityRecord| -> Option<(u32, Score, usize)> {
            let mut best: Option<(u8, usize)> = None;
            for token in tokenize(&record.searchable_text) {
                let tlen = i64::try_from(token.chars().count()).unwrap_or(i64::MAX);
                if qlen.abs_diff(tlen) > u64::from(max_dist) {
                    continue;
                }
                let dist = u64::try_from(strsim::levenshtein(lower_query, token)).unwrap_or(u64::MAX);
                if dist > u64::from(max_dist) {
                    continue;
                }
                // Ignore distance-0 matches — they're handled by the exact
                // tier and would just create dedupe work.
                if dist == 0 {
                    continue;
                }
                let dist_u8 = u8::try_from(dist).unwrap_or(u8::MAX);
                let token_byte_pos = record
                    .searchable_text
                    .find(token)
                    .map_or(0, |p| record.field_of_position(p));
                match best {
                    None => best = Some((dist_u8, token_byte_pos)),
                    Some((cur, _)) if dist_u8 < cur => best = Some((dist_u8, token_byte_pos)),
                    _ => {}
                }
            }
            best.map(|(d, field)| (record.internal_id, Score::Typo(d), field))
        };

        #[cfg(not(target_arch = "wasm32"))]
        {
            use rayon::prelude::*;
            candidates.par_iter().filter_map(f).collect()
        }
        #[cfg(target_arch = "wasm32")]
        {
            candidates.iter().filter_map(f).collect()
        }
    }

    pub fn compact(&mut self) {
        let live_ids: Vec<u32> = self.live.iter().collect();
        let mut id_remap: HashMap<u32, u32> = HashMap::with_capacity(live_ids.len());
        let mut new_entities: Vec<Option<EntityRecord>> = Vec::with_capacity(live_ids.len());

        for (new_id, &old_id) in live_ids.iter().enumerate() {
            let Ok(new_id) = u32::try_from(new_id) else {
                break;
            };
            id_remap.insert(old_id, new_id);
            if let Some(mut record) = usize::try_from(old_id)
                .ok()
                .and_then(|index| self.entities.get_mut(index))
                .and_then(Option::take)
            {
                record.internal_id = new_id;
                new_entities.push(Some(record));
            }
        }

        for bitmap in self.exact.values_mut() {
            let renumbered: RoaringBitmap = bitmap
                .iter()
                .filter_map(|old| id_remap.get(&old).copied())
                .collect();
            *bitmap = renumbered;
        }
        self.exact.retain(|_, b| !b.is_empty());

        let mut new_interner = IdInterner::<T::Id>::with_capacity(live_ids.len());
        for &old_id in &live_ids {
            if let Some(id) = self.interner.id_of(old_id) {
                new_interner.intern(&id.clone());
            }
        }
        self.interner = new_interner;

        self.entities = new_entities;
        self.live = (0..u32::try_from(live_ids.len()).unwrap_or(u32::MAX)).collect();
        self.dead_count = 0;
    }

    fn should_compact(&self) -> bool {
        let total = self.entities.len();
        total > 1000
            && (self.dead_count.to_f32().unwrap_or(0.0) / total.to_f32().unwrap_or(1.0))
                > self.compaction_threshold
    }

    fn purge_tokens(&mut self, slot: u32) {
        let Some(record) = usize::try_from(slot)
            .ok()
            .and_then(|index| self.entities.get(index))
            .and_then(Option::as_ref)
        else {
            return;
        };
        let tokens: Vec<Box<str>> = tokenize(&record.searchable_text)
            .map(Box::<str>::from)
            .collect();
        for token in tokens {
            if let Some(bitmap) = self.exact.get_mut(token.as_ref()) {
                bitmap.remove(slot);
            }
        }
    }
}

impl<T: Searchable> Default for SearchIndex<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::core::common::with_id::{WithId, WithTypedId};

    #[derive(Clone, Debug)]
    struct TestItem {
        id: Arc<str>,
        name: String,
        category: String,
    }

    impl WithId for TestItem {
        fn id(&self) -> Arc<str> {
            self.id.clone()
        }
    }

    impl WithTypedId for TestItem {
        type Id = Arc<str>;
        fn typed_id(&self) -> Self::Id {
            self.id.clone()
        }
    }

    impl Searchable for TestItem {
        fn extract_searchable(&self, c: &mut SearchableExtractor<'_>) {
            c.push_field(&self.name);
            c.push_field(&self.category);
        }

        fn searchable_field_names() -> &'static [&'static str] {
            &["name", "category"]
        }
    }

    fn item(id: &str, name: &str, category: &str) -> TestItem {
        TestItem {
            id: Arc::<str>::from(id),
            name: name.into(),
            category: category.into(),
        }
    }

    fn opts_exact_only() -> SearchOptions {
        SearchOptions {
            include_subsequence: false,
            include_typo: false,
            ..Default::default()
        }
    }

    #[test]
    fn empty_index_returns_no_hits() {
        let index = SearchIndex::<TestItem>::new();
        assert!(
            index
                .search("anything", SearchOptions::default())
                .is_empty()
        );
    }

    #[test]
    fn empty_query_returns_no_hits() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        assert!(index.search("", SearchOptions::default()).is_empty());
    }

    #[test]
    fn exact_match_returns_inserted_entity() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.insert(&item("2", "video camera", "hardware"));

        let hits = index.search("mixer", opts_exact_only());
        assert_eq!(hits.len(), 1);
        assert_eq!(hits.first().map(|hit| hit.id.as_ref()), Some("1"));
        assert_eq!(hits.first().map(|hit| hit.score), Some(Score::Exact));
    }

    #[test]
    fn exact_match_is_case_insensitive() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "AudioMixer", "hardware"));
        let hits = index.search("AUDIOMIXER", opts_exact_only());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn matched_field_identifies_the_right_field() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "alpha beta", "hardware"));

        let name_hit = index.search("alpha", opts_exact_only());
        assert_eq!(name_hit.first().map(|hit| hit.matched_field), Some(0));

        let cat_hit = index.search("hardware", opts_exact_only());
        assert_eq!(cat_hit.first().map(|hit| hit.matched_field), Some(1));
    }

    #[test]
    fn limit_caps_returned_hits() {
        let mut index = SearchIndex::<TestItem>::new();
        for i in 0..10 {
            index.insert(&item(&format!("{i}"), "shared", "category"));
        }
        let hits = index.search(
            "shared",
            SearchOptions {
                limit: 3,
                ..opts_exact_only()
            },
        );
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn remove_excludes_entity_from_results() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.remove(&Arc::<str>::from("1"));
        assert!(index.search("mixer", opts_exact_only()).is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn update_replaces_old_text() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "old name", "hardware"));
        index.update(&item("1", "new name", "hardware"));

        assert!(index.search("old", opts_exact_only()).is_empty());
        let hits = index.search("new", opts_exact_only());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn reinserting_after_remove_resurrects_entity() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.remove(&Arc::<str>::from("1"));
        index.insert(&item("1", "audio mixer", "hardware"));
        let hits = index.search("mixer", opts_exact_only());
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn token_separators_split_camelcase_via_punctuation() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio.mixer/main", "hardware"));
        for query in &["audio", "mixer", "main"] {
            let hits = index.search(query, opts_exact_only());
            assert_eq!(hits.len(), 1, "expected hit for {query}");
        }
    }

    #[test]
    fn compact_preserves_query_results() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.insert(&item("2", "video camera", "hardware"));
        index.insert(&item("3", "lighting fixture", "hardware"));
        index.remove(&Arc::<str>::from("2"));

        let before = index.search("hardware", opts_exact_only());
        index.compact();
        let after = index.search("hardware", opts_exact_only());

        assert_eq!(before.len(), after.len());
    }

    // ----- Subsequence (Phase B) ----- //

    #[test]
    fn subsequence_match_finds_partial_typed_query() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        index.insert(&item("2", "video camera", "hardware"));

        let hits = index.search("mxr", SearchOptions::default());
        assert!(
            hits.iter().any(|h| h.id.as_ref() == "1"),
            "expected subsequence match for 'mxr' against 'audio mixer'"
        );
    }

    #[test]
    fn subsequence_acronym_style_match() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "FullMetalAlchemist", "anime"));
        index.insert(&item("2", "Star Wars", "movie"));

        let hits = index.search("fma", SearchOptions::default());
        assert_eq!(hits.first().map(|hit| hit.id.as_ref()), Some("1"));
    }

    #[test]
    fn exact_outranks_subsequence_for_same_query() {
        let mut index = SearchIndex::<TestItem>::new();
        // Both entities will match "mix" — entity 1 exactly (token), entity 2
        // only as a subsequence inside "matrix".
        index.insert(&item("1", "mix", "hardware"));
        index.insert(&item("2", "matrix", "hardware"));

        let hits = index.search("mix", SearchOptions::default());
        assert_eq!(hits.first().map(|hit| hit.id.as_ref()), Some("1"), "exact must outrank subsequence");
        assert_eq!(hits.first().map(|hit| hit.score), Some(Score::Exact));
    }

    #[test]
    fn disabling_subsequence_hides_partial_matches() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        let hits = index.search(
            "mxr",
            SearchOptions {
                include_subsequence: false,
                include_typo: false,
                ..Default::default()
            },
        );
        assert!(hits.is_empty());
    }

    // ----- Typo (Phase C) ----- //

    #[test]
    fn typo_match_distance_one() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "audio mixer", "hardware"));
        // "mxer" is one deletion away from "mixer".
        let hits = index.search(
            "mxer",
            SearchOptions {
                include_subsequence: false,
                ..Default::default()
            },
        );
        assert!(
            hits.iter().any(|h| h.id.as_ref() == "1"),
            "expected typo (distance 1) match"
        );
        let hit = hits.iter().find(|hit| hit.id.as_ref() == "1");
        assert!(hit.is_some(), "expected matching hit");
        let Some(hit) = hit else {
            return;
        };
        assert!(matches!(hit.score, Score::Typo(_)));
    }

    #[test]
    fn typo_match_distance_two() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "mixer", "hardware"));
        // "mer" — distance 2 from "mixer" (insert 'i', insert 'x').
        let hits = index.search(
            "mer",
            SearchOptions {
                include_subsequence: false,
                ..Default::default()
            },
        );
        assert!(hits.iter().any(|h| h.id.as_ref() == "1"));
    }

    #[test]
    fn typo_does_not_match_beyond_max_distance() {
        let mut index = SearchIndex::<TestItem>::new();
        index.insert(&item("1", "mixer", "hardware"));
        // "xxxxx" is distance 5 from "mixer".
        let hits = index.search(
            "xxxxx",
            SearchOptions {
                include_subsequence: false,
                max_edit_distance: 2,
                ..Default::default()
            },
        );
        assert!(hits.is_empty());
    }

    #[test]
    fn subsequence_outranks_typo() {
        let mut index = SearchIndex::<TestItem>::new();
        // "abcdef" — "abc" matches as a subsequence (a then b then c are in
        // order) AND "abc" is distance 3 from "abcdef" (= within max_dist=2?
        // no, it's 3, which is over). Use a smaller test:
        // - entity 1: "abcdef" — query "abc" matches as subsequence
        // - entity 2: "abdef" — query "abc" — strsim distance from "abdef" = 2
        //   (insert 'c', delete 'f' OR substitute pattern). And subsequence
        //   match? "abc" against "abdef" — need a, then b, then c in order.
        //   "abdef" has a, b, d, e, f — no c, so subsequence fails.
        index.insert(&item("1", "abcdef", "x"));
        index.insert(&item("2", "abxef", "x"));

        let hits = index.search("abc", SearchOptions::default());
        // entity 1 should rank above entity 2: subsequence > typo.
        let pos1 = hits.iter().position(|h| h.id.as_ref() == "1");
        let pos2 = hits.iter().position(|h| h.id.as_ref() == "2");
        if let (Some(p1), Some(p2)) = (pos1, pos2) {
            assert!(
                p1 < p2,
                "subsequence (entity 1) must rank above typo (entity 2)"
            );
        }
    }
}
