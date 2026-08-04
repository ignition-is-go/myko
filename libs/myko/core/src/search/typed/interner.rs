use std::{collections::HashMap, hash::Hash};

/// Maps opaque ids to dense `u32` slots so the index can use compact bitmaps
/// and Vec-indexed records. Reuses freed slots across compactions.
pub struct IdInterner<Id> {
    forward: HashMap<Id, u32>,
    /// Slot `i` holds `Some(id)` if live; `None` if recycled and not yet
    /// reassigned. Length == high-water mark of slot ids.
    reverse: Vec<Option<Id>>,
    free_list: Vec<u32>,
}

impl<Id: Clone + Eq + Hash> IdInterner<Id> {
    pub fn new() -> Self {
        Self {
            forward: HashMap::new(),
            reverse: Vec::new(),
            free_list: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            forward: HashMap::with_capacity(cap),
            reverse: Vec::with_capacity(cap),
            free_list: Vec::new(),
        }
    }

    /// Intern an id. Returns the existing slot if already present (idempotent),
    /// otherwise assigns a fresh slot — preferring recycled slots from the
    /// free list to keep the dense range tight.
    pub fn intern(&mut self, id: &Id) -> u32 {
        if let Some(&existing) = self.forward.get(id) {
            return existing;
        }
        let slot = if let Some(reused) = self.free_list.pop() {
            self.reverse[reused as usize] = Some(id.clone());
            reused
        } else {
            let new_slot = self.reverse.len() as u32;
            self.reverse.push(Some(id.clone()));
            new_slot
        };
        self.forward.insert(id.clone(), slot);
        slot
    }

    pub fn lookup(&self, id: &Id) -> Option<u32> {
        self.forward.get(id).copied()
    }

    pub fn id_of(&self, slot: u32) -> Option<&Id> {
        self.reverse.get(slot as usize).and_then(|s| s.as_ref())
    }

    /// Mark a slot as free. Future `intern` calls may reuse it.
    pub fn release(&mut self, id: &Id) {
        if let Some(slot) = self.forward.remove(id) {
            self.reverse[slot as usize] = None;
            self.free_list.push(slot);
        }
    }

    pub fn len(&self) -> usize {
        self.forward.len()
    }

    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }
}

impl<Id: Clone + Eq + Hash> Default for IdInterner<Id> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_returns_dense_slots() {
        let mut interner = IdInterner::<String>::new();
        assert_eq!(interner.intern(&"a".into()), 0);
        assert_eq!(interner.intern(&"b".into()), 1);
        assert_eq!(interner.intern(&"c".into()), 2);
    }

    #[test]
    fn intern_is_idempotent() {
        let mut interner = IdInterner::<String>::new();
        let first = interner.intern(&"a".into());
        let second = interner.intern(&"a".into());
        assert_eq!(first, second);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn release_then_intern_reuses_slot() {
        let mut interner = IdInterner::<String>::new();
        let a = interner.intern(&"a".into());
        let b = interner.intern(&"b".into());
        interner.release(&"a".into());
        let c = interner.intern(&"c".into());
        assert_eq!(c, a, "freed slot should be reused");
        assert_eq!(b, 1);
    }

    #[test]
    fn lookup_after_release_returns_none() {
        let mut interner = IdInterner::<String>::new();
        interner.intern(&"a".into());
        interner.release(&"a".into());
        assert_eq!(interner.lookup(&"a".into()), None);
    }

    #[test]
    fn id_of_roundtrips() {
        let mut interner = IdInterner::<String>::new();
        let slot = interner.intern(&"hello".into());
        assert_eq!(interner.id_of(slot), Some(&"hello".to_string()));
    }
}
