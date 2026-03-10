use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io,
};

use serde::Serialize;

pub trait CacheKey {
    fn cache_key(&self, state: &mut dyn Hasher);

    fn cache_key_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.cache_key(&mut hasher);
        hasher.finish()
    }
}

pub fn write_hash_cache_key<T: Hash>(value: &T, state: &mut dyn Hasher) {
    let mut inner = DefaultHasher::new();
    value.hash(&mut inner);
    state.write_u64(inner.finish());
}

pub fn write_serde_cache_key<T: Serialize>(value: &T, state: &mut dyn Hasher) {
    let mut writer = HashWriter { hasher: state };
    if serde_json::to_writer(&mut writer, value).is_err() {
        state.write_u8(0xff);
    }
}

pub fn write_str_key(state: &mut dyn Hasher, value: &str) {
    state.write_usize(value.len());
    state.write(value.as_bytes());
}

struct HashWriter<'a> {
    hasher: &'a mut dyn Hasher,
}

impl io::Write for HashWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.hasher.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
