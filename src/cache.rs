use std::{
    time::{Duration, Instant},
};

#[derive(Clone)]
pub struct CacheEntry<T> {
    pub inserted_at: Instant,
    pub value: T,
}

impl<T> CacheEntry<T> {
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() < ttl
    }
}
