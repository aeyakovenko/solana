/// Generate a stable hash of `self` for each `hash_index`
/// Best effort can be made for uniqueness of each hash.
pub trait BloomHashIndex {
    fn hash(&self, hash_index: u64) -> u64;
}
