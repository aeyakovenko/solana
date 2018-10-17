//! Simple Bloom Filter
use bv::BitVec;
use rand::{self, Rng};
use std::cmp;

#[derive(Default, Clone, Debug, PartialEq)]
pub struct Bloom {
    pub keys: Vec<u8>,
    pub bits: BitVec<u8>,
}

impl Bloom {
    /// create filter optimal for num size given the `false_rate`
    /// the keys are randomized for picking data out of a collision resistant hash of size
    /// `keysize` bytes
    /// https://hur.st/bloomfilter/
    pub fn random(num: usize, keysize: u8, false_rate: f64, max_bits: usize) -> Self {
        let min_num_bits = ((num as f64 * false_rate.log(2f64))
            / (1f64 / 2f64.powf(2f64.log(2f64))).log(2f64)).ceil()
            as usize;
        let num_bits = cmp::max(1, cmp::min(min_num_bits, max_bits));
        let num_keys = ((num_bits as f64 / num as f64) * 2f64.log(2f64)).round() as usize;
        let mut keys: Vec<u8> = (0..(keysize - 4)).into_iter().map(|x| x as u8).collect();
        rand::thread_rng().shuffle(&mut keys);
        keys.truncate(num_keys);
        let bits = BitVec::new_fill(false, num_bits as u64);
        Bloom { keys, bits }
    }
    fn pos(&self, key: &[u8], k: usize) -> usize {
        ((key[k] as usize) << 3
            | (key[k + 1] as usize) << 2
            | (key[k + 2] as usize) << 1
            | (key[k]) as usize)
            % (self.bits.len() as usize)
    }
    pub fn add(&mut self, key: &[u8]) {
        for k in &self.keys {
            let pos = self.pos(key, *k as usize);
            self.bits.set(pos as u64, true);
        }
    }
    pub fn contains(&mut self, key: &[u8]) -> bool {
        for k in &self.keys {
            let pos = self.pos(key, *k as usize);
            if !self.bits.get(pos as u64) {
                return false;
            }
        }
        return true;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use hash::{hash, Hash};

    #[test]
    fn test_bloom_filter() {
        //empty
        let bloom = Bloom::random(0, 32, 0.1, 100);
        assert_eq!(bloom.keys.len(), 0);
        assert_eq!(bloom.bits.len(), 1);

        //normal
        let bloom = Bloom::random(10, 32, 0.1, 100);
        assert_eq!(bloom.keys.len(), 3);
        assert_eq!(bloom.bits.len(), 34);

        //saturated
        let bloom = Bloom::random(100, 32, 0.1, 100);
        assert_eq!(bloom.keys.len(), 1);
        assert_eq!(bloom.bits.len(), 100);
    }
    #[test]
    fn test_add_contains() {
        let mut bloom = Bloom::random(100, Hash::default().as_ref().len() as u8, 0.1, 100);

        let key = hash(b"hello");
        assert!(!bloom.contains(key.as_ref()));
        bloom.add(key.as_ref());
        assert!(bloom.contains(key.as_ref()));

        let key = hash(b"world");
        assert!(!bloom.contains(key.as_ref()));
        bloom.add(key.as_ref());
        assert!(bloom.contains(key.as_ref()));
    }
}
