use solana_sdk::pubkey::Pubkey;
use hash::Hash;
use crds_traits::BloomHashIndex;

fn slice_hash(slice: &[u8], hash_index: u64) -> u64 {
       let len = slice.len() as u64;
        let mut rv = 0u64;
        for i in 0 .. 8 {
            let pos = (hash_index.overflowing_add(i).0 % len) as usize;
            rv |= (slice[pos] as u64) << i;
        }
        rv 
}

impl BloomHashIndex for Pubkey {
    fn hash(&self, hash_index: u64) -> u64
    {
        slice_hash(self.as_ref(), hash_index)
    }
}

impl BloomHashIndex for Hash {
    fn hash(&self, hash_index: u64) -> u64
    {
        slice_hash(self.as_ref(), hash_index)
    }
}

