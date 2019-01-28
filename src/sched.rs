use crate::bank_state::BankCheckpoint;
use fnv::FnvHasher;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote_program;
use std::hash::Hasher;

pub const REFRESH_RATE: u64 = 1000;

struct Sched {
    min_slot: u64,
    ranks: Vec<(Pubkey, u64)>,
}

impl Sched {
    fn should_regenerate(prev_root: u64, new_root: u64) -> bool {
        prev_root / REFRESH_RATE != new_root / REFRESH_RATE
    }

    /// ranked leaders
    fn new_schedule(root: &BankCheckpoint) -> Sched {
        let accounts = root.accounts.accounts_db.read().unwrap();
        let leaders: Vec<(Pubkey, u64)> = accounts
            .accounts
            .iter()
            .filter_map(|(id, account)| {
                if vote_program::check_id(&account.owner) {
                    return Some((id, account.tokens));
                }
                None
            })
            .collect();
        let start = (Pubkey::default(), 0);
        let ranks = leaders
            .into_iter()
            .scan(start, |z, x| Some((x.0, z.1 + x.1)))
            .collect();
        let min_slot = ((root.fork_id() + REFRESH_RATE) / REFRESH_RATE) * REFRESH_RATE;
        Sched { ranks, min_slot }
    }

    fn compute_node(&self, slot: u64) -> Option<Pubkey> {
        if slot < self.min_slot {
            return None;
        }
        let total = self.ranks.last().unwrap().1;
        // use a stable known hasher because this MUST be the same across the entire network
        let mut hasher = FnvHasher::with_key(self.min_slot);
        hasher.write(&slot.to_le_bytes());
        let random = hasher.finish();
        let val = random % total;
        Some(
            self.ranks
                .iter()
                .skip_while(|l| val < l.1)
                .nth(0)
                .unwrap()
                .0,
        )
    }
}
