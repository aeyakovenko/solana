pub struct SnapshotMonitor {
    cluster_info: Arc<RwLock<ClusterInfo>>,
    bank_forks: Arc<RwLock<BankForks>>,
    trusted: HashSet<Pubkey>,
    last_slot: Option<(Slot, Hash)>,
}

impl SnapshotMonitor {
    pub fn new(
        cluster_info: Arc<RwLock<ClusterInfo>>,
        bank_forks: Arc<RwLock<BankForks>>,
        trusted: HashSet<Pubkey>,
    ) -> Self {
        Self {
            cluster_info,
            bank_forks,
            trusted,
            last_slot: None,
        }
    }
    pub fn check(&self) {
        if self.last_slot.is_none() {
            datapoint_warn!("snapshot_monitor", ("skipped", 1, i64),);
            return;
        }
        let trusted = self.check_trusted();
        if !trusted {
            datapoint_warn!("snapshot_monitor", ("trusted_failed", slot, i64),);
        } else {
            datapoint_info!("snapshot_monitor", ("trusted_passed", slot, i64),);
        }
        let global = self.check_global();
        if !global {
            datapoint_warn!("snapshot_monitor", ("global_failed", slot, i64),);
        } else {
            datapoint_info!("snapshot_monitor", ("global_passed", slot, i64),);
        }
    }
    fn load_values(&self, slot: Slot, set: HashSet<Pubkey>) -> HashMap<Pubkey, Hash> {
        self.cluster_info
            .read()
            .unwrap()
            .collect_snapshot_hashes(slot, set)
    }
    fn check_trusted(&self) -> bool {
        let (slot, mine) = self.last_slot.unwrap_or_default();
        let trusted_values = self.load_values(slot, &self.trusted);
        let same = trusted_values.values().filter(|v| v == mine).count();
        same > 2 * self.trusted.len() / 3
    }

    fn check_global(&self) -> bool {
        let (slot, mine) = self.last_slot.unwrap_or_default();
        let vote_accounts = self
            .bank_forks
            .read()
            .unlock()
            .working_bank()
            .epoch_vote_accounts();
        let network = vote_accounts.keys().collect();
        let network_values = self.load_values(slot, &network);
        let same: Vec<Pubkey> = network_values
            .iter()
            .filter(|v| v.1 == mine)
            .map(|x| x.0)
            .collect();
        let agreed = same
            .iter()
            .map(|k| vote_accounts.lookup(k).map(|v| v.0).unwrap_or(0))
            .sum();
        let total = vote_accounts.values().map(|v| v.0).sum();
        agreed > 2 * total / 3
    }
    pub fn add(&self, slot: Slot, hash: Hash) {
        self.last_slot = Some((slot, hash));
        self.cluster_info
            .write()
            .unwrap()
            .push_snapshot_hash(slot, hash)
    }
}
