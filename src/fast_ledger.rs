use memmap;

struct Ledger {
    poh: memmap::MmapMut,
    tx_data: memmap::MmapMut,
    state: memmap::MmapMut,
    poher: Poh
}

//dummy
type Hash = usize;

struct Poh {
    usize poh;
}

impl Poh {
    /// give me a sample, when rebuildign the ledger, if `hash` is Some, then mix it into the stream right before this count
    /// tbd before or after
    pub next_sample(&mut self, hash: Option<Hash>) -> (Hash, usize) {
        self.poh += 1;
        (self.poh, self.poh)
    }
}

struct Tx {
    from: Hash,
    toaddr: Hash,
    op: u8,
    amount: u64,
    fee: u64,
}

impl Ledger {
    fn new(poh: &str, tx_data: &str, state: &str, poher: Poh) -> Ledger {
        Ledger {poh: memmap::map_mut(poh).expect("map poh"),
                tx_data: memmap::map_mut(tx_data).expect("map tx_data"),
                state: memmap::map_mut(state).expect("map state"),
                poher: poher}
    }
}
