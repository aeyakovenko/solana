/// The `page_table` module implements a fast way to process requests to smart contracts including
/// simple transactions.  Benchmarks show about 125,791 ns/iter of 256 Tx's which is near 2m TPS in
/// single threaded mode.
///
/// How `Call`s travely through the pipeline can be seen in the `bench::move_funds` and test
/// 1. pt.acquire_memory_lock(&transactions, &mut lock);
/// - Memory is locked.  Any pages referenced in the Tx's are locked while the batch of
/// `transcations` is moving throuhg the pipeline.  
/// 2. pt.validate_debits(&transactions, &lock, &mut from_pages);
/// - Memory is checked for funds. All pages have a `balance`.  A caller spends this balance, so
/// all the caller pages are checked for funds.
/// 3. pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
/// - New pages might need to be allocated.  This first finds if any pages need to be allocated.
/// 4. pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
/// - PageTable WRITE lock.  This operation requires us to lock the page table and allocaet
/// pages.
/// 5. pt.load_and_execute(&transactions, &mut from_pages, &mut to_pages);
/// - Spends are actually moved.
/// 6. pt.release_memory_lock(&transactions, &lock);
/// - Memory is released
///
/// This can be safely pipelined with an `unsafe`.  The memory lock ensures that all pages
/// traveling through the system are non overlapping, and using the WRITE lock durring allocation
/// ensures that they are present when the READ lock is held.  To safely execute the contracts in
/// parallel the READ lock must be held while loading the pages, and executing the contract.
///
///
/// Main differences from the `Bank`
/// 1. `last_id` and `last_hash` are PoH identifiers, these are processed outside of this pipeline
/// 2. `Call.version` is used to prevent duplicate spends, each PublicKey has 2**64 number of calls
/// it can make.
/// 3. `Page` entry is similar to an `Account`, but it also has a `contract` that owns it.  That
///    tag allows the contract to Write to the memory owned by the page.  Contracts can spend money
use bincode::{deserialize, serialize};
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::{Mutex, RwLock};

//these types vs just u64 had a 40% impact on perf without FastHasher
type Hash = [u64; 4];
type PublicKey = [u64; 4];
type Signature = [u64; 8];

const DEFAULT_CONTRACT: [u64; 4] = [0u64; 4];

/// SYSTEM interface, same for very contract, methods 0 to 127
/// method 0
/// reallocate
/// spend the funds from the call to the first recepient
pub fn system_0_realloc(call: &Call, pages: &mut Vec<Page>) {
    let size: u64 = deserialize(&call.user_data).unwrap();
    // TODO(anatoly): add a stage to cleanup any pages that do not hold enough balance for the size
    // of the page
    pages[0].memory.resize(size as usize, 0u8);
}
/// method 1
/// assign
/// assign the page to a contract
pub fn system_1_assign(call: &Call, pages: &mut Vec<Page>) {
    let contract = deserialize(&call.user_data).unwrap();
    if call.contract == DEFAULT_CONTRACT || pages[0].contract == call.contract {
        pages[0].contract = contract;
    }
}
/// DEFAULT_CONTRACT interface
/// All contracts start with 128
/// method 128
/// move_funds
/// spend the funds from the call to the first recepient
pub fn default_contract_128_move_funds(call: &Call, pages: &mut Vec<Page>) {
    let amount: u64 = deserialize(&call.user_data).unwrap();
    pages[0].balance -= amount;
    pages[1].balance += amount;
}

//157,173 ns/iter vs 125,791 ns/iter with using this hasher, 110,000 ns with u64 as the key
struct FastHasher {
    //generates some seeds to pluck bytes out of the keys
    //since these are random every time we create a new hashset, there is no way to
    //predict what these will be for the node or the network
    rand32: [usize; 8],
    rand8: [usize; 8],
    data64: [u8; 64],
    len: usize,
}

impl Hasher for FastHasher {
    /// pluck the bytes out of the data and glue it into a u64
    fn finish(&self) -> u64 {
        let seed = if self.len == 32 {
            self.rand32
        } else if self.len == 8 {
            self.rand8
        } else {
            assert!(false);
            [0xfefefefefefefefe; 8]
        };
        (self.data64[seed[0]] as u64)
            | ((self.data64[seed[1]] as u64) << (1 * 8))
            | ((self.data64[seed[2]] as u64) << (2 * 8))
            | ((self.data64[seed[3]] as u64) << (3 * 8))
            | ((self.data64[seed[4]] as u64) << (4 * 8))
            | ((self.data64[seed[5]] as u64) << (5 * 8))
            | ((self.data64[seed[6]] as u64) << (6 * 8))
            | ((self.data64[seed[7]] as u64) << (7 * 8))
    }
    fn write(&mut self, bytes: &[u8]) {
        assert!(bytes.len() < 64);
        self.data64[..bytes.len()].copy_from_slice(bytes);
        self.len = bytes.len();
    }
}

impl FastHasher {
    fn gen_rand(size: usize, rand: &mut [usize]) {
        let mut seed: Vec<_> = (0..size).collect();
        thread_rng().shuffle(&mut seed);
        rand[0..8].copy_from_slice(&seed[0..8]);
    }

    fn new() -> FastHasher {
        let mut rand32 = [0usize; 8];
        Self::gen_rand(32, &mut rand32);
        let mut rand8 = [0usize; 8];
        Self::gen_rand(8, &mut rand8);

        FastHasher {
            rand32: rand32,
            rand8: rand8,
            data64: [0; 64],
            len: 0,
        }
    }
}

impl BuildHasher for FastHasher {
    type Hasher = FastHasher;
    fn build_hasher(&self) -> Self::Hasher {
        FastHasher {
            rand32: self.rand32.clone(),
            rand8: self.rand8.clone(),
            data64: [0; 64],
            len: 0,
        }
    }
}

/// Generic Page for the PageTable
#[derive(Clone)]
pub struct Page {
    /// key that indexes this page
    /// proove ownership of this key to spend from this Page
    owner: PublicKey,
    /// contract that owns this page
    /// contract can write to the data that is pointed to by `pointer`
    contract: PublicKey,
    /// balance that belongs to owner
    balance: u64,
    /// version of the structure
    version: u64,
    /// hash of the page data
    memhash: Hash,
    /// The following could be in a separate structure
    memory: Vec<u8>,
}

impl Page {
    pub fn new(owner: [u64; 4], contract: [u64; 4], balance: u64) -> Page {
        Page {
            owner: owner,
            contract: contract,
            balance: balance,
            version: 0,
            memhash: [0, 0, 0, 0],
            memory: vec![],
        }
    }
}
impl Default for Page {
    fn default() -> Page {
        Page {
            owner: [0, 0, 0, 0],
            contract: [0, 0, 0, 0],
            balance: 0,
            version: 0,
            memhash: [0, 0, 0, 0],
            memory: vec![],
        }
    }
}
/// Call definition
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Call {
    /// Signatures and Keys
    /// proofs[0] is the signature
    /// number of proofs of ownership of `inkeys`, `owner` is proven by the signature
    proofs: Vec<Option<Signature>>,
    /// number of keys to load, aka the to key
    /// inkeys[0] is the caller's key
    keys: Vec<PublicKey>,

    /// PoH data
    /// last id PoH observed by the sender
    last_id: u64,
    /// last PoH hash observed by the sender
    last_hash: Hash,

    /// Program
    /// the address of the program we want to call
    contract: PublicKey,
    /// OS scheduling fee
    fee: u64,
    /// struct version to prevent duplicate spends
    /// Calls with a version <= Page.version are rejected
    version: u64,
    /// method to call in the contract
    method: u8,
    /// usedata in bytes
    user_data: Vec<u8>,
}

/// simple transaction over a Call
/// TODO: The pipeline will need to pass the `destination` public keys in a side buffer to generalize
/// this
impl Call {
    pub fn new_tx(
        from: PublicKey,
        last_id: u64,
        last_hash: Hash,
        amount: u64,
        fee: u64,
        version: u64,
        to: PublicKey,
    ) -> Self {
        Call {
            proofs: vec![],
            keys: vec![from, to],
            last_id: last_id,
            last_hash: last_hash,
            contract: DEFAULT_CONTRACT,
            method: 0,
            fee: fee,
            version: version,
            user_data: serialize(&amount).unwrap(),
        }
    }
}

pub struct AllocatedPages {
    max: usize,
    allocated: BTreeMap<PublicKey, usize>,
    free: Vec<usize>,
}

impl AllocatedPages {
    pub fn new() -> Self {
        AllocatedPages {
            max: 0,
            allocated: BTreeMap::new(),
            free: vec![],
        }
    }
    pub fn lookup(&self, key: &PublicKey) -> Option<usize> {
        self.allocated.get(key).cloned().map(|x| x as usize)
    }
    pub fn free(&mut self, key: &PublicKey) {
        let page = self.lookup(key).unwrap();
        self.free.push(page);
        self.allocated.remove(key);
    }
    pub fn allocate(&mut self, key: PublicKey) -> usize {
        let page = if let Some(p) = self.free.pop() {
            p
        } else {
            let p = self.max;
            self.max += 1;
            p
        };
        let old = self.allocated.insert(key, page);
        assert!(old.is_none());
        page
    }
}

pub struct PageTable {
    /// entries of Pages
    page_table: Vec<Page>,
    /// a map from page public keys, to index into the page_table
    allocated_pages: RwLock<AllocatedPages>,
    /// locked pages that are currently processed
    mem_locks: Mutex<HashSet<PublicKey, FastHasher>>,
}

impl PageTable {
    pub fn new() -> Self {
        PageTable {
            page_table: vec![],
            allocated_pages: RwLock::new(AllocatedPages::new()),
            mem_locks: Mutex::new(HashSet::with_hasher(FastHasher::new())),
        }
    }
    // for each call in the packet acquire the memory lock for the pages it references if possible
    pub fn acquire_memory_lock(&self, packet: &Vec<Call>, acquired_memory: &mut Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            let collision: u64 = tx.keys
                .iter()
                .map({ |k| mem_locks.contains(k) as u64 })
                .sum();
            acquired_memory[i] = collision != 0;
            if collision != 0 {
                for k in &tx.keys {
                    mem_locks.insert(*k);
                }
            }
        }
    }
    // validate that we can process the fee
    pub fn validate_debits(
        &mut self,
        packet: &Vec<Call>,
        acquired_memory: &Vec<bool>,
        from_pages: &mut Vec<bool>,
    ) {
        //holds page table READ lock
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            from_pages[i] = false;
            if !acquired_memory[i] {
                continue;
            }
            let caller = &tx.keys[0];
            if let Some(memix) = allocated_pages.lookup(caller) {
                if let Some(page) = self.page_table.get(memix) {
                    assert_eq!(page.owner, *caller);
                    // skip calls that are to old
                    // this check prevents retransmitted transactions from being processed
                    // the client must increase the tx.versoin to be greater than the
                    // page_table[keys[0]].version
                    if page.version >= tx.version {
                        continue;
                    }
                    // pages must belong to the contract
                    if page.contract != tx.contract {
                        continue;
                    }
                    if page.balance >= tx.fee {
                        from_pages[i] = true;
                    }
                }
            }
        }
    }
    pub fn find_new_keys(
        &mut self,
        packet: &Vec<Call>,
        from_pages: &Vec<bool>,
        needs_alloc: &mut Vec<bool>,
        pages: &mut Vec<Vec<Option<usize>>>,
    ) {
        //holds READ lock to the page table
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if !from_pages[i] {
                continue;
            }
            pages[i] = tx.keys.iter().map(|k| allocated_pages.lookup(k)).collect();
            needs_alloc[i] = false;
            pages[i].iter().map(|x| {
                needs_alloc[i] |= x.is_none();
            });
        }
    }
    #[cfg(test)]
    pub fn force_allocate(&mut self, packet: &Vec<Call>, owner: bool, amount: u64) {
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        for tx in packet.iter() {
            let key = if owner {
                tx.keys[0]
            } else {
                tx.keys[1]
            };
            let page = Page::new(key, tx.contract, amount);
            let ix = allocated_pages.allocate(key) as usize;
            if self.page_table.len() <= ix {
                self.page_table.resize(ix + 1, page);
            } else {
                self.page_table[ix] = page;
            }
        }
    }
    #[cfg(test)]
    pub fn check_pages(
        &self,
        txs: &Vec<Call>,
        from_pages: &Vec<Option<usize>>,
        to_pages: &Vec<Option<usize>>,
    ) {
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        for (i, tx) in txs.iter().enumerate() {
            if from_pages[i].is_some() {
                assert_eq!(tx.keys[0], self.page_table[from_pages[i].unwrap()].owner);
            }
            if to_pages[i].is_some() {
                assert_eq!(tx.keys[1], self.page_table[to_pages[i].unwrap()].owner);
            }
        }
    }
    pub fn allocate_keys(
        &mut self,
        packet: &Vec<Call>,
        from_pages: &Vec<bool>,
        needs_alloc: &Vec<bool>,
        pages: &mut Vec<Vec<Option<usize>>>,
    ) {
        //holds WRITE lock to the page table, since we are adding keys and resizing the table here
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        for (i, tx) in packet.iter().enumerate() {
            if !from_pages[i] {
                continue;
            }
            if !needs_alloc[i] {
                continue;
            }
            for (j, call_pages) in pages[i].iter_mut().enumerate() {
                if call_pages.is_some() {
                    continue;
                }
                let key = tx.keys[j];
                let page = Page {
                    owner: key,
                    contract: tx.contract,
                    version: 0,
                    balance: 0,
                    memhash: [0, 0, 0, 0],
                    memory: vec![],
                };
                //safe to do while the WRITE lock is held
                let ix = allocated_pages.allocate(key) as usize;
                if self.page_table.len() <= ix {
                    trace!("reallocating page table {}", ix);
                    //safe to do while the WRITE lock is held
                    self.page_table.resize(ix + 1, Page::default());
                }
                self.page_table[ix] = page;
                *call_pages = Some(ix);
            }
        }
    }

    fn load_pages(
        // Pass the _allocated_pages argument to make sure the lock is held for this call
        _allocated_pages: &AllocatedPages,
        page_table: &mut Vec<Page>,
        checked: &Vec<bool>,
        pages: &Vec<Vec<Option<usize>>>,
        loaded_page_table: &mut Vec<Vec<&mut Page>>,
    ) {
        for (i, check) in checked.into_iter().enumerate() {
            if *check {
                continue;
            }
            for (j, oix) in pages[i].iter().enumerate() {
                let ix = oix.expect("checked pages should be loadable");
                let free_ref = unsafe {
                    let ptr = &mut page_table[ix] as *mut Page;
                    // This unsafe decouples the liftime of the page from the page_table
                    // this is safe todo while `_allocated_pages` READ lock is alive
                    &mut *ptr
                };
                loaded_page_table[i][j] = free_ref;
            }
        }
    }

    /// calculate the balances in the loaded pages
    /// at the end of the contract the balance must be the same
    /// and contract can only spend tokens from pages assigned to it
    fn calc_balance_limits(
        tx: &Call,
        pre_pages: &Vec<&mut Page>,
        post_pages: &Vec<Page>,
    ) -> (u64, u64) {
        // contract can spend any of the tokens it owns
        let spendable: u64 = pre_pages
            .iter()
            .zip(post_pages.iter())
            .map(|(pre, post)| {
                if pre.contract == tx.contract {
                    post.balance
                } else {
                    0
                }
            })
            .sum();

        // contract can't spend any of the tokens it doesn't own
        let unspendable = pre_pages
            .iter()
            .zip(post_pages.iter())
            .map(|(pre, post)| {
                if pre.contract != tx.contract {
                    post.balance
                } else {
                    0
                }
            })
            .sum();
        // in the end the total must be the same
        let total = spendable + unspendable;
        (unspendable, total)
    }

    /// parallel execution of contracts
    fn par_execute(
        // Pass the _allocated_pages argument to make sure the lock is held for this call
        _allocated_pages: &AllocatedPages,
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        loaded_page_table: &mut Vec<Vec<&mut Page>>,
    ) {
        packet
            .into_par_iter()
            .zip(loaded_page_table)
            .zip(checked)
            .map(|((tx, loaded_pages), checked)| {
                if !checked {
                    return;
                }
                // Spend the fee first
                loaded_pages[0].balance -= tx.fee;
                let mut call_pages: Vec<_> = loaded_pages.iter().map(|x| (*x).clone()).collect();
                let (pre_unspendable, pre_total) =
                    Self::calc_balance_limits(&tx, loaded_pages, &call_pages);
                // TODO(anatoly): Load actual memory

                // Find the method
                match (tx.contract, tx.method) {
                    // system interface
                    // everyone has the same reallocate
                    (_, 0) => system_0_realloc(&tx, &mut call_pages),
                    (_, 1) => system_1_assign(&tx, &mut call_pages),
                    // contract methods
                    (DEFAULT_CONTRACT, 128) => {
                        default_contract_128_move_funds(&tx, &mut call_pages)
                    }
                    (contract, method) => {
                        warn!("unknown contract and method {:?} {:x}", contract, method)
                    }
                };

                // TODO(anatoly): Verify Memory
                // Pages owned by the contract are Read/Write,
                // pages not owned by the contract are
                // Read only.  Code should verify memory integrity or
                // verify contract bytecode.

                // verify tokens
                let (post_unspendable, post_total) =
                    Self::calc_balance_limits(&tx, loaded_pages, &call_pages);

                //commit
                if post_total == pre_total && post_unspendable >= pre_unspendable {
                    loaded_pages
                        .into_iter()
                        .zip(call_pages.into_iter())
                        .map(|(pre, post)| **pre = post);
                }
            });
    }

    /// parallel execution of contracts
    /// first we load the pages, then we pass all the pages to `par_execute` function which can
    /// safely call them all in parallel
    pub fn load_and_execute(
        &mut self,
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        page_indexes: &Vec<Vec<Option<usize>>>,
        loaded_page_table: &mut Vec<Vec<&mut Page>>,
    ) {
        let allocated_pages = self.allocated_pages.read().unwrap();
        Self::load_pages(
            &allocated_pages,
            &mut self.page_table,
            checked,
            page_indexes,
            loaded_page_table,
        );
        Self::par_execute(&allocated_pages, packet, checked, loaded_page_table);
    }

    pub fn get_balance(&self, key: &PublicKey) -> Option<u64> {
        //todo does this hold the lock through the map?
        self.allocated_pages
            .read()
            .unwrap()
            .lookup(key)
            .map(|dx| self.page_table[dx].balance)
    }
    pub fn get_version(&self, key: &PublicKey) -> Option<u64> {
        //todo does this hold the lock through the map?
        self.allocated_pages
            .read()
            .unwrap()
            .lookup(key)
            .map(|dx| self.page_table[dx].version)
    }
    fn _load_memory(
        &self,
        _packet: &Vec<Call>,
        _from_pages: &Vec<Option<usize>>,
        _to_pages: &Vec<Option<usize>>,
    ) {
        //TBD
    }
    fn _execute_call(
        &self,
        _packet: &Vec<Call>,
        _from_pages: &Vec<Option<usize>>,
        _to_pages: &Vec<Option<usize>>,
    ) {
        //TBD
    }
    fn _write_memory(
        &self,
        _packet: &Vec<Call>,
        _from_pages: &Vec<Option<usize>>,
        _to_pages: &Vec<Option<usize>>,
    ) {
        //TBD
    }
    pub fn release_memory_lock(&self, packet: &Vec<Call>, lock: &Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, call) in packet.iter().enumerate() {
            if !lock[i] {
                continue;
            }
            call.keys.iter().map(|key| mem_locks.remove(key));
        }
    }
    /// fill up to the blob
    pub fn fill_blob(
        packet: &Vec<Call>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &Vec<Option<&Page>>,
        blob: &mut Vec<Call>,
        filled: &mut usize,
    ) {
        //nolock
        for (i, tx) in packet.iter().enumerate() {
            if from_pages[i].is_none() || to_pages[i].is_none() {
                continue;
            }
            blob[*filled] = tx.clone();
            *filled += 1;
            if *filled == blob.len() {
                return;
            }
        }
    }
}

//#[cfg(test)]
//mod test {
//    use logger;
//    use page_table::{Call, PageTable};
//    use rand;
//    use rand::RngCore;
//    const N: usize = 2;
//
//    pub fn rand4() -> [u64; 4] {
//        let mut r = rand::thread_rng();
//        [r.next_u64(), r.next_u64(), r.next_u64(), r.next_u64()]
//    }
//    pub fn random_tx() -> Call {
//        //sanity check
//        assert_ne!(rand4(), rand4());
//        Call::new_tx(rand4(), 0, rand4(), 1, 1, 1, rand4())
//    }
//    #[test]
//    fn mem_lock() {
//        let pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        let mut lock = vec![false; N];
//        let mut lock2 = vec![false; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        for x in &lock {
//            assert!(*x);
//        }
//        pt.acquire_memory_lock(&transactions, &mut lock2);
//        for x in &lock2 {
//            assert!(!*x);
//        }
//        pt.release_memory_lock(&transactions, &lock);
//
//        pt.acquire_memory_lock(&transactions, &mut lock2);
//        for x in &lock2 {
//            assert!(*x);
//        }
//        pt.release_memory_lock(&transactions, &lock2);
//    }
//    #[test]
//    fn validate_debits_miss() {
//        let mut pt = PageTable::new();
//        let fill_table: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&fill_table, true, 1_000_000);
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        for x in &from_pages {
//            assert!(x.is_none());
//        }
//    }
//    #[test]
//    fn validate_debits_hit() {
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        for x in &from_pages {
//            assert!(x.is_some());
//        }
//    }
//    #[test]
//    fn validate_debits_low_version() {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        for tx in &mut transactions {
//            tx.version = 0;
//        }
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        for x in &from_pages {
//            assert!(x.is_none());
//        }
//    }
//    #[test]
//    fn find_new_keys_all() {
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        let mut to_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//        for x in &to_pages {
//            assert!(x.is_none());
//        }
//    }
//    #[test]
//    fn find_new_keys_none() {
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        pt.force_allocate(&transactions, false, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        let mut to_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//        for x in &to_pages {
//            assert!(x.is_some());
//        }
//    }
//    #[test]
//    fn allocate_new_keys_all() {
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        let mut to_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//        for x in &to_pages {
//            assert!(x.is_some());
//        }
//    }
//    #[test]
//    fn allocate_new_keys_none() {
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        pt.force_allocate(&transactions, false, 1_000_000);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        let mut to_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//        for x in &to_pages {
//            assert!(x.is_some());
//        }
//    }
//    #[test]
//    fn move_funds() {
//        logger::setup();
//        let mut pt = PageTable::new();
//        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 10);
//        let mut lock = vec![false; N];
//        let mut from_pages = vec![None; N];
//        let mut to_pages = vec![None; N];
//        pt.acquire_memory_lock(&transactions, &mut lock);
//        pt.validate_debits(&transactions, &lock, &mut from_pages);
//        pt.check_pages(&transactions, &from_pages, &to_pages);
//        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//        pt.check_pages(&transactions, &from_pages, &to_pages);
//        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//        pt.check_pages(&transactions, &from_pages, &to_pages);
//        pt.move_funds(&transactions, &mut from_pages, &mut to_pages);
//        pt.check_pages(&transactions, &from_pages, &to_pages);
//        for (i, x) in transactions.iter().enumerate() {
//            assert!(to_pages[i].is_some());
//            assert!(from_pages[i].is_some());
//            assert_eq!(pt.get_balance(&x.destination), Some(x.call.amount));
//            assert_eq!(pt.get_version(&x.call.caller), Some(x.call.version));
//            assert_eq!(
//                pt.get_balance(&x.call.caller),
//                Some(10 - (x.call.amount + x.call.fee))
//            );
//        }
//    }
//}
//
//#[cfg(all(feature = "unstable", test))]
//mod bench {
//    extern crate test;
//    use self::test::Bencher;
//    use page_table::test::random_tx;
//    use page_table::PageTable;
//    use rand;
//    use rand::RngCore;
//    const N: usize = 256;
//    #[bench]
//    fn bench_update_version(bencher: &mut Bencher) {
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        bencher.iter(move || {
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//        });
//    }
//    #[bench]
//    fn bench_mem_lock(bencher: &mut Bencher) {
//        let pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_validate_debits_miss(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let fill_table: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&fill_table, true, 1_000_000);
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_validate_debits_hit(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_find_new_keys_all(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_find_new_keys_none(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        pt.force_allocate(&transactions, false, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_allocate_new_keys_all(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_allocate_new_keys_none(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        pt.force_allocate(&transactions, false, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_move_funds(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
//        pt.force_allocate(&transactions, true, 1_000_000);
//        bencher.iter(move || {
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in &mut transactions {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(&transactions, &mut lock);
//            pt.validate_debits(&transactions, &lock, &mut from_pages);
//            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
//            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
//            pt.move_funds(&transactions, &mut from_pages, &mut to_pages);
//            pt.release_memory_lock(&transactions, &lock);
//        });
//    }
//    #[bench]
//    fn bench_move_funds_large_table(bencher: &mut Bencher) {
//        let mut pt = PageTable::new();
//        let mut ttx: Vec<Vec<_>> = (0..N)
//            .map(|_| (0..N).map(|_r| random_tx()).collect())
//            .collect();
//        for transactions in &ttx {
//            pt.force_allocate(transactions, true, 1_000_000);
//        }
//        bencher.iter(move || {
//            let transactions = &mut ttx[rand::thread_rng().next_u64() as usize % N];
//            let mut lock = vec![false; N];
//            let mut from_pages = vec![None; N];
//            let mut to_pages = vec![None; N];
//            for tx in transactions.iter_mut() {
//                tx.version += 1;
//            }
//            pt.acquire_memory_lock(transactions, &mut lock);
//            pt.validate_debits(transactions, &lock, &mut from_pages);
//            pt.find_new_keys(transactions, &from_pages, &mut to_pages);
//            pt.allocate_keys(transactions, &from_pages, &mut to_pages);
//            pt.move_funds(transactions, &mut from_pages, &mut to_pages);
//            pt.release_memory_lock(transactions, &lock);
//        });
//    }
//}
