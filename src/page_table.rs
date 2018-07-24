/// The `page_table` module implements a fast way to process requests to smart contracts including
/// simple transactions.  Benchmarks show about 125,791 ns/iter of 256 Tx's which is near 2m TPS in
/// single threaded mode.
///
/// This can be safely pipelined.  The memory lock ensures that all pages
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
use rand::{thread_rng, Rng, RngCore};
use signature::{KeyPair, KeyPairUtil, PublicKey, Signature, SignatureUtil};
use std::collections::{BTreeMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::{Mutex, RwLock};

//run with 'cargo +nightly bench --features=unstable,parex page_table::bench'
#[cfg(feature = "parex")]
use rayon::prelude::*;

//these types vs just u64 had a 40% impact on perf without FastHasher
type Hash = [u64; 4];
type PublicKey = [u64; 4];
type Signature = [u64; 8];

const DEFAULT_CONTRACT: [u64; 4] = [0u64; 4];

/// SYSTEM interface, same for very contract, methods 0 to 127
/// method 0
/// reallocate
/// spend the funds from the call to the first recepient
pub fn system_0_realloc(call: &Call, pages: &mut [Page]) {
    if call.contract == DEFAULT_CONTRACT {
        let size: u64 = deserialize(&call.user_data).unwrap();
        pages[0].memory.resize(size as usize, 0u8);
    }
}
/// method 1
/// assign
/// assign the page to a contract
pub fn system_1_assign(call: &Call, pages: &mut [Page]) {
    let contract = deserialize(&call.user_data).unwrap();
    if call.contract == DEFAULT_CONTRACT {
        pages[0].contract = contract;
        //zero out the memory in pages[0].memory
        //Contracts need to own the state of that data otherwise a use could fabricate the state and
        //manipulate the contract
        pages[0].memory.clear();
    }
}
/// DEFAULT_CONTRACT interface
/// All contracts start with 128
/// method 128
/// move_funds
/// spend the funds from the call to the first recepient
pub fn default_contract_128_move_funds(call: &Call, pages: &mut [Page]) {
    let amount: u64 = deserialize(&call.user_data).unwrap();
    pages[0].balance -= amount;
    pages[1].balance += amount;
}

#[cfg(test)]
pub fn default_contract_254_create_new_other(call: &Call, pages: &mut [Page]) {
    let amount: u64 = deserialize(&call.user_data).unwrap();
    pages[1].balance += amount;
}

#[cfg(test)]
pub fn default_contract_253_create_new_mine(call: &Call, pages: &mut [Page]) {
    let amount: u64 = deserialize(&call.user_data).unwrap();
    pages[0].balance += amount;
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
    balance: i64,
    /// version of the structure, public for testing
    /// Only a transactin that matches the current version in the page table is accepted
    /// Once its processed, the version number is incremented.  This is wrap around safe.
    pub version: u32,
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
/// Signed portion
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct CallData {
    // TODO(anatoly): allow for read only pages
    /// number of keys to load, aka the to key
    /// keys[0] is the caller's key
    keys: Vec<PublicKey>,

    /// PoH data
    /// last id PoH observed by the sender
    last_id: u64,
    /// last PoH hash observed by the sender
    pub last_hash: Hash,

    /// Program
    /// the address of the program we want to call
    pub contract: PublicKey,
    /// OS scheduling fee
    pub fee: u64,
    /// struct version to prevent duplicate spends
    /// Calls with a version <= Page.version are rejected
    pub version: u64,
    /// method to call in the contract
    pub method: u8,
    /// usedata in bytes
    pub user_data: Vec<u8>,
}
/// All the proofs
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Call {
    /// Signatures and Keys
    /// (signature, key index)
    /// This vector contains a tuple of signatures, and the key index the signature is for
    /// proofs[0] is always key[0]
    proofs: Vec<(Signature, u8)>,
    pub data: CallData,
}

/// simple transaction over a Call
/// TODO: The pipeline will need to pass the `destination` public keys in a side buffer to generalize
/// this
impl Call {
    /// Create a new Call object
    pub fn new(
        keys: &[PublicKey],
        last_id: u64,
        last_hash: Hash,
        contract: &PublicKey,
        version: u64,
        fee: u64,
        method: u8,
        user_data: Vec<u8>,
    ) -> Self {
        Call {
            proofs: vec![],
            data: CallData {
                keys: vec![from, to],
                last_id: last_id,
                last_hash: last_hash,
                contract: contract,
                fee: fee,
                version: version,
                method: method,
                user_data: user_data,
            },
        }
    }
    pub fn new_tx(
        from: PublicKey,
        last_id: u64,
        last_hash: Hash,
        amount: u64,
        fee: u64,
        version: u64,
        to: PublicKey,
    ) -> Self {
        let user_data = serialize(&amount).unwrap();
        Self::new(
            &[from, to],
            last_id,
            last_hash,
            &DEFAULT_CONTRACT,
            version,
            fee,
            128,
            user_data,
        )
    }

    /// Get the transaction data to sign.
    fn get_sign_data(&self) -> Vec<u8> {
        serialize(&self.data).expect("serialize CallData");
    }

    pub fn append_signature(&mut self, index: u8, keypair: &KeyPair) {
        let sign_data = self.get_sign_data();
        let signature = Signature::clone_from_slice(keypair.sign(&sign_data).as_ref());
        if Some(keypair.pubkey()) != self.data.keys.get(index as usize) {
            warn!("signature is for an invalid pubkey");
        }
        self.proofs.append((signature, index));
    }
    /// Verify only the transaction signature.
    pub fn verify_sig(&self) -> bool {
        warn!("transaction signature verification called");
        let sig_data = self.get_sign_data();
        self.proofs
            .map(|(index, sig)| sig.verify(&self.data.keys[index], &sign_data))
            .fold(|x, y| x && y, true)
    }

    fn rand4() -> [u64; 4] {
        let mut r = thread_rng();
        [r.next_u64(), r.next_u64(), r.next_u64(), r.next_u64()]
    }
    pub fn random_tx() -> Call {
        //sanity check
        assert_ne!(Self::rand4(), Self::rand4());
        Self::new_tx(Self::rand4(), 0, Self::rand4(), 1, 1, 1, Self::rand4())
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
    /// a map from page public keys, to index into the page_table
    allocated_pages: RwLock<AllocatedPages>,
    // TODO(anatoly): allow for read only pages
    // read only pages would need a ref count per page, so a HashMap is probably a better structure
    /// locked pages that are currently processed
    mem_locks: Mutex<HashSet<PublicKey, FastHasher>>,
    /// entries of Pages
    page_table: RwLock<Vec<Page>>,
}

pub const N: usize = 256;
pub const K: usize = 16;
pub struct Context {
    lock: Vec<bool>,
    needs_alloc: Vec<bool>,
    checked: Vec<bool>,
    pages: Vec<Vec<Option<usize>>>,
    loaded_page_table: Vec<Vec<Page>>,
    commit: Vec<bool>,
}
impl Default for Context {
    fn default() -> Self {
        let lock = vec![false; N];
        let needs_alloc = vec![false; N];
        let checked = vec![false; N];
        let pages = vec![vec![None; K]; N];
        let loaded_page_table: Vec<Vec<_>> = vec![vec![Page::default(); K]; N];
        let commit = vec![false; N];
        Context {
            lock,
            needs_alloc,
            checked,
            pages,
            loaded_page_table,
            commit,
        }
    }
}
impl PageTable {
    pub fn new() -> Self {
        PageTable {
            allocated_pages: RwLock::new(AllocatedPages::new()),
            mem_locks: Mutex::new(HashSet::with_hasher(FastHasher::new())),
            page_table: RwLock::new(vec![]),
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
            trace!("contains count: {}", collision);
            acquired_memory[i] = collision == 0;
            if collision == 0 {
                for k in &tx.keys {
                    mem_locks.insert(*k);
                }
            }
        }
    }
    pub fn acquire_validate_find(&self, transactions: &Vec<Call>, ctx: &mut Context) {
        self.acquire_memory_lock(&transactions, &mut ctx.lock);
        self.validate_call(&transactions, &ctx.lock, &mut ctx.checked);
        self.find_new_keys(
            &transactions,
            &ctx.checked,
            &mut ctx.needs_alloc,
            &mut ctx.pages,
        );
    }
    pub fn allocate_keys_with_ctx(&self, transactions: &Vec<Call>, ctx: &mut Context) {
        self.allocate_keys(
            &transactions,
            &ctx.checked,
            &ctx.needs_alloc,
            &mut ctx.pages,
        );
    }
    pub fn execute_with_ctx(transactions: &Vec<Call>, ctx: &mut Context) {
        PageTable::execute(
            &transactions,
            &ctx.checked,
            &mut ctx.loaded_page_table,
            &mut ctx.commit,
        );
    }

    /// validate that we can process the fee and its not a dup call
    pub fn validate_call(
        &self,
        packet: &Vec<Call>,
        acquired_memory: &Vec<bool>,
        checked: &mut Vec<bool>,
    ) {
        let allocated_pages = self.allocated_pages.read().unwrap();
        let page_table = self.page_table.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            checked[i] = false;
            if !acquired_memory[i] {
                continue;
            }
            let caller = &tx.keys[0];
            if let Some(memix) = allocated_pages.lookup(caller) {
                if let Some(page) = page_table.get(memix) {
                    assert_eq!(page.owner, *caller);
                    // skip calls that are to old
                    // this check prevents retransmitted transactions from being processed
                    // the client must increase the tx.versoin to be greater than the
                    // page_table[keys[0]].version
                    if page.version != tx.version {
                        continue;
                    }
                    // pages must belong to the contract
                    if page.contract != tx.contract {
                        continue;
                    }
                    if page.balance >= tx.fee {
                        checked[i] = true;
                    }
                }
            }
        }
    }
    pub fn find_new_keys(
        &self,
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        needs_alloc: &mut Vec<bool>,
        pages: &mut Vec<Vec<Option<usize>>>,
    ) {
        //holds READ lock to the page table
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if !checked[i] {
                continue;
            }
            needs_alloc[i] = false;
            for (j, k) in tx.keys.iter().enumerate() {
                let lookup = allocated_pages.lookup(k);
                needs_alloc[i] = needs_alloc[i] || lookup.is_none();
                pages[i][j] = lookup;
            }
        }
    }
    pub fn force_allocate(&self, packet: &Vec<Call>, owner: bool, amount: u64) {
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        let mut page_table = self.page_table.write().unwrap();
        for tx in packet.iter() {
            let key = if owner {
                tx.keys[0]
            } else {
                tx.keys[1]
            };
            let page = Page::new(key, tx.contract, amount);
            let ix = allocated_pages.allocate(key) as usize;
            if page_table.len() <= ix {
                page_table.resize(ix + 1, page);
            } else {
                page_table[ix] = page;
            }
        }
    }
    pub fn sanity_check_pages(
        &self,
        txs: &Vec<Call>,
        checked: &Vec<bool>,
        pages: &Vec<Vec<Option<usize>>>,
    ) {
        let page_table = self.page_table.read().unwrap();
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        for (i, tx) in txs.iter().enumerate() {
            if checked[i] {
                assert!(pages[i][0].is_some());
            }
            for (p, k) in pages[i].iter().zip(&tx.keys) {
                if p.is_some() {
                    assert_eq!(*k, page_table[p.unwrap()].owner);
                }
            }
        }
    }
    pub fn allocate_keys(
        &self,
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        needs_alloc: &Vec<bool>,
        pages: &mut Vec<Vec<Option<usize>>>,
    ) {
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        let mut page_table = self.page_table.write().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if !checked[i] {
                continue;
            }
            if !needs_alloc[i] {
                continue;
            }
            for (j, key) in tx.keys.iter().enumerate() {
                if pages[i][j].is_some() {
                    continue;
                }
                let page = Page {
                    owner: *key,
                    contract: tx.contract,
                    version: 0,
                    balance: 0,
                    memhash: [0, 0, 0, 0],
                    memory: vec![],
                };
                let ix = allocated_pages.allocate(*key) as usize;
                //recheck since we are getting a new lock
                if page_table.len() <= ix {
                    trace!("reallocating page table {}", ix);
                    //safe to do while the WRITE lock is held
                    page_table.resize(ix + 1, Page::default());
                }
                page_table[ix] = page;
                pages[i][j] = Some(ix);
            }
        }
    }

    pub fn load_pages(
        &self,
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        pages: &Vec<Vec<Option<usize>>>,
        loaded_page_table: &mut Vec<Vec<Page>>,
    ) {
        let page_table = self.page_table.read().unwrap();
        for (i, check) in checked.into_iter().enumerate() {
            if !*check {
                continue;
            }
            for (j, (_, oix)) in packet[i].keys.iter().zip(pages[i].iter()).enumerate() {
                let ix = oix.expect("checked pages should be loadable");
                loaded_page_table[i][j] = page_table[ix].clone();
            }
        }
    }
    pub fn load_pages_with_ctx(&self, packet: &Vec<Call>, ctx: &mut Context) {
        self.load_pages(packet, &ctx.checked, &ctx.pages, &mut ctx.loaded_page_table);
    }

    /// calculate the balances in the loaded pages
    /// at the end of the contract the balance must be the same
    /// and contract can only spend tokens from pages assigned to it
    fn validate_balances(tx: &Call, pre_pages: &Vec<Page>, post_pages: &Vec<Page>) -> bool {
        // contract can spend any of the tokens it owns
        for ((pre, post), _tx) in pre_pages.iter().zip(post_pages.iter()).zip(tx.keys.iter()) {
            if pre.contract != tx.contract && pre.balance != post.balance {
                return false;
            }
        }
        // contract can't spend any of the tokens it doesn't own
        let pre_sum: u64 = pre_pages
            .iter()
            .zip(tx.keys.iter())
            .map(|(pre, _)| pre.balance)
            .sum();
        let post_sum: u64 = post_pages
            .iter()
            .zip(tx.keys.iter())
            .map(|(pre, _)| pre.balance)
            .sum();
        pre_sum == post_sum
    }

    /// parallel execution of contracts should be possible here since all the alls have no
    /// dependencies
    pub fn execute(
        // Pass the _allocated_pages argument to make sure the lock is held for this call
        packet: &Vec<Call>,
        checked: &Vec<bool>,
        loaded_page_table: &mut Vec<Vec<Page>>,
        commit: &mut Vec<bool>,
    ) {
        #[cfg(not(feature = "parex"))]
        let iter = packet
            .into_iter()
            .zip(loaded_page_table)
            .zip(checked)
            .zip(commit);
        #[cfg(feature = "parex")]
        let iter = packet
            .into_par_iter()
            .zip(loaded_page_table)
            .zip(checked)
            .zip(commit);

        iter.for_each(|(((tx, loaded_pages), checked), commit)| {
            if !checked {
                return;
            }
            // fee is paid
            *commit = true;
            loaded_pages[0].balance -= tx.fee;
            loaded_pages[0].version = tx.version;

            let mut call_pages: Vec<Page> = tx.keys
                .iter()
                .zip(loaded_pages.iter())
                .map(|(_, x)| x.clone())
                .collect();

            // Find the method
            match (tx.contract, tx.method) {
                // system interface
                // everyone has the same reallocate
                (_, 0) => system_0_realloc(&tx, &mut call_pages),
                (_, 1) => system_1_assign(&tx, &mut call_pages),
                // contract methods
                (DEFAULT_CONTRACT, 128) => default_contract_128_move_funds(&tx, &mut call_pages),
                #[cfg(test)]
                (DEFAULT_CONTRACT, 254) => {
                    default_contract_254_create_new_other(&tx, &mut call_pages)
                }
                #[cfg(test)]
                (DEFAULT_CONTRACT, 253) => {
                    default_contract_253_create_new_mine(&tx, &mut call_pages)
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
            if !Self::validate_balances(&tx, &loaded_pages, &call_pages) {
                return;
            }
            //update the version
            //TODO(anatoly): wrap around test, the OS should correclty handle this value as it
            //wrapps around, since only 1 call can access this page at a time.
            call_pages[0].version += 1;
            // write pages back to memory
            for (pre, post) in loaded_pages.iter_mut().zip(call_pages.into_iter()) {
                *pre = post;
            }
        });
    }

    /// parallel execution of contracts
    /// first we load the pages, then we pass all the pages to `execute` function which can
    /// safely call them all in parallel
    pub fn commit(
        &self,
        packet: &Vec<Call>,
        commits: &Vec<bool>,
        pages: &Vec<Vec<Option<usize>>>,
        loaded_page_table: &Vec<Vec<Page>>,
    ) {
        let mut page_table = self.page_table.write().unwrap();
        let mut count = 0;
        for (i, commit) in commits.into_iter().enumerate() {
            if !*commit {
                continue;
            }
            for (j, (_, oix)) in packet[i].keys.iter().zip(pages[i].iter()).enumerate() {
                let ix = oix.expect("checked pages should be loadable");
                page_table[ix] = loaded_page_table[i][j].clone();
            }
            count += 1;
        }
        self.transaction_count.fetch_add(count, Ordering::Relaxed);
    }
    pub fn commit_release_with_ctx(&self, packet: &Vec<Call>, ctx: &Context) {
        self.commit(packet, &ctx.commit, &ctx.pages, &ctx.loaded_page_table);
        self.release_memory_lock(packet, &ctx.lock);
    }

    pub fn get_balance(&self, key: &PublicKey) -> Option<u64> {
        let ap = self.allocated_pages.read().unwrap();
        let pt = self.page_table.read().unwrap();
        ap.lookup(key).map(|dx| pt[dx].balance)
    }
    pub fn get_version(&self, key: &PublicKey) -> Option<u64> {
        let ap = self.allocated_pages.read().unwrap();
        let pt = self.page_table.read().unwrap();
        ap.lookup(key).map(|dx| pt[dx].version)
    }
    pub fn release_memory_lock(&self, packet: &Vec<Call>, lock: &Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, call) in packet.iter().enumerate() {
            if !lock[i] {
                continue;
            }
            for key in &call.keys {
                mem_locks.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use bincode::deserialize;
    use logger;
    use packet::Recycler;
    use page_table::{Call, Context, Page, PageTable, K, N};
    use rayon::prelude::*;
    use std::collections::VecDeque;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread::spawn;
    use std::time::Instant;

    #[test]
    fn mem_lock() {
        logger::setup();
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        let mut lock = vec![false; N];
        let mut lock2 = vec![false; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        for x in &lock {
            assert!(*x);
        }
        pt.acquire_memory_lock(&transactions, &mut lock2);
        for x in &lock2 {
            assert!(!*x);
        }
        pt.release_memory_lock(&transactions, &lock);

        pt.acquire_memory_lock(&transactions, &mut lock2);
        for x in &lock2 {
            assert!(*x);
        }
        pt.release_memory_lock(&transactions, &lock2);
    }
    #[test]
    fn validate_call_miss() {
        let pt = PageTable::new();
        let fill_table: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&fill_table, true, 1_000_000);
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        for x in &checked {
            assert!(!x);
        }
    }
    #[test]
    fn validate_call_hit() {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        for x in &checked {
            assert!(x);
        }
    }
    #[test]
    fn validate_call_low_version() {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        for tx in &mut transactions {
            tx.version = 0;
        }
        pt.validate_call(&transactions, &lock, &mut checked);
        for x in &checked {
            assert!(!x);
        }
    }
    #[test]
    fn find_new_keys_needs_alloc() {
        logger::setup();
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut pages = vec![vec![None; K]; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut pages);
        for x in &needs_alloc {
            assert!(x);
        }
    }
    #[test]
    fn find_new_keys_no_alloc() {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut pages = vec![vec![None; K]; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut pages);
        for x in &needs_alloc {
            assert!(!x);
        }
    }
    #[test]
    fn allocate_new_keys_some_new_allocs() {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut pages = vec![vec![None; K]; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut pages);
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut pages);
        for (i, tx) in transactions.iter().enumerate() {
            for (j, _) in tx.keys.iter().enumerate() {
                assert!(pages[i][j].is_some());
            }
        }
    }
    #[test]
    fn allocate_new_keys_no_new_allocs() {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut pages = vec![vec![None; K]; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut pages);
        for a in &needs_alloc {
            assert!(!a);
        }
        for (i, tx) in transactions.iter().enumerate() {
            for (j, _) in tx.keys.iter().enumerate() {
                assert!(pages[i][j].is_some());
            }
        }
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut pages);
        for (i, tx) in transactions.iter().enumerate() {
            for (j, _) in tx.keys.iter().enumerate() {
                assert!(pages[i][j].is_some());
            }
        }
    }
    #[test]
    fn load_and_execute() {
        logger::setup();
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 10);
        let mut lock = vec![false; N];
        let mut needs_alloc = vec![false; N];
        let mut checked = vec![false; N];
        let mut pages = vec![vec![None; K]; N];
        let mut loaded_page_table: Vec<Vec<_>> = vec![vec![Page::default(); K]; N];
        let mut commit = vec![false; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_call(&transactions, &lock, &mut checked);
        pt.find_new_keys(&transactions, &checked, &mut needs_alloc, &mut pages);
        pt.sanity_check_pages(&transactions, &checked, &pages);
        pt.allocate_keys(&transactions, &checked, &needs_alloc, &mut pages);
        pt.sanity_check_pages(&transactions, &checked, &pages);
        pt.load_pages(
            &transactions,
            &mut checked,
            &mut pages,
            &mut loaded_page_table,
        );
        PageTable::execute(&transactions, &checked, &mut loaded_page_table, &mut commit);
        pt.commit(&transactions, &commit, &pages, &loaded_page_table);
        pt.release_memory_lock(&transactions, &lock);
        for (i, x) in transactions.iter().enumerate() {
            assert!(checked[i]);
            let amount: u64 = deserialize(&x.user_data).unwrap();
            assert_eq!(pt.get_balance(&x.keys[1]), Some(amount));
            assert_eq!(pt.get_version(&x.keys[1]), Some(0));
            assert_eq!(pt.get_version(&x.keys[0]), Some(x.version));
            assert_eq!(pt.get_balance(&x.keys[0]), Some(10 - (amount + x.fee)));
        }
    }
    #[test]
    fn load_and_execute_double_spends() {
        let pt = PageTable::new();
        let mut txs: Vec<_> = (0..2).map(|_r| Call::random_tx()).collect();
        let start_bal = 1_000_000;
        pt.force_allocate(&txs, true, start_bal);
        let mut ctx = Context::default();
        txs[0].method = 254;
        txs[1].method = 253;
        txs[0].fee = 3;
        txs[1].fee = 4;
        pt.acquire_validate_find(&txs, &mut ctx);
        pt.allocate_keys_with_ctx(&txs, &mut ctx);
        pt.load_pages_with_ctx(&txs, &mut ctx);
        PageTable::execute_with_ctx(&txs, &mut ctx);
        pt.commit_release_with_ctx(&txs, &ctx);
        assert_eq!(pt.get_balance(&txs[0].keys[1]), Some(0));
        assert_eq!(
            pt.get_balance(&txs[0].keys[0]),
            Some(start_bal - txs[0].fee)
        );
        assert_eq!(pt.get_balance(&txs[1].keys[1]), Some(0));
        assert_eq!(
            pt.get_balance(&txs[1].keys[0]),
            Some(start_bal - txs[1].fee)
        );
    }
    //TODO test assignment
    //TODO test spends of unasigned funds
    //TODO test realloc
    type ContextRecycler = Recycler<Context>;
    #[test]
    fn load_and_execute_pipeline_bench() {
        logger::setup();
        let context_recycler = ContextRecycler::default();
        let pt = PageTable::new();
        let count = 10000;
        let mut ttx: Vec<Vec<Call>> = (0..count)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for tx in &ttx {
            pt.force_allocate(tx, true, 1_000_000);
        }
        let (send_transactions, recv_transactions) = channel();
        let (send_execute, recv_execute) = channel();
        let (send_commit, recv_commit) = channel();
        let (send_answer, recv_answer) = channel();
        let spt = Arc::new(pt);

        let _reader = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for transactions in recv_transactions.iter() {
                    let octx = recycler.allocate();
                    {
                        let mut ctx = octx.write().unwrap();
                        lpt.acquire_validate_find(&transactions, &mut ctx);
                        lpt.allocate_keys_with_ctx(&transactions, &mut ctx);
                        lpt.load_pages_with_ctx(&transactions, &mut ctx);
                    }
                    send_execute.send((transactions, octx)).unwrap();
                }
            })
        };
        let _executor = {
            spawn(move || {
                for (transactions, octx) in recv_execute.iter() {
                    {
                        let mut ctx = octx.write().unwrap();
                        PageTable::execute_with_ctx(&transactions, &mut ctx);
                    }
                    send_commit.send((transactions, octx)).unwrap();
                }
            })
        };
        let _commiter = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for (transactions, octx) in recv_commit.iter() {
                    {
                        let ctx = octx.read().unwrap();
                        lpt.commit_release_with_ctx(&transactions, &ctx);
                        send_answer.send(()).unwrap();
                    }
                    recycler.recycle(octx);
                }
            })
        };
        //warmup
        let tt = ttx.pop().unwrap();
        send_transactions.send(tt).unwrap();
        recv_answer.recv().unwrap();

        let start = Instant::now();
        for _ in 1..count {
            let tt = ttx.pop().unwrap();
            send_transactions.send(tt).unwrap();
        }
        for _ in 1..count {
            recv_answer.recv().unwrap();
        }
        let done = start.elapsed();
        let ns = done.as_secs() as usize * 1_000_000_000 + done.subsec_nanos() as usize;
        let total = count * N;
        println!(
            "PIPELINE: done {:?} {}ns/packet {}ns/t {} tp/s",
            done,
            ns / (count - 1),
            ns / total,
            (1_000_000_000 * total) / ns
        );
    }
    #[test]
    fn load_and_execute_par_pipeline_bench() {
        logger::setup();
        let context_recycler = ContextRecycler::default();
        let pt = PageTable::new();
        let count = 10000;
        let mut ttx: Vec<Vec<Call>> = (0..count)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for tx in &ttx {
            pt.force_allocate(tx, true, 1_000_000);
        }
        let (send_transactions, recv_transactions) = channel();
        let (send_execute, recv_execute) = channel();
        let (send_commit, recv_commit) = channel();
        let (send_answer, recv_answer) = channel();
        let spt = Arc::new(pt);

        let _reader = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for transactions in recv_transactions.iter() {
                    let octx = recycler.allocate();
                    {
                        let mut ctx = octx.write().unwrap();
                        lpt.acquire_validate_find(&transactions, &mut ctx);
                        lpt.allocate_keys_with_ctx(&transactions, &mut ctx);
                        lpt.load_pages_with_ctx(&transactions, &mut ctx);
                    }
                    send_execute.send((transactions, octx)).unwrap();
                }
            })
        };
        let _executor = {
            spawn(move || {
                while let Ok(tx) = recv_execute.recv() {
                    let mut events = VecDeque::new();
                    events.push_back(tx);
                    while let Ok(more) = recv_execute.try_recv() {
                        events.push_back(more);
                    }
                    events.par_iter().for_each(|(transactions, octx)| {
                        let mut ctx = octx.write().unwrap();
                        PageTable::execute_with_ctx(&transactions, &mut ctx);
                    });
                    send_commit.send(events).unwrap();
                }
            })
        };
        let _commiter = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for mut events in recv_commit.iter() {
                    for (transactions, octx) in &events {
                        let ctx = octx.read().unwrap();
                        lpt.commit_release_with_ctx(transactions, &ctx);
                    }
                    send_answer.send(events.len()).unwrap();
                    while let Some((_, octx)) = events.pop_front() {
                        recycler.recycle(octx);
                    }
                }
            })
        };
        let _sender = {
            spawn(move || {
                while let Some(tt) = ttx.pop() {
                    send_transactions.send(tt).unwrap();
                }
            })
        };
        //warmup
        recv_answer.recv().unwrap();
        let start = Instant::now();
        let mut total = 1;
        while total < count {
            total += recv_answer.recv().unwrap();
        }
        let done = start.elapsed();
        let ns = done.as_secs() as usize * 1_000_000_000 + done.subsec_nanos() as usize;
        let total = count * N;
        println!(
            "PAR_PIPELINE: done {:?} {}ns/packet {}ns/t {} tp/s",
            done,
            ns / (count - 1),
            ns / total,
            (1_000_000_000 * total) / ns
        );
    }
    pub fn load_and_execute_mt_bench(max_threads: usize) {
        let pt = PageTable::new();
        let count = 10000;
        let mut ttx: Vec<Vec<Call>> = (0..count)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for tx in &ttx {
            pt.force_allocate(tx, true, 1_000_000);
        }
        let (send_answer, recv_answer) = channel();
        let spt = Arc::new(pt);
        let threads: Vec<_> = (0..max_threads)
            .map(|_| {
                let (send, recv) = channel();
                let response = send_answer.clone();
                let lpt = spt.clone();
                let t = spawn(move || {
                    let mut ctx = Context::default();
                    for transactions in recv.iter() {
                        lpt.acquire_validate_find(&transactions, &mut ctx);
                        lpt.allocate_keys_with_ctx(&transactions, &mut ctx);
                        lpt.load_pages_with_ctx(&transactions, &mut ctx);
                        PageTable::execute_with_ctx(&transactions, &mut ctx);
                        lpt.commit_release_with_ctx(&transactions, &ctx);
                        response.send(()).unwrap();
                    }
                });
                (t, send)
            })
            .collect();
        let _sender = {
            spawn(move || {
                for thread in 0..count {
                    let tt = ttx.pop().unwrap();
                    threads[thread % max_threads].1.send(tt).unwrap();
                }
            })
        };
        //warmup
        for _ in 0..max_threads {
            recv_answer.recv().unwrap();
        }

        let start = Instant::now();
        for _thread in max_threads..count {
            recv_answer.recv().unwrap();
        }
        let done = start.elapsed();
        let ns = done.as_secs() as usize * 1_000_000_000 + done.subsec_nanos() as usize;
        let total = (count - max_threads) * N;
        println!(
            "MT-{}: done {:?} {}ns/packet {}ns/t {} tp/s",
            max_threads,
            done,
            ns / (count - max_threads),
            ns / total,
            (1_000_000_000 * total) / ns
        );
    }
    #[test]
    fn load_and_execute_mt_benches() {
        load_and_execute_mt_bench(1);
        load_and_execute_mt_bench(2);
        load_and_execute_mt_bench(3);
        load_and_execute_mt_bench(4);
        load_and_execute_mt_bench(8);
        load_and_execute_mt_bench(16);
        load_and_execute_mt_bench(32);
    }
}

#[cfg(all(feature = "unstable", test))]
mod bench {
    extern crate test;
    use self::test::Bencher;
    use packet::Recycler;
    use page_table::{self, Call, Context, PageTable, N};
    use rand::{thread_rng, RngCore};
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread::spawn;

    #[bench]
    fn update_version_baseline(bencher: &mut Bencher) {
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
        });
    }
    #[bench]
    fn mem_lock(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        let mut lock = vec![false; N];
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn validate_call_miss(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let fill_table: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&fill_table, true, 1_000_000);
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_call(&transactions, &lock, &mut checked);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn validate_call_hit(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut checked = vec![false; N];
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_call(&transactions, &lock, &mut checked);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn find_new_keys_needs_alloc(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    }
    #[bench]
    fn find_new_keys_no_alloc(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    }
    #[bench]
    fn allocate_new_keys_some_new_allocs(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }

            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    }
    #[bench]
    fn allocate_new_keys_no_new_allocs(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    }
    #[bench]
    fn load_pages(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.load_pages_with_ctx(&transactions, &mut ctx);
            pt.release_memory_lock(&transactions, &ctx.lock);
        });
    }
    #[bench]
    fn load_and_execute(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut transactions: Vec<_> = (0..N).map(|_r| Call::random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut ctx = Context::default();
        bencher.iter(move || {
            for tx in &mut transactions {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.load_pages_with_ctx(&transactions, &mut ctx);
            PageTable::execute_with_ctx(&transactions, &mut ctx);
            pt.commit_release_with_ctx(&transactions, &ctx);
        });
    }

    #[bench]
    fn load_and_execute_large_table(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let mut ttx: Vec<Vec<_>> = (0..N)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for transactions in &ttx {
            pt.force_allocate(transactions, true, 1_000_000);
        }
        page_table::test::load_and_execute();
        let mut ctx = Context::default();
        bencher.iter(move || {
            let transactions = &mut ttx[thread_rng().next_u64() as usize % N];
            for tx in transactions.iter_mut() {
                tx.version += 1;
            }
            pt.acquire_validate_find(&transactions, &mut ctx);
            pt.allocate_keys_with_ctx(&transactions, &mut ctx);
            pt.load_pages_with_ctx(&transactions, &mut ctx);
            PageTable::execute_with_ctx(&transactions, &mut ctx);
            pt.commit_release_with_ctx(&transactions, &ctx);
        });
    }
    #[bench]
    fn load_and_execute_mt3_experimental(bencher: &mut Bencher) {
        const T: usize = 3;
        let pt = PageTable::new();
        let count = 10000;
        let mut ttx: Vec<Vec<Call>> = (0..count)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for tx in &ttx {
            pt.force_allocate(tx, true, 1_000_000);
        }
        let (send_answer, recv_answer) = channel();
        let spt = Arc::new(pt);
        let threads: Vec<_> = (0..T)
            .map(|_| {
                let (send, recv) = channel();
                let response = send_answer.clone();
                let lpt = spt.clone();
                let t = spawn(move || {
                    let mut ctx = Context::default();
                    for transactions in recv.iter() {
                        lpt.acquire_validate_find(&transactions, &mut ctx);
                        lpt.allocate_keys_with_ctx(&transactions, &mut ctx);
                        lpt.load_pages_with_ctx(&transactions, &mut ctx);
                        PageTable::execute_with_ctx(&transactions, &mut ctx);
                        lpt.commit_release_with_ctx(&transactions, &ctx);
                        response.send(()).unwrap();
                    }
                });
                (t, send)
            })
            .collect();
        //warmup
        for thread in 0..T {
            let tt = ttx.pop().unwrap();
            threads[thread].1.send(tt).unwrap();
        }
        for _ in 0..T {
            recv_answer.recv().unwrap();
        }
        let mut thread = 0;
        bencher.iter(move || {
            for thread in 0..T {
                let tt = ttx.pop().unwrap();
                threads[thread].1.send(tt).unwrap();
            }
            for _ in 0..T {
                recv_answer.recv().unwrap();
            }
            thread += T;
        });
    }
    type ContextRecycler = Recycler<Context>;
    #[bench]
    fn load_and_execute_pipeline_experimental(bencher: &mut Bencher) {
        let context_recycler = ContextRecycler::default();
        let pt = PageTable::new();
        let count = 10000;
        const STAGES: usize = 3;
        let mut ttx: Vec<Vec<Call>> = (0..count)
            .map(|_| (0..N).map(|_r| Call::random_tx()).collect())
            .collect();
        for tx in &ttx {
            pt.force_allocate(tx, true, 1_000_000);
        }
        let (send_transactions, recv_transactions) = channel();
        let (send_execute, recv_execute) = channel();
        let (send_commit, recv_commit) = channel();
        let (send_answer, recv_answer) = channel();
        let spt = Arc::new(pt);

        let _reader = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for transactions in recv_transactions.iter() {
                    let octx = recycler.allocate();
                    {
                        let mut ctx = octx.write().unwrap();
                        lpt.acquire_validate_find(&transactions, &mut ctx);
                        lpt.allocate_keys_with_ctx(&transactions, &mut ctx);
                        lpt.load_pages_with_ctx(&transactions, &mut ctx);
                    }
                    send_execute.send((transactions, octx)).unwrap();
                }
            })
        };
        let _executor = {
            spawn(move || {
                for (transactions, octx) in recv_execute.iter() {
                    {
                        let mut ctx = octx.write().unwrap();
                        PageTable::execute_with_ctx(&transactions, &mut ctx);
                    }
                    send_commit.send((transactions, octx)).unwrap();
                }
            })
        };
        let _commiter = {
            let lpt = spt.clone();
            let recycler = context_recycler.clone();
            spawn(move || {
                for (transactions, octx) in recv_commit.iter() {
                    {
                        let ctx = octx.read().unwrap();
                        lpt.commit_release_with_ctx(&transactions, &ctx);
                        send_answer.send(()).unwrap();
                    }
                    recycler.recycle(octx);
                }
            })
        };
        //warmup
        let tt = ttx.pop().unwrap();
        send_transactions.send(tt).unwrap();
        recv_answer.recv().unwrap();

        //fill the stages
        for _ in 0..STAGES {
            let tt = ttx.pop().unwrap();
            send_transactions.send(tt).unwrap();
        }
        bencher.iter(move || {
            let tt = ttx.pop().unwrap();
            send_transactions.send(tt).unwrap();
            recv_answer.recv().unwrap();
        });
    }
}
