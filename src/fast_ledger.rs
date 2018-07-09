use bincode::serialize;
use result::Result;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Mutex, RwLock};

//dummy defs
type Hash = u64;
type PublicKey = u64;
type Signature = u64;

pub struct StateMachine {
    /// append only array of Call structures
    record: Record,
    /// state machine for transactions and contract memory
    _page_table: PageTable,
    /// append only array of PohEntry structures
    poh: PoH,
}

/// Generic Page for the PageTable
#[repr(C)]
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
    /// hash of the page data
    memhash: Hash,
    /// The following could be in a separate structure
    /// size of the page
    size: u64,
    /// when signing this is 0
    /// this is the pointer to allocated memory on the system for this page
    pointer: u64,
}
impl Default for Page {
    fn default() -> Page {
        Page {
            owner: 0,
            contract: 0,
            balance: 0,
            memhash: 0,
            size: 0,
            pointer: 0,
        }
    }
}
/// Call definition
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Call {
    /// proof of `caller` key owndershp over the whole structure
    signature: Signature,
    /// owner address, aka from in a simple transaction
    owner: PublicKey,
    /// the address of the program we want to call
    contract: PublicKey,
    /// amount to send to the contract
    amount: u64,
    /// OS scheduling fee
    fee: u64,
    /// method to call in the contract
    method: u8,
    /// number of keys to load, aka the to key
    inkeys: u8,
    /// usedata in bytes
    num_userdata: u8,
    /// number of spends expected in this call
    num_spends: u8,
    /// number of proofs of ownership of `inkeys`, `owner` is proven by the signature
    num_proofs: u8,
    /// Sender appends an array of PublicKeys, Signature, and key indexies of size `numproofs`
    /// {Call,[PublicKey; inkeys],[Sig; num_proofs],[u8; num_proofs],[userdata]}
    /// unused
    unused: [u8; 3],
}

/// simple transaction over a Call
/// TODO: The pipeline will need to pass the `destination` public keys in a side buffer to generalize
/// this
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Tx {
    call: Call,             //inkeys = 1, numspends = 1, numproofs = 0
    destination: PublicKey, //simple defintion that makes it easy to define the rest of the pipeline for a simple Tx
}

pub struct Record {
    /// a slice of Call, that is appended to
    records: BufWriter<File>,
    height: usize,
}

impl Record {
    /// write a blob to the record, and return the hash of the blob and the index at which it was
    /// inserted into the record
    pub fn insert(&mut self, blob: &Vec<Tx>) -> Result<(Hash, u64)> {
        let len = blob.len();
        //TODO: compute real hash
        let hash = self.height;
        let height = self.height;
        let bytes = serialize(&blob).unwrap();
        self.records.write(&bytes)?;
        self.height += len;
        // returns the hash of the blob
        return Ok((hash as Hash, height as u64));
    }
}

pub struct AllocatedPages {
    max: u64,
    allocated: BTreeMap<PublicKey, u64>,
    free: Vec<u64>,
}

impl AllocatedPages {
    pub fn new() -> Self {
        AllocatedPages {
            max: 0,
            allocated: BTreeMap::new(),
            free: vec![],
        }
    }
    pub fn lookup(&self, key: &PublicKey) -> Option<u64> {
        self.allocated.get(key).cloned()
    }
    pub fn free(&mut self, key: &PublicKey) {
        let page = self.lookup(key).unwrap();
        self.free.push(page);
        self.allocated.remove(key);
    }
    pub fn allocate(&mut self, key: PublicKey) -> u64 {
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
    page_table: Vec<Page>,
    /// a map from page public keys, to index into the page_table
    allocated_pages: RwLock<AllocatedPages>,
    mem_locks: Mutex<HashSet<PublicKey>>,
}

impl PageTable {
    pub fn new() -> Self {
        PageTable {
            page_table: vec![],
            allocated_pages: RwLock::new(AllocatedPages::new()),
            mem_locks: Mutex::new(HashSet::new()),
        }
    }
    pub fn acquire_memory_lock(&self, packet: &Vec<Tx>, acquired_memory: &mut Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, p) in packet.iter().enumerate() {
            let collision = mem_locks.contains(&p.call.owner) || mem_locks.contains(&p.destination);
            acquired_memory[i] = !collision;
            if !collision {
                mem_locks.insert(p.call.owner);
                mem_locks.insert(p.destination);
            }
        }
    }
    pub fn validate_debits(
        &mut self,
        packet: &Vec<Tx>,
        acquired_memory: &Vec<bool>,
        from_pages: &mut Vec<Option<*mut Page>>,
    ) {
        //holds page table READ lock
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            from_pages[i] = None;
            if !acquired_memory[i] {
                continue;
            }
            if let Some(memix) = allocated_pages.lookup(&tx.call.owner) {
                let mem = self.page_table.get_mut(memix as usize);
                if let Some(m) = mem {
                    if m.balance >= tx.call.fee + tx.call.amount {
                        from_pages[i] = Some(m as *mut Page);
                    }
                }
            }
        }
    }
    pub fn find_new_keys(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<*mut Page>>,
        to_pages: &mut Vec<Option<*mut Page>>,
    ) {
        //holds READ lock to the page table
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            to_pages[i] = None;
            if from_pages[i].is_none() {
                continue;
            }
            if let Some(memix) = allocated_pages.lookup(&tx.destination) {
                if let Some(m) = self.page_table.get_mut(memix as usize) {
                    to_pages[i] = Some(m as *mut Page);
                }
            }
        }
    }
    #[cfg(test)]
    pub fn force_allocate(&mut self, packet: &Vec<Tx>, owner: bool, amount: u64) {
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        for tx in packet.iter() {
            let key = if owner {
                tx.call.owner
            } else {
                tx.destination
            };
            let page = Page {
                owner: key,
                contract: tx.call.contract,
                balance: amount,
                size: 0,
                pointer: 0,
                memhash: 0,
            };
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
        txs: &Vec<Tx>,
        from_pages: &Vec<Option<*mut Page>>,
        to_pages: &Vec<Option<*mut Page>>,
    ) {
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        for (i, tx) in txs.iter().enumerate() {
            if from_pages[i].is_some() {
                assert_eq!(
                    from_pages[i].unwrap() as *const Page,
                    &self.page_table[self.allocated_pages
                                         .read()
                                         .unwrap()
                                         .lookup(&tx.call.owner)
                                         .unwrap() as usize] as *const Page
                );
            }
            if to_pages[i].is_some() {
                assert_eq!(
                    to_pages[i].unwrap() as *const Page,
                    &self.page_table[self.allocated_pages
                                         .read()
                                         .unwrap()
                                         .lookup(&tx.destination)
                                         .unwrap() as usize] as *const Page
                );
            }
        }
    }
    pub fn allocate_keys(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<*mut Page>>,
        to_pages: &mut Vec<Option<*mut Page>>,
    ) {
        //holds WRITE lock to the page table
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        for (i, tx) in packet.iter().enumerate() {
            if from_pages[i].is_none() {
                continue;
            }
            if to_pages[i].is_some() {
                continue;
            }
            let page = Page {
                owner: tx.destination,
                contract: tx.call.contract,
                balance: 0,
                size: 0,
                pointer: 0,
                memhash: 0,
            };
            let ix = allocated_pages.allocate(tx.destination) as usize;
            if self.page_table.len() <= ix {
                self.page_table.resize(ix + 1, Page::default());
            }
            self.page_table[ix] = page;
            to_pages[i] = Some(&mut self.page_table[ix] as *mut Page);
        }
    }
    pub fn move_funds(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &mut Vec<Option<*mut Page>>,
        to_pages: &mut Vec<Option<*mut Page>>,
    ) {
        //holds page table read lock
        let _allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if let (Some(from), Some(to)) = (from_pages[i], to_pages[i]) {
                unsafe {
                    //todo: only move fee here, move the amount as part of `num_spends`
                    (*from).balance -= tx.call.amount + tx.call.fee;
                    assert_eq!((*from).balance, 8);
                    assert_eq!((*from).owner, tx.call.owner);
                    assert_eq!((*to).owner, tx.destination);
                    (*to).balance += tx.call.amount;
                }
            }
        }
    }
    fn _load_memory(
        &self,
        _packet: &Vec<Tx>,
        _from_pages: &Vec<Option<&Page>>,
        _to_pages: &Vec<Option<&Page>>,
    ) {
        //TBD
    }
    fn _execute_call(
        &self,
        _packet: &Vec<Tx>,
        _from_pages: &Vec<Option<&Page>>,
        _to_pages: &Vec<Option<&Page>>,
    ) {
        //TBD
    }
    fn _write_memory(
        &self,
        _packet: &Vec<Tx>,
        _from_pages: &Vec<Option<&Page>>,
        _to_pages: &Vec<Option<&Page>>,
    ) {
        //TBD
    }
    pub fn release_memory_lock(&self, packet: &Vec<Tx>, lock: &Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, p) in packet.iter().enumerate() {
            if !lock[i] {
                continue;
            }
            mem_locks.remove(&p.call.owner);
            mem_locks.remove(&p.destination);
        }
    }
    /// fill up to the blob
    pub fn fill_blob(
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &Vec<Option<&Page>>,
        blob: &mut Vec<Tx>,
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

pub struct PoH {
    /// number of hashes to produce for each entry
    num_hashes: u64,
    poh_file: BufWriter<File>,
    bytes_written: usize,
    sender: Sender<(Hash, u64)>,
    receiver: Receiver<(Hash, u64)>,
    hash: Hash,
    entries_written: usize,
}

/// Points to indexes into the Record structure
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct PohEntry {
    /// hash produced by PoH process
    poh_hash: Hash,
    /// when current == previous, this means its an empty entry
    record_index: u64,
}

impl PoH {
    /// mix in the hash and record offset into the poh_file
    pub fn insert(&mut self, record_hash: Hash, record_index: u64) -> Result<()> {
        self.sender.send((record_hash, record_index))?;
        Ok(())
    }
    //separate thread calls this
    pub fn generator(&mut self) -> Result<()> {
        let mut last_record_index;
        loop {
            //TODO: check if this slower then a Mutex<VecDeque>
            let input = self.receiver.try_recv();
            let mixin_hash;
            if let Ok((in_hash, in_index)) = input {
                mixin_hash = in_hash;
                last_record_index = in_index;
            } else {
                //TODO:  should we keep the last value for the record index to indicate 0 bytes
                //have been added to the record at this point?
                mixin_hash = 0; //or last?
                last_record_index = 0; //or last
            }
            self.hash += mixin_hash;
            for _ in 0..self.num_hashes {
                // do the sha256 loop now, with mixin_hash as the seed
                // fake poh for now, mix in `mixin_hash` for actual PoH
                self.hash += 1;
            }
            let entry = PohEntry {
                poh_hash: self.hash,
                record_index: last_record_index,
            };
            let bytes = serialize(&entry).unwrap();
            self.bytes_written += bytes.len();
            self.entries_written += 1;
            self.poh_file.write(&bytes)?;
        }
    }
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl StateMachine {
    pub fn output(&mut self, blob: &Vec<Tx>) -> Result<()> {
        let (hash, pos) = self.record.insert(blob)?;
        self.poh.insert(hash, pos as u64)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use fast_ledger::{Call, PageTable, Tx};
    use rand;
    use rand::RngCore;
    const N: usize = 2;

    fn random_tx() -> Tx {
        Tx {
            call: Call {
                signature: 0,
                owner: rand::thread_rng().next_u64(),
                contract: rand::thread_rng().next_u64(),
                amount: 1,
                fee: 1,
                method: 0,
                inkeys: 1,
                num_userdata: 0,
                num_spends: 1,
                num_proofs: 0,
                unused: [0u8; 3],
            },
            destination: rand::thread_rng().next_u64(),
        }
    }
    #[test]
    fn mem_lock() {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
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
    fn validate_debits_miss() {
        let mut pt = PageTable::new();
        let fill_table: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&fill_table, true, 1_000_000);
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        for x in &from_pages {
            assert!(x.is_none());
        }
    }
    #[test]
    fn validate_debits_hit() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        for x in &from_pages {
            assert!(x.is_some());
        }
    }
    #[test]
    fn find_new_keys_all() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        let mut to_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
        for x in &to_pages {
            assert!(x.is_none());
        }
    }
    #[test]
    fn find_new_keys_none() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        let mut to_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
        for x in &to_pages {
            assert!(x.is_some());
        }
    }
    #[test]
    fn allocate_new_keys_all() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        let mut to_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
        for x in &to_pages {
            assert!(x.is_some());
        }
    }
    #[test]
    fn allocate_new_keys_none() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        let mut to_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
        for x in &to_pages {
            assert!(x.is_some());
        }
    }
    #[test]
    fn move_funds() {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 10);
        let mut lock = vec![false; N];
        let mut from_pages = vec![None; N];
        let mut to_pages = vec![None; N];
        pt.acquire_memory_lock(&transactions, &mut lock);
        pt.validate_debits(&transactions, &lock, &mut from_pages);
        pt.check_pages(&transactions, &to_pages, &from_pages);
        pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
        pt.check_pages(&transactions, &to_pages, &from_pages);
        pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
        pt.check_pages(&transactions, &to_pages, &from_pages);
        pt.move_funds(&transactions, &mut from_pages, &mut to_pages);
        pt.check_pages(&transactions, &to_pages, &from_pages);
        for (i, x) in transactions.iter().enumerate() {
            assert!(to_pages[i].is_some());
            assert!(from_pages[i].is_some());
            let dx: u64 = pt.allocated_pages
                .read()
                .unwrap()
                .lookup(&x.destination)
                .unwrap();
            assert_eq!(pt.page_table[dx as usize].balance, x.call.amount);
            let sx: u64 = pt.allocated_pages
                .read()
                .unwrap()
                .lookup(&x.call.owner)
                .unwrap();
            assert_eq!(
                pt.page_table[sx as usize].balance,
                10 - (x.call.amount + x.call.fee)
            );
        }
    }
}

#[cfg(all(feature = "unstable", test))]
mod bench {
    extern crate test;
    use self::test::Bencher;
    use fast_ledger::{Call, PageTable, Tx};
    use rand;
    use rand::RngCore;
    const N: usize = 256;

    fn random_tx() -> Tx {
        Tx {
            call: Call {
                signature: 0,
                owner: rand::thread_rng().next_u64(),
                contract: rand::thread_rng().next_u64(),
                amount: 1,
                fee: 1,
                method: 0,
                inkeys: 1,
                num_userdata: 0,
                num_spends: 1,
                num_proofs: 0,
                unused: [0u8; 3],
            },
            destination: rand::thread_rng().next_u64(),
        }
    }
    #[bench]
    fn bench_mem_lock(bencher: &mut Bencher) {
        let pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        bencher.iter(move || {
            let mut lock = vec![false; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_validate_debits_miss(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let fill_table: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&fill_table, true, 1_000_000);
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_validate_debits_hit(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_find_new_keys_all(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            let mut to_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_find_new_keys_none(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            let mut to_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_allocate_new_keys_all(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            let mut to_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_allocate_new_keys_none(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        pt.force_allocate(&transactions, false, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            let mut to_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
    #[bench]
    fn bench_move_funds(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        let transactions: Vec<_> = (0..N).map(|_r| random_tx()).collect();
        pt.force_allocate(&transactions, true, 1_000_000);
        bencher.iter(move || {
            let mut lock = vec![false; N];
            let mut from_pages = vec![None; N];
            let mut to_pages = vec![None; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.validate_debits(&transactions, &lock, &mut from_pages);
            pt.find_new_keys(&transactions, &from_pages, &mut to_pages);
            pt.allocate_keys(&transactions, &from_pages, &mut to_pages);
            pt.move_funds(&transactions, &mut from_pages, &mut to_pages);
            pt.release_memory_lock(&transactions, &lock);
        });
    }
}
