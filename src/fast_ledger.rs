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

/// Call definition
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Call {
    /// proof of `caller` key owndershp over the whole structure
    signature: Signature,
    /// caller address, aka from in a simple transaction
    caller: PublicKey,
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
    /// number of proofs of ownership of `inkeys`, `caller` is proven by the signature
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

struct AllocatedPages {
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
            let collision =
                mem_locks.contains(&p.call.caller) || mem_locks.contains(&p.destination);
            acquired_memory[i] = !collision;
            if !collision {
                mem_locks.insert(p.call.caller);
                mem_locks.insert(p.destination);
            }
        }
    }
    pub fn validate_debits<'a>(
        &'a self,
        packet: &Vec<Tx>,
        acquired_memory: &Vec<bool>,
        from_pages: &mut Vec<Option<&'a Page>>,
    ) {
        //holds page table READ lock
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            from_pages[i] = None;
            if !acquired_memory[i] {
                continue;
            }
            if let Some(memix) = allocated_pages.lookup(&tx.call.caller) {
                let mem = self.page_table.get(memix as usize);
                if let Some(m) = mem {
                    if m.balance > tx.call.amount {
                        from_pages[i] = Some(m);
                    }
                }
            }
        }
    }
    pub fn find_new_keys<'a>(
        &'a self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &mut Vec<Option<&'a Page>>,
    ) {
        //holds READ lock to the page table
        let allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            to_pages[i] = None;
            if from_pages[i].is_none() {
                continue;
            }
            if let Some(memix) = allocated_pages.lookup(&tx.destination) {
                if let Some(m) = self.page_table.get(memix as usize) {
                    to_pages[i] = Some(m);
                }
            }
        }
    }
    #[cfg(test)]
    pub fn from_allocate(&mut self, packet: &Vec<Tx>, caller: bool) {
        let mut allocated_pages = self.allocated_pages.write().unwrap();
        for tx in packet.iter() {
            let key = if caller {
                tx.call.caller
            } else {
                tx.destination
            };
            let amount = if caller {
                tx.call.amount + tx.call.fee
            } else {
                0
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
    pub fn allocate_keys(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &mut Vec<Option<&Page>>,
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
                self.page_table.resize(ix + 1, page);
            } else {
                self.page_table[ix] = page;
            }
        }
    }
    pub fn move_funds(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &mut Vec<Option<&mut Page>>,
        to_pages: &mut Vec<Option<&mut Page>>,
    ) {
        //holds page table read lock
        let _allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if let (Some(ref mut from), Some(ref mut to)) = (&mut from_pages[i], &mut to_pages[i]) {
                from.balance -= tx.call.amount;
                to.balance += tx.call.amount;
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
            mem_locks.remove(&p.call.caller);
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
        let mut last_record_index = 0;
        loop {
            //TODO: check if this slower then a Mutex<VecDeque>
            let input = self.receiver.try_recv();
            let mut mixin_hash = 0;
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
        Ok(())
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

#[cfg(all(feature = "unstable", test))]
mod bench {
    extern crate test;
    use self::test::Bencher;
    use fast_ledger::{Call, PageTable, Tx};
    use rand;
    use rand::RngCore;
    #[bench]
    fn bench_fast_ledger(bencher: &mut Bencher) {
        let mut pt = PageTable::new();
        const N: usize = 1_000;
        let transactions: Vec<_> = (0..N)
            .map(|r| Tx {
                call: Call {
                    signature: 0,
                    caller: rand::thread_rng().next_u64(),
                    contract: rand::thread_rng().next_u64(),
                    amount: 1,
                    fee: 0,
                    method: 0,
                    inkeys: 1,
                    num_userdata: 0,
                    num_spends: 1,
                    num_proofs: 0,
                    unused: [0u8; 3],
                },
                destination: rand::thread_rng().next_u64(),
            })
            .collect();
        bencher.iter(move || {
            let mut lock = vec![false; N];
            pt.acquire_memory_lock(&transactions, &mut lock);
            pt.release_memory_lock(&transactions, &lock);
        });;

    }
}
