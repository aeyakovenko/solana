use bincode::serialize;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Mutex, RwLock};

//dummy defs
type Hash = u64;
type PublicKey = u64;
type Signature = u64;

struct StateMachine {
    /// append only array of Call structures
    record: Record,
    /// state machine for transactions and contract memory
    page_table: PageTable,
    /// append only array of PohEntry structures
    poh: PoH,
}

/// Generic Page for the PageTable
#[repr(C)]
#[derive(Clone)]
struct Page {
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
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Call {
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
    /// {Call,[PublicKey; inkeys],[userdata],[Sig; num_proofs],[u8; num_proofs]}
    /// unused
    unused: [u8; 3],
}

/// simple transaction over a Call
/// TODO: The pipeline will need to pass the `destination` public keys in a side buffer to generalize
/// this
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct Tx {
    call: Call,             //inkeys = 1, numspends = 1, numproofs = 0
    destination: PublicKey, //simple defintion that makes it easy to define the rest of the pipeline for a simple Tx
}

struct Record {
    /// a slice of Call, that is appended to
    records: BufWriter<File>,
    height: usize,
}

impl Record {
    /// write a blob to the record, and return the hash of the blob and the index at which it was
    /// inserted into the record
    pub fn insert(&self, blob: &Vec<Tx>) -> (Hash, u64) {
        let len = blob.len();
        //TODO: compute real hash
        let hash = self.height;
        let height = self.height;
        let bytes = serialize(&blob).unwrap();
        self.records.write(&bytes);
        self.height += len;
        // returns the hash of the blob
        return (hash as Hash, height as u64);
    }
}

struct AllocatedPages {
    max: u64,
    allocated: BTreeMap<PublicKey, u64>,
    free: Vec<u64>,
}
impl AllocatedPages {
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
struct PageTable {
    /// Vec<Page> over a large array of [Page]
    page_table: Vec<Page>,
    /// a map from page public keys, to index into the page_table
    allocated_pages: RwLock<AllocatedPages>,
    mem_locks: Mutex<HashSet<PublicKey>>,
}

impl PageTable {
    fn acquire_memory_lock(&self, packet: &Vec<Tx>, acquired_memory: &mut Vec<bool>) {
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
    fn validate_debits<'a>(
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
    fn find_new_keys<'a>(
        &'a self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &Vec<Option<&'a mut Page>>,
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
                    to_pages[i] = Some(m);
                }
            }
        }
    }
    fn allocate_keys(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&Page>>,
        to_pages: &Vec<Option<&Page>>,
    ) {
        //holds WRITE lock to the page table
        let allocated_pages = self.allocated_pages.write().unwrap();
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
    fn move_funds(
        &mut self,
        packet: &Vec<Tx>,
        from_pages: &Vec<Option<&mut Page>>,
        to_pages: &Vec<Option<&mut Page>>,
    ) {
        //holds page table read lock
        let _allocated_pages = self.allocated_pages.read().unwrap();
        for (i, tx) in packet.iter().enumerate() {
            if from_pages[i].is_none() {
                continue;
            }
            if to_pages[i].is_none() {
                continue;
            }
            from_pages[i].unwrap().balance -= tx.call.amount;
            to_pages[i].unwrap().balance += tx.call.amount;
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
    fn release_memory_lock(&self, packet: &Vec<Tx>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for p in packet.iter() {
            mem_locks.remove(&p.call.caller);
            mem_locks.remove(&p.destination);
        }
    }
}
struct PoH {
    /// number of hashes to produce for each entry
    num_hashes: u64,
    poh_file: BufWriter<File>,
    bytes_written: usize,
    sender: Sender<(Hash, u64)>,
    receiver: Receiver<(Hash, u64)>,
    hash: Hash,
    height: usize,
}

/// Points to indexes into the Record structure
#[repr(C)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct PohEntry {
    /// hash produced by PoH process
    poh_hash: Hash,
    /// when current == previous, this means its an empty entry
    record_index: u64,
}

impl PoH {
    /// mix in the hash and record offset into the poh_file
    pub fn insert(&mut self, record_hash: Hash, record_index: u64) {
        self.sender.send((record_hash, record_index));
    }
    //separate thread calls this
    pub fn generator(&self) {
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
            self.poh_file.write(&bytes);
        }
    }
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl StateMachine {
    /// fill up to the blob
    fn fill_blob(
        &self,
        packet: &Vec<Tx>,
        has_funds: &Vec<bool>,
        blob: &mut Vec<Tx>,
        filled: &mut usize,
    ) {
        //nolock
        for (i, tx) in packet.iter().enumerate() {
            if !has_funds[i] {
                continue;
            }
            blob[*filled] = *tx;
            *filled += 1;
            if *filled == blob.len() {
                return;
            }
        }
    }
    fn output(&self, blob: &Vec<Tx>) {
        let (hash, pos) = self.record.insert(blob);
        self.poh.insert(hash, pos as u64);
    }
}
