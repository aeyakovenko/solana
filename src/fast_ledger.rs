use hasht;
use memmap;

//dummy defs
type Hash = usize;
type PublicKey = usize;
type Signature = usize;

struct StateMachine {
    /// append only array of Call structures
    record: Record,
    /// state machine for transactions and contract memory
    page_table: PageTable,
    /// append only array of PoHEntry structures
    poh: PoH,
}

/// Generic Page for the PageTable
#[crepr]
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
    /// size of the page
    size: u64,
    /// when signing this is 0
    /// this is the pointer to allocated memory on the system for this page
    pointer: u64,
}

/// Call definition
#[crepr]
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
    /// number of spends expected in this call
    numspends; u8,
    /// number of proofs of ownership of `inkeys`, `caller` is proven by the signature
    numproofs; u8,
    /// Sender appends an array of PublicKeys, Signature, and key indexies of size `numproofs`
    /// {Call,[PublicKey; inkeys],[Sig; numproofs],[u8; numproofs]}
}

/// simple transaction over a Call
/// TODO: The pipeline will need to pass the `destination` public keys in a side buffer to generalize
/// this
#[crepr]
struct Tx {
    call: Call, //inkeys = 1, numspends = 1, numproofs = 0
    destination: PublicKey, //simple defintion that makes it easy to define the rest of the pipeline for a simple Tx
}
 
/// simple single memory hashtable implementation
impl hasht::Key for PublicKey {
    fn start(&self) -> usize {
        self[0]
            | (self[1] << (1 * 8))
            | (self[2] << (2 * 8))
            | (self[3] << (3 * 8))
            | (self[4] << (4 * 8))
            | (self[5] << (5 * 8))
            | (self[6] << (6 * 8))
            | (self[7] << (7 * 8))
    }
    fn unused(&self) -> bool {
        *self == 0usize
    }
}

impl hasht::Val<PublicKey> for Page {
    fn key(&self) -> &PublicKey {
        &self.owner
    }
}
type PageT = hasht::HashT<PublicKey, Page>;

struct Record {
    /// a slice of Call, that is appended to
    records: BufWriter,
    height: usize,
}

impl Record {
    /// write a blob to the record, and return the hash of the blob and the index at which it was
    /// inserted into the record
    pub fn insert(&self, blob: &Vec<Tx>) -> (Hash, usize) {
        let len = blob.len();
        //TODO: compute real hash
        let hash = self.height;
        let height = self.height;
        self.records.write_all(&blob);
        self.height += len;
        // returns the hash of the blob 
        return (hash, height);
    }
}

struct PageTable {
    /// HashT over a large array of [Page]
    page_table: memmap::MmapMut,
    table_lock: RwLock<bool>,
    mem_locks: Mutex<HashSet<PublicKey>>,
}

struct PoH {
    poh: BufWriter,
    hash: Hash,
    height: usize,
    dummy_current: Hash,
}

/// Points to indexes into the Record structure
#[crepr]
struct PohEntry {
    poh_hash: Hash,
    /// when 0, this means its an empty entry
    record_index: u64,
    number_of_txs: u64,
}

impl PoH {
    /// mix the offset into the stream
    pub fn insert(&mut self, record_hash: Hash, record_index: u64, number_of_txs: u64) {
        // lock memory
        // fake poh for now, mix in `record_hash` for actual PoH
        self.hash += 1;
        // store the offset to file
        let entry = self.poh.get_mut(self.height);
        *entry = PoHEntry {
            poh_hash: self.hash,
            record_index: record_index,
            number_of_txs: number_of_txs,
        };
        self.height += 1;
    }
    pub fn height(&self) -> usize {
        self.height
    }
}

impl StateMachine {
    fn new(poh: &str, tx_data: &str, state: &str, poher: Poh) -> StateMachine {
        StateMachine {
            poh: memmap::map_mut(poh).expect("map poh"),
            tx_data: memmap::map_mut(tx_data).expect("map tx_data"),
            state: memmap::map_mut(state).expect("map state"),
            poher: poher,
        }
    }
    fn acquire_memory_lock(&self, packet: &Vec<Tx>, acquired_memory: &mut Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, p) in packet.iter().enumerate() {
            let collision = mem_locks.contains(p.call.caller) || mem_locks.contains(p.destination);
            acquired_memory[i] = !collision;
            if !collision {
                mem_locks.insert(&p.call.caller);
                mem_locks.insert(&p.destination);
            }
        }
    }
    fn validate_debits(
        &self,
        packet: &Vec<Tx>,
        acquired_memory: &Vec<bool>,
        has_funds: &mut Vec<bool>,
    ) {
        //holds page table read lock
        let table_lock = self.table_lock.read().unwrap();
        assert!(!*table_lock);
        for (i, p) in packet.iter().enumerate() {
            acquired_funds[i] = false;
            if !acquired_memory[i] {
                continue;
            }
            if Some(mem) = self.page_table.get(&p.call.caller) {
                if mem.balance > p.ammount {
                    has_funds[i] = true;
                }
            }
        }
    }
    fn find_new_keys(&self, packet: &Vec<Tx>, has_funds: &Vec<bool>, new_keys: &mut Vec<bool>) {
        //holds page table read lock
        let table_lock = self.table_lock.read().unwrap();
        assert!(!*table_lock);
        for (i, p) in packet.iter().enumerate() {
            new_keys[i] = false;
            if !has_funds[i] {
                continue;
            }
            if None == self.page_table.get(&p.destination) {
                new_keys[i] = true;
            }
        }
    }
    fn allocate_keys(&mut self, packet: &Vec<Tx>, new_keys: &Vec<bool>) {
        //holds write lock to the page table
        let table_lock = self.table_lock.write().unwrap();
        //while we hold the write lock, this is where we can create another anonymouns page
        //with copy and write, and send this table to the vote signer
        *table_lock = true;
        for (i, p) in packet.iter().enumerate() {
            if !new_keys[i] {
                continue;
            }
            let page = Page {
                key: tx.destination,
                owner: tx.destination, //CONTRACT KEY
                balance: 0,
                size: 0,
                pointer: 0,
            };
            PageT::insert(&self.page_table, &tx.destination, page);
        }
        *table_lock = false;
    }
    fn move_funds(&mut self, packet: &Vec<Tx>, has_funds: &mut Vec<bool>) {
        //holds page table read lock
        let table_lock = self.table_lock.read().unwrap();
        assert!(!*table_lock);
        for (i, p) in packet.iter().enumerate() {
            if !has_funds[i] {
                continue;
            }
            self.page_table[&p.call.caller].balance -= tx.amount;
            self.page_table[&p.destination].balance += tx.amount;
        }
    }
    fn load_memory(&mut self, packet: &Vec<Tx>, has_funds: &Vec<bool>) {
        //TBD
    }
    fn execute_call(&mut self, packet: &Vec<Tx>, has_funds: &Vec<bool>) {
        //TBD
    }
    fn write_memory(&mut self, packet: &Vec<Tx>, has_funds: &Vec<bool>) {
        //TBD
    }
    fn release_memory_lock(&self, packet: &Vec<Tx>, acquired_memory: &mut Vec<bool>) {
        //holds mem_locks mutex
        let mut mem_locks = self.mem_locks.lock().unwrap();
        for (i, p) in packet.iter().enumerate() {
            mem_locks.remove(&p.call.caller);
            mem_locks.remove(&p.destination);
        }
    }
    /// fill up to the blob
    fn fill_blob(
        &self,
        packet: &Vec<Tx>,
        has_funds: &Vec<bool>,
        blob: &mut Vec<Tx>,
        filled: &mut usize,
    ) {
        //nolock
        for (i, p) in packet.iter().enumerate() {
            if !has_funds[i] {
                continue;
            }
            blob[*filled] = packet[i];
            *filled += 1;
            if *filled == blob.len() {
                return;
            }
        }
    }
    fn output(&self, blob: &Vec<Tx>) {
        let (hash, pos) = self.record.insert(blob);
        self.poh.insert(hash, pos, blob.len());
    }
}
