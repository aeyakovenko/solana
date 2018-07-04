use hasht;
use memmap;

//dummy
type Hash = usize;
type PublicKey = usize;

struct StateMachine {
    /// append only array of Tx structures
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
    /// a slice of Tx, that is appended to
    data: memmap::MmapMut,
    height: usize,
}

impl Record {
    /// write a blob to the record, and return the hash of the blob and the index at which it was
    /// inserted into the record
    pub fn insert(&self, blob: &Vec<Tx>) -> (Hash, usize) {
        let len = blob.len();
        //TODO: compute real hash
        let hash = blobs.len();
        let height = self.height;
        let slice = self.data.get_mut(height..(height + len));
        slice.clone_from_slice(&blob);
        self.height += len;
        return (hash, len);
    }
}

struct PageTable {
    /// HashT over a large array of [Page]
    page_table: Vec<Page>,
    table_lock: RwLock<bool>,
    mem_locks: Mutex<HashSet<PublicKey>>,
}

struct PoH {
    poh: memmap::MmapMut,
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

/// simple transaction with two mem keys
struct Tx {
    from: PublicKey,
    to: PublicKey,
    op: u8,
    amount: u64,
    fee: u64,
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
            let collision = mem_locks.contains(p.from) || mem_locks.contains(p.to);
            acquired_memory[i] = !collision;
            if !collision {
                mem_locks.insert(&p.from);
                mem_locks.insert(&p.to);
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
            if Some(mem) = self.page_table.get(&p.from) {
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
            if None == self.page_table.get(&p.to) {
                new_keys[i] = true;
            }
        }
    }
    fn allocate_keys(&mut self, packet: &Vec<Tx>, new_keys: &Vec<bool>) {
        let table_lock = self.table_lock.write().unwrap();
        *table_lock = true;
        for (i, p) in packet.iter().enumerate() {
            if !new_keys[i] {
                continue;
            }
            let page = Page {
                key: tx.to,
                owner: tx.to, //CONTRACT KEY
                balance: 0,
                size: 0,
                pointer: 0,
            };
            PageT::insert(&self.page_table, &tx.to, page);
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
            self.page_table[&p.from].balance -= tx.amount;
            self.page_table[&p.to].balance += tx.amount;
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
            mem_locks.remove(&p.from);
            mem_locks.remove(&p.to);
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
