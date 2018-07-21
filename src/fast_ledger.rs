use bincode::serialize;
use page_table::{Call, Hash};
use result::Result;
use std::fs::File;
use std::io::{BufWriter, Write, Seek, SeekFrom, BufReader};
use std::sync::mpsc::{Receiver, Sender};

pub struct Ledger {
    /// append only array of Call structures
    tx_writer: TransactionWriter,
    /// state machine for transactions and contract memory
    poh_writer: PoHWriter,
}

pub struct TransactionWriter {
    /// a slice of Call, that is appended to
    records: BufWriter<File>,
    height: usize,
}

impl TransactionWriter {
    /// write a blob to the record, and return the hash of the blob and the index at which it was
    /// inserted into the record
    pub fn write(&mut self, blob: &Vec<Tx>) -> Result<(Hash, u64)> {
        let len = blob.len();
        //TODO: compute real hash
        let hash = [self.height as u64, 0, 0, 0];
        let height = self.height;
        let bytes = serialize(&blob).unwrap();
        self.records.write(&bytes)?;
        self.height += len;
        // returns the hash of the blob
        return Ok((hash, height as u64));
    }
}

pub struct PoHWriter {
    /// number of hashes to produce for each entry
    sender: Sender<(Hash, u64)>,
    generator: JoinHandle<()>
}

 pub struct PoHReader {
    /// number of hashes to produce for each entry
    reader: BufReader<File>,
    poh_entry_size: usize,
    capacity: usize,
    pos: usize,
}
 
/// Points to indexes into the Record structure
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct PohEntry {
    /// hash produced by PoH process
    poh_hash: Hash,
    /// when 0, means it was empty
    record_hash: Hash,
    /// when record_hash is 0, this is 0 as well
    record_index: u64,
}
impl PoHReader {
    pub new(file: &str, capacity: usize) -> Self {
        let read_file = OpenOptions::new()
                        .read(true)
                        .open(file))?;
        let reader = BufReader::new_with_capacity(read_file, capacity); 
        let poh_entry_size = serialize(PohEntry::default()).unwrap().len();
        PohReader { reader, poh_entry_size, capacity, pos: 0}
    }
    pub get_entry(&mut self, entry: u64) -> Result<PohEntry> {
        let len = self.reader.get_ref().metadata().len();
        let pos = entry * self.poh_entry_size;
        if pos > self.pos + self.capacity {
            self.reader.seek(SeekFrom::Start(pos));    
            self.fill_buf()?;
        }
    }
}

impl PoHWriter {
    pub new(file: &str, last: Hash) -> Result<Self> {
        let write_file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(file))?;
        let writer = BufWriter::new(write_file);
        let (sender, receiver) = channel();
        let generator = spawn(|| {
            Self::generator(start, writer, receiver);
        })?;

        PoHWriter{writer, receiver }
    }
    /// mix in the hash and record offset into the poh_file
    pub fn write(&mut self, record_hash: Hash, record_index: u64) -> Result<()> {
        self.sender.send((record_hash, record_index))?;
        Ok(())
    }
    //separate thread calls this
    pub fn generator(writer: BufWriter, receiver: Receiver<(Hash, u64)>, mut last: Hash) -> Result<()> {
        loop {
            let input = receiver.try_recv();
            let record_hash;
            let record_index;
            if let Ok((in_hash, in_index)) = input {
                record_hash = in_hash;
                record_index = in_index;
            } else {
                //TODO:  should we keep the last value for the record index to indicate 0 bytes
                //have been added to the record at this point?
                record_hash = [0, 0, 0, 0]; //or last?
                record_index = 0; //or last
            }
            // fake PoH mixin
            hash[0] += record_hash[0];
            for _ in 0..self.num_hashes {
                // do the sha256 loop now, with record_hash as the seed
                // fake poh for now, mix in `record_hash` for actual PoH
                self.hash[0] += 1;
            }
            let entry = PohEntry {
                poh_hash: self.hash,
                record_index: record_index,
                record_hash: record_hash,
            };
            let bytes = serialize(&entry).unwrap();
            //should always be the same size
            self.bytes_written += bytes.len();
            self.entries_written += 1;
            self.poh_file.write(&bytes)?;
        }
    }
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

impl Ledger {
    pub fn new() -> Self {}
    pub fn output(&mut self, blob: &Vec<Call>) -> Result<()> {
        let (hash, pos) = self.tx_writer.write(blob)?;
        self.poh_writer.write(hash, pos as u64)?;
        Ok(())
    }
}
