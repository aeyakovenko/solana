use bincode::serialize;
use page_table::{Hash, Tx};
use result::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc::{Receiver, Sender};

pub struct Ledger {
    /// append only array of Call structures
    record: Record,
    /// state machine for transactions and contract memory
    poh: PoH,
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
        let hash = [self.height as u64, 0, 0, 0];
        let height = self.height;
        let bytes = serialize(&blob).unwrap();
        self.records.write(&bytes)?;
        self.height += len;
        // returns the hash of the blob
        return Ok((hash, height as u64));
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
    /// when 0, means it was empty
    record_hash: Hash,
    /// when record_hash is 0, this is 0 as well
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
        loop {
            let input = self.receiver.try_recv();
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
            self.hash[0] += record_hash[0];
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
    pub fn output(&mut self, blob: &Vec<Tx>) -> Result<()> {
        let (hash, pos) = self.record.insert(blob)?;
        self.poh.insert(hash, pos as u64)?;
        Ok(())
    }
}
