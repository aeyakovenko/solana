use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::thread::{spawn, JoinHandle};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use result::{Error, Result};

const NUM_BLOCKS: usize = 10 * 2048;
const BLOCK_SIZE: usize = 64 * 1024;

#[derive(Clone)]
pub struct Block {
    data: [u8; BLOCK_SIZE],
    pub size: usize,
    pub addr: [u16; 8],
    pub port: u16,
    pub v6: bool,
    pub retransmitted: bool,
}

pub type SharedBlock = Arc<RwLock<Block>>;
pub type Sender = mpsc::Sender<SharedBlock>;
pub type Receiver = mpsc::Receiver<SharedBlock>;
pub type Recycler = Arc<Mutex<Vec<SharedBlock>>>;
pub type Window = Arc<Mutex<Vec<Option<SharedBlock>>>>;

impl Default for Block {
    fn default() -> Block {
        Block {
            data: [0u8; BLOCK_SIZE],
            size: 0,
            addr: [0u16; 8],
            port: 0,
            v6: false,
            retransmitted: false,
        }
    }
}

impl Block {
    pub fn payload(&self) -> &[u8] {
        &self.data[8..]
    }
    pub fn index(&self) -> Result<u64> {
        let mut rdr = io::Cursor::new(&self.data[0..8]);
        let r = rdr.read_u64::<LittleEndian>()?;
        Ok(r)
    }
    pub fn set_index(&mut self, ix: u64) -> Result<()> {
        let mut wtr = vec![];
        wtr.write_u64::<LittleEndian>(ix)?;
        self.data[..8].clone_from_slice(&wtr);
        Ok(())
    }
    pub fn get_addr(&self) -> SocketAddr {
        if !self.v6 {
            let ipv4 = Ipv4Addr::new(
                self.addr[0] as u8,
                self.addr[1] as u8,
                self.addr[2] as u8,
                self.addr[3] as u8,
            );
            SocketAddr::new(IpAddr::V4(ipv4), self.port)
        } else {
            let ipv6 = Ipv6Addr::new(
                self.addr[0],
                self.addr[1],
                self.addr[2],
                self.addr[3],
                self.addr[4],
                self.addr[5],
                self.addr[6],
                self.addr[7],
            );
            SocketAddr::new(IpAddr::V6(ipv6), self.port)
        }
    }

    pub fn set_addr(&mut self, a: &SocketAddr) {
        match a {
            &SocketAddr::V4(v4) => {
                let ip = v4.ip().octets();
                self.addr[0] = u16::from(ip[0]);
                self.addr[1] = u16::from(ip[1]);
                self.addr[2] = u16::from(ip[2]);
                self.addr[3] = u16::from(ip[3]);
                self.port = a.port();
            }
            &SocketAddr::V6(v6) => {
                self.addr = v6.ip().segments();
                self.port = a.port();
                self.v6 = true;
            }
        }
    }
}

//TODO, we would need to stick block authentication before we create the
//window.
fn reader(
    window: &Window,
    recycler: &Recycler,
    consumed: &mut usize,
    socket: &UdpSocket,
    s: &Sender,
) -> Result<()> {
    socket.set_nonblocking(false)?;
    for i in 0.. {
        let b = allocate(recycler);
        let b_ = b.clone();
        let mut p = b.write().unwrap();
        match socket.recv_from(&mut p.data) {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                recycle(recycler, b_);
                break;
            }
            Err(e) => {
                recycle(recycler, b_);
                return Err(Error::IO(e));
            }
            Ok((nrecv, from)) => {
                p.size = nrecv;
                p.set_addr(&from);
                if i == 0 {
                    socket.set_nonblocking(true)?;
                }
                let pix = p.index()? as usize;
                let w = pix % NUM_BLOCKS;
                //TODO, after the block are authenticated
                //if we get different blocks at the same index
                //that is a network failure/attack
                {
                    let mut mw = window.lock().unwrap();
                    if mw[w].is_none() {
                        mw[w] = Some(b_);
                    }
                    //send a contiguous set of blocks
                    loop {
                        let k = *consumed % NUM_BLOCKS;
                        match mw[k].clone() {
                            None => break,
                            Some(x) => {
                                s.send(x)?;
                                mw[k] = None;
                            }
                        }
                        *consumed += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn service(exit: Arc<AtomicBool>, r: Recycler, s: Sender, sock: UdpSocket) -> JoinHandle<()> {
    spawn(move || {
        let window = Arc::new(Mutex::new(Vec::new()));
        let mut consumed = 0;
        loop {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            match reader(&window, &r, &mut consumed, &sock, &s) {
                Ok(_) => (),
                Err(e) => {
                    error!("error {:?}", e);
                    break;
                }
            }
        }
    })
}

pub fn allocate(r: &Recycler) -> SharedBlock {
    let mut gc = r.lock().expect("lock");
    gc.pop()
        .unwrap_or_else(|| Arc::new(RwLock::new(Block::default())))
}

pub fn recycle(r: &Recycler, msgs: SharedBlock) {
    let mut gc = r.lock().expect("lock");
    gc.push(msgs);
}

#[cfg(test)]
mod test {
    use window::service;
    use std::net::UdpSocket;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::channel;

    #[test]
    fn window_test() {
        let read = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = read.local_addr().unwrap();
        let exit = Arc::new(AtomicBool::new(false));
        let recycler = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = channel();
        let jt = service(exit, recycler, sender, read);
    }
}
