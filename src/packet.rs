use std::sync::{Arc, Mutex, RwLock};
use std::fmt;
use std::io;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::collections::VecDeque;
use result::{Error, Result};

pub type SharedPackets = Arc<RwLock<Packets>>;
pub type SharedBlob = Arc<RwLock<Blob>>;
pub type PacketRecycler = Recycler<Packets>;
pub type BlobRecycler = Recycler<Blob>;

const NUM_PACKETS: usize = 1024 * 8;
const BLOB_SIZE: usize = 64 * 1024;
pub const PACKET_SIZE: usize = 256;
pub const NUM_BLOBS: usize = (NUM_PACKETS * PACKET_SIZE) / BLOB_SIZE;

#[derive(Clone, Default)]
pub struct Meta {
    pub size: usize,
    pub addr: [u16; 8],
    pub port: u16,
    pub v6: bool,
}

#[derive(Clone)]
pub struct Packet {
    pub data: [u8; PACKET_SIZE],
    pub meta: Meta,
}

impl fmt::Debug for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Packet {{ size: {:?}, addr: {:?} }}",
            self.meta.size,
            self.meta.get_addr()
        )
    }
}

impl Default for Packet {
    fn default() -> Packet {
        Packet {
            data: [0u8; PACKET_SIZE],
            meta: Meta::default(),
        }
    }
}

impl Meta {
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
        match *a {
            SocketAddr::V4(v4) => {
                let ip = v4.ip().octets();
                self.addr[0] = u16::from(ip[0]);
                self.addr[1] = u16::from(ip[1]);
                self.addr[2] = u16::from(ip[2]);
                self.addr[3] = u16::from(ip[3]);
                self.port = a.port();
            }
            SocketAddr::V6(v6) => {
                self.addr = v6.ip().segments();
                self.port = a.port();
                self.v6 = true;
            }
        }
    }
}

#[derive(Debug)]
pub struct Packets {
    pub packets: Vec<Packet>,
}

impl Default for Packets {
    fn default() -> Packets {
        Packets {
            packets: vec![Packet::default(); NUM_PACKETS],
        }
    }
}

#[derive(Clone)]
pub struct Blob {
    pub data: [u8; BLOB_SIZE],
    pub meta: Meta,
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Blob {{ size: {:?}, addr: {:?} }}",
            self.meta.size,
            self.meta.get_addr()
        )
    }
}

impl Default for Blob {
    fn default() -> Blob {
        Blob {
            data: [0u8; BLOB_SIZE],
            meta: Meta::default(),
        }
    }
}

pub struct Recycler<T: Default> {
    gc: Arc<Mutex<Vec<Arc<RwLock<T>>>>>,
}

impl<T: Default> Recycler<T> {
    pub fn clone(&self) -> Recycler<T> {
        Recycler {
            gc: self.gc.clone(),
        }
    }
    pub fn new() -> Recycler<T> {
        Recycler {
            gc: Arc::new(Mutex::new(vec![])),
        }
    }
    pub fn allocate(&self) -> Arc<RwLock<T>> {
        let mut gc = self.gc.lock().expect("recycler lock");
        gc.pop()
            .unwrap_or_else(|| Arc::new(RwLock::new(Default::default())))
    }
    pub fn recycle(&self, msgs: Arc<RwLock<T>>) {
        let mut gc = self.gc.lock().expect("recycler lock");
        gc.push(msgs);
    }
}

impl Packets {
    fn run_read_from(&mut self, socket: &UdpSocket) -> Result<usize> {
        self.packets.resize(NUM_PACKETS, Packet::default());
        let mut i = 0;
        socket.set_nonblocking(false)?;
        for p in &mut self.packets {
            p.meta.size = 0;
            match socket.recv_from(&mut p.data) {
                Err(_) if i > 0 => {
                    trace!("got {:?} messages", i);
                    break;
                }
                Err(e) => {
                    info!("recv_from err {:?}", e);
                    return Err(Error::IO(e));
                }
                Ok((nrecv, from)) => {
                    p.meta.size = nrecv;
                    p.meta.set_addr(&from);
                    if i == 0 {
                        socket.set_nonblocking(true)?;
                    }
                }
            }
            i += 1;
        }
        Ok(i)
    }
    pub fn recv_from(&mut self, socket: &UdpSocket) -> Result<()> {
        let sz = self.run_read_from(socket)?;
        self.packets.resize(sz, Packet::default());
        Ok(())
    }
    pub fn send_to(&self, socket: &UdpSocket) -> Result<()> {
        for p in self.packets.iter() {
            let a = p.meta.get_addr();
            socket.send_to(&p.data[..p.meta.size], &a)?;
        }
        Ok(())
    }
}

impl Blob {
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
    pub fn data(&self) -> &[u8] {
        &self.data[8..]
    }
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[8..]
    }
    pub fn recv_from(re: &BlobRecycler, socket: &UdpSocket) -> Result<VecDeque<SharedBlob>> {
        let mut v = VecDeque::new();
        socket.set_nonblocking(false)?;
        for i in 0..NUM_BLOBS {
            let r = re.allocate();
            {
                let mut p = r.write().unwrap();
                match socket.recv_from(&mut p.data) {
                    Err(_) if i > 0 => {
                        trace!("got {:?} messages", i);
                        break;
                    }
                    Err(e) => {
                        info!("recv_from err {:?}", e);
                        return Err(Error::IO(e));
                    }
                    Ok((nrecv, from)) => {
                        p.meta.size = nrecv;
                        p.meta.set_addr(&from);
                        if i == 0 {
                            socket.set_nonblocking(true)?;
                        }
                    }
                }
            }
            v.push_back(r);
        }
        Ok(v)
    }
    pub fn send_to(
        re: &BlobRecycler,
        socket: &UdpSocket,
        v: &mut VecDeque<SharedBlob>,
    ) -> Result<()> {
        while let Some(r) = v.pop_front() {
            {
                let p = r.read().unwrap();
                let a = p.meta.get_addr();
                socket.send_to(&p.data[..p.meta.size], &a)?;
            }
            re.recycle(r);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::net::UdpSocket;
    use std::io::Write;
    use std::io;
    use std::collections::VecDeque;
    use packet::{Blob, BlobRecycler, Packet, PacketRecycler, Packets};
    #[test]
    pub fn packet_recycler_test() {
        let r = PacketRecycler::new();
        let p = r.allocate();
        r.recycle(p);
    }
    #[test]
    pub fn blob_recycler_test() {
        let r = BlobRecycler::new();
        let p = r.allocate();
        r.recycle(p);
    }
    #[test]
    pub fn packet_send_recv() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let saddr = sender.local_addr().unwrap();
        let r = PacketRecycler::new();
        let p = r.allocate();
        p.write().unwrap().packets.resize(10, Packet::default());
        for m in p.write().unwrap().packets.iter_mut() {
            m.meta.set_addr(&addr);
            m.meta.size = 256;
        }
        p.read().unwrap().send_to(&sender).unwrap();
        p.write().unwrap().recv_from(&reader).unwrap();
        for m in p.write().unwrap().packets.iter_mut() {
            assert_eq!(m.meta.size, 256);
            assert_eq!(m.meta.get_addr(), saddr);
        }

        r.recycle(p);
    }

    #[test]
    pub fn blob_send_recv() {
        let reader = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let r = BlobRecycler::new();
        let p = r.allocate();
        p.write().unwrap().meta.set_addr(&addr);
        p.write().unwrap().meta.size = 1024;
        let mut v = VecDeque::new();
        v.push_back(p);
        assert_eq!(v.len(), 1);
        Blob::send_to(&r, &sender, &mut v).unwrap();
        trace!("send_to");
        assert_eq!(v.len(), 0);
        let mut rv = Blob::recv_from(&r, &sender).unwrap();
        trace!("recv_from");
        assert_eq!(rv.len(), 1);
        let rp = rv.pop_front().unwrap();
        assert_eq!(rp.write().unwrap().meta.size, 1024);
        r.recycle(rp);
    }

    #[cfg(ipv6)]
    #[test]
    pub fn blob_ipv6_send_recv() {
        let reader = UdpSocket::bind("[::1]:0").expect("bind");
        let addr = reader.local_addr().unwrap();
        let sender = UdpSocket::bind("[::1]:0").expect("bind");
        let r = BlobRecycler::new();
        let p = r.allocate();
        p.write().unwrap().meta.set_addr(&addr);
        p.write().unwrap().meta.size = 1024;
        let mut v = VecDeque::new();
        v.push_back(p);
        Blob::send_to(&r, &sender, &mut v);
        let mut rv = Blob::recv_from(&r, &sender).unwrap();
        let rp = rv.pop_front().unwrap();
        assert_eq!(rp.write().unwrap().meta.size, 1024);
        r.recycle(rp);
    }

    #[test]
    pub fn debug_trait() {
        write!(io::sink(), "{:?}", Packet::default()).unwrap();
        write!(io::sink(), "{:?}", Packets::default()).unwrap();
        write!(io::sink(), "{:?}", Blob::default()).unwrap();
    }
}
