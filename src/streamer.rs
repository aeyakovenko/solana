use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::fmt;
use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::thread::{spawn, JoinHandle};
use std::collections::VecDeque;
use result::{Error, Result};
use packet::{Blob, BlobRecycler, Packet, PacketRecycler, SharedBlob, SharedPackets, NUM_BLOBS};

pub type PacketReceiver = mpsc::Receiver<SharedPackets>;
pub type PacketSender = mpsc::Sender<SharedPackets>;
pub type BlobSender = mpsc::Sender<VecDeque<SharedBlob>>;
pub type BlobReceiver = mpsc::Receiver<VecDeque<SharedBlob>>;

fn recv_loop(
    sock: &UdpSocket,
    exit: &Arc<AtomicBool>,
    re: &PacketRecycler,
    channel: &PacketSender,
) -> Result<()> {
    loop {
        let msgs = re.allocate();
        let msgs_ = msgs.clone();
        loop {
            match msgs.write().unwrap().read_from(sock) {
                Ok(()) => {
                    channel.send(msgs_)?;
                    break;
                }
                Err(_) => {
                    if exit.load(Ordering::Relaxed) {
                        re.recycle(msgs_);
                        return Ok(());
                    }
                }
            }
        }
    }
}

pub fn receiver(
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    recycler: PacketRecycler,
    channel: PacketSender,
) -> Result<JoinHandle<()>> {
    let timer = Duration::new(1, 0);
    sock.set_read_timeout(Some(timer))?;
    Ok(spawn(move || {
        let _ = recv_loop(&sock, &exit, &recycler, &channel);
        ()
    }))
}

fn recv_send(sock: &UdpSocket, recycler: &BlobRecycler, r: &BlobReceiver) -> Result<()> {
    let timer = Duration::new(1, 0);
    let msgs = r.recv_timeout(timer)?;
    Blob::send_to(msgs, sock, recycler)?;
    Ok(())
}

pub fn responder(
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    recycler: BlobRecycler,
    r: BlobReceiver,
) -> JoinHandle<()> {
    spawn(move || loop {
        if recv_send(&sock, &recycler, &r).is_err() || exit.load(Ordering::Relaxed) {
            break;
        }
    })
}

//TODO, we would need to stick block authentication before we create the
//window.
fn recv_window(
    window: &mut Vec<SharedBlob>,
    recycler: &BlobRecycler,
    consumed: &mut usize,
    socket: &UdpSocket,
    s: &BlobSender,
) -> Result<()> {
    let dq = Blob::recv_from(recycler, socket)?;
    while let Some(b) = dq.pop_front() {
        let mut p = b.write().unwrap();
        let pix = p.index()? as usize;
        let w = pix % NUM_BLOBS;
        //TODO, after the block are authenticated
        //if we get different blocks at the same index
        //that is a network failure/attack
        {
            let mut mw = window.lock().unwrap();
            if mw[w].is_none() {
                mw[w] = Some(b);
            } else {
                debug!("duplicate blob at index {:}", w);
            }
            //send a contiguous set of blocks
            let mut dq = VecDeque::new();
            loop {
                let k = *consumed % NUM_BLOBS;
                match mw[k].clone() {
                    None => break,
                    Some(x) => {
                        dq.push_front(x);
                        mw[k] = None;
                    }
                }
                *consumed += 1;
            }
            if dq.len() > 0 {
                s.send(dq)?;
            }
        }
    }
    Ok(())
}

pub fn window(
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    r: BlobRecycler,
    s: BlobSender,
) -> JoinHandle<()> {
    spawn(move || {
        let window = Vec::new();
        let mut consumed = 0;
        let timer = Duration::new(1, 0);
        sock.set_read_timeout(Some(timer))?;
        loop {
            if recv_window(&window, &r, &mut consumed, &sock, &s).is_err()
                && exit.load(Ordering::Relaxed)
            {
                break;
            }
        }
    })
}

#[cfg(all(feature = "unstable", test))]
mod bench {
    extern crate test;
    use self::test::Bencher;
    use std::thread::sleep;
    use std::sync::{Arc, Mutex};
    use std::net::{SocketAddr, UdpSocket};
    use std::time::Duration;
    use std::time::SystemTime;
    use std::thread::{spawn, JoinHandle};
    use std::sync::mpsc::channel;
    use std::sync::atomic::{AtomicBool, Ordering};
    use result::Result;
    use streamer::{allocate, receiver, recycle, Packet, PacketRecycler, Receiver, PACKET_SIZE};

    fn producer(
        addr: &SocketAddr,
        recycler: PacketRecycler,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let send = UdpSocket::bind("0.0.0.0:0").unwrap();
        let msgs = allocate(&recycler);
        let msgs_ = msgs.clone();
        msgs.write().unwrap().packets.resize(10, Packet::default());
        for w in msgs.write().unwrap().packets.iter_mut() {
            w.meta.size = PACKET_SIZE;
            w.meta.set_addr(&addr);
        }
        spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            let mut num = 0;
            for p in msgs_.read().unwrap().packets.iter() {
                let a = p.meta.get_addr();
                send.send_to(&p.data[..p.meta.size], &a).unwrap();
                num += 1;
            }
            assert_eq!(num, 10);
        })
    }

    fn sinc(
        recycler: PacketRecycler,
        exit: Arc<AtomicBool>,
        rvs: Arc<Mutex<usize>>,
        r: Receiver,
    ) -> JoinHandle<()> {
        spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                return;
            }
            let timer = Duration::new(1, 0);
            match r.recv_timeout(timer) {
                Ok(msgs) => {
                    let msgs_ = msgs.clone();
                    *rvs.lock().unwrap() += msgs.read().unwrap().packets.len();
                    recycle(&recycler, msgs_);
                }
                _ => (),
            }
        })
    }
    fn run_streamer_bench() -> Result<()> {
        let read = UdpSocket::bind("127.0.0.1:0")?;
        let addr = read.local_addr()?;
        let exit = Arc::new(AtomicBool::new(false));
        let recycler = Arc::new(Mutex::new(Vec::new()));

        let (s_reader, r_reader) = channel();
        let t_reader = receiver(read, exit.clone(), recycler.clone(), s_reader)?;
        let t_producer1 = producer(&addr, recycler.clone(), exit.clone());
        let t_producer2 = producer(&addr, recycler.clone(), exit.clone());
        let t_producer3 = producer(&addr, recycler.clone(), exit.clone());

        let rvs = Arc::new(Mutex::new(0));
        let t_sinc = sinc(recycler.clone(), exit.clone(), rvs.clone(), r_reader);

        let start = SystemTime::now();
        let start_val = *rvs.lock().unwrap();
        sleep(Duration::new(5, 0));
        let elapsed = start.elapsed().unwrap();
        let end_val = *rvs.lock().unwrap();
        let time = elapsed.as_secs() * 10000000000 + elapsed.subsec_nanos() as u64;
        let ftime = (time as f64) / 10000000000f64;
        let fcount = (end_val - start_val) as f64;
        println!("performance: {:?}", fcount / ftime);
        exit.store(true, Ordering::Relaxed);
        t_reader.join()?;
        t_producer1.join()?;
        t_producer2.join()?;
        t_producer3.join()?;
        t_sinc.join()?;
        Ok(())
    }
    #[bench]
    pub fn streamer_bench(_bench: &mut Bencher) {
        run_streamer_bench().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::net::UdpSocket;
    use std::time::Duration;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::channel;
    use std::io::Write;
    use std::io;
    use packet::{Blob, BlobRecycler, Packet, PacketRecycler, Packets};
    use streamer::{receiver, responder, PacketReceiver};

    fn get_msgs(r: Receiver, num: &mut usize) {
        for _t in 0..5 {
            let timer = Duration::new(1, 0);
            match r.recv_timeout(timer) {
                Ok(m) => *num += m.read().unwrap().packets.len(),
                e => println!("error {:?}", e),
            }
            if *num == 10 {
                break;
            }
        }
    }
    #[test]
    pub fn streamer_debug() {
        write!(io::sink(), "{:?}", Packet::default()).unwrap();
        write!(io::sink(), "{:?}", Packets::default()).unwrap();
        write!(io::sink(), "{:?}", Blob::default()).unwrap();
    }
    #[test]
    pub fn streamer_send_test() {
        let read = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let addr = read.local_addr().unwrap();
        let send = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let exit = Arc::new(AtomicBool::new(false));
        let packet_recycler = PacketRecycler::new();
        let resp_recycler = BlobRecycler::new();
        let (s_reader, r_reader) = channel();
        let t_receiver = receiver(read, exit.clone(), packet_recycler.clone(), s_reader).unwrap();
        let (s_responder, r_responder) = channel();
        let t_responder = responder(send, exit.clone(), resp_recycler.clone(), r_responder);
        let msg = allocate(&resp_recycler);
        msgs.write().unwrap().responses.resize(10, Blob::default());
        for (i, w) in msgs.write().unwrap().responses.iter_mut().enumerate() {
            w.data[0] = i as u8;
            w.meta.size = PACKET_SIZE;
            w.meta.set_addr(&addr);
            assert_eq!(w.meta.get_addr(), addr);
        }
        s_responder.send(msgs).expect("send");
        let mut num = 0;
        get_msgs(r_reader, &mut num);
        assert_eq!(num, 10);
        exit.store(true, Ordering::Relaxed);
        t_receiver.join().expect("join");
        t_responder.join().expect("join");
    }
}
