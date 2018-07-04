use result::{Result, Error};
use core::marker::PhantomData;

pub trait Key: Eq {
    fn start(&self) -> usize;
    fn unused(&self) -> bool;
}

pub trait Val<T: Key>: Sized + Clone {
    fn key(&self) -> &T;
}

pub struct HashT<K: Key, V: Val<K> > {
    _key: PhantomData<K>,
    _val: PhantomData<V>,
}

impl<K, V> HashT<K, V> 
    where K: Key,
          V: Val<K>
{
    pub fn find(tbl: &[V], key: &K) -> Result<usize> {
        let num_elems = tbl.len();
        let st = key.start();
        for i in 0 .. num_elems {
            let pos = st.wrapping_add(i) % num_elems;
            unsafe {
                let k = tbl.get_unchecked(pos).key();
                if k.unused() || k == key { 
                    return Ok(pos);
                }
            }
        }
        return Err(Error::NoSpace)
    }
    pub fn migrate(src: &[V], dst: &mut [V]) -> Result<()> {
        for i in src.iter() {
            if i.key().unused() {
                continue;
            }
            let p = Self::find(src, i.key())?;
            unsafe {
                *dst.get_unchecked_mut(p) = (*i).clone();
            }
        }
        return Ok(());
    }

}

#[cfg(test)]
mod test {
    use hasht;
    use result::{Error};
    impl hasht::Key for usize {
        fn start(&self) -> usize {
            *self
        }
        fn unused(&self) -> bool {
            *self == 0usize
        }
    }

    impl hasht::Val<usize> for usize {
        fn key(&self) -> &usize {
            &self
        }
    }
    type UsizeT = hasht::HashT<usize, usize>;
    #[test]
    fn hash_test() {
        let mut v = Vec::new();
        v.clear();
        v.resize(0, 0usize);
        let r = UsizeT::find(&v, &1usize);
        assert_eq!(r.unwrap_err(), Error::NoSpace);
    }
    #[test]
    fn hash_test2() {
        let mut v = Vec::new();
        v.clear();
        v.resize(1, 0usize);
        let r = UsizeT::find(&v, &1usize);
        assert_eq!(r.unwrap(), 0);
    }
    #[test]
    fn hash_test3() {
        let mut v = Vec::new();
        v.clear();
        v.resize(1, 0usize);
        let a = UsizeT::find(&v, &1usize).expect("find 1");
        v[a] = 1;
        let b = UsizeT::find(&v, &2usize);
        assert_eq!(b.unwrap_err(), Error::NoSpace);
    }
    #[test]
    fn hash_test4() {
        let mut v = Vec::new();
        v.clear();
        v.resize(2, 0usize);
        let a = UsizeT::find(&v, &1usize).expect("find 1");
        v[a] = 1;
        let b = UsizeT::find(&v, &2usize).expect("find 2");
        assert_ne!(a, b);
        v[b] = 2;
        assert_eq!(UsizeT::find(&v, &1usize).unwrap(), a);
        assert_eq!(UsizeT::find(&v, &2usize).unwrap(), b);
        assert_eq!(UsizeT::find(&v, &3usize).unwrap_err(), Error::NoSpace);
        assert_eq!(UsizeT::find(&v, &usize::max_value()).unwrap_err(),
                   Error::NoSpace);
    }
}


