//! Port pool for per-call media sockets: a port is never shared across calls.

use std::sync::Mutex;

pub struct PortAllocator {
    free: Mutex<Vec<u16>>,
    total: usize,
}

impl PortAllocator {
    pub fn new(base: u16, count: u16) -> Self {
        // Reversed so `pop` hands out the lowest port first.
        let free: Vec<u16> = (base..base + count).rev().collect();
        Self { total: free.len(), free: Mutex::new(free) }
    }

    pub fn alloc(&self) -> Option<u16> {
        self.free.lock().unwrap().pop()
    }

    pub fn free(&self, port: u16) {
        let mut f = self.free.lock().unwrap();
        if !f.contains(&port) {
            f.push(port);
        }
    }

    pub fn in_use(&self) -> usize {
        self.total - self.free.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_free_exhaust_and_double_free() {
        let p = PortAllocator::new(20000, 2);
        let (a, b) = (p.alloc().unwrap(), p.alloc().unwrap());
        assert_eq!((a, b), (20000, 20001));
        assert!(p.alloc().is_none());
        p.free(a);
        p.free(a); // double free must not duplicate the port
        assert_eq!(p.in_use(), 1);
        assert_eq!(p.alloc(), Some(20000));
        assert!(p.alloc().is_none());
    }
}
