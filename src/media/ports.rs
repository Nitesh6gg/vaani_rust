//! Port pool for per-call media sockets: a port is never shared across calls.

use std::{collections::VecDeque, sync::Mutex};

pub struct PortAllocator {
    /// Oldest-freed first: a returned port goes to the back, so the previous call has the
    /// longest time to finish closing it before it is handed out again.
    free: Mutex<VecDeque<u16>>,
    total: usize,
}

impl PortAllocator {
    pub fn new(base: u16, count: u16) -> Self {
        let free: VecDeque<u16> = (base..base + count).collect();
        Self { total: free.len(), free: Mutex::new(free) }
    }

    pub fn alloc(&self) -> Option<u16> {
        self.free.lock().unwrap().pop_front()
    }

    pub fn free(&self, port: u16) {
        let mut f = self.free.lock().unwrap();
        if !f.contains(&port) {
            f.push_back(port);
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

    #[test]
    fn a_freed_port_is_reused_last() {
        let p = PortAllocator::new(20000, 3);
        let a = p.alloc().unwrap();
        p.free(a);
        assert_eq!(p.alloc(), Some(20001), "the port that rested longest comes first");
        assert_eq!(p.alloc(), Some(20002));
        assert_eq!(p.alloc(), Some(20000), "the just-freed port only when the others are taken");
    }
}
