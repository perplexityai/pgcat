//! Seeded random inputs for the fuzz tests. Each case has its own seed, so a
//! failure names the input that reproduces it, and every run tries the same
//! inputs.

/// xorshift64*: small, and the same on every platform.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number in `0..n`.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    pub fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    /// Up to `max` random bytes.
    pub fn bytes(&mut self, max: usize) -> Vec<u8> {
        let len = self.below(max + 1);
        (0..len).map(|_| self.next_u64() as u8).collect()
    }

    /// A few random edits to a protocol message (code, length, body): bytes
    /// changed, inserted or cut, or the body cut short. Half the time the
    /// length is made to match again, so parsers that trust it get past it.
    pub fn mutate(&mut self, message: &[u8]) -> Vec<u8> {
        let mut message = message.to_vec();
        for _ in 0..1 + self.below(3) {
            let at = self.below(message.len() + 1);
            match self.below(4) {
                0 if at < message.len() => message[at] = self.next_u64() as u8,
                1 => message.insert(at, self.next_u64() as u8),
                2 if at < message.len() => drop(message.remove(at)),
                _ => message.truncate(at),
            }
        }
        if message.len() >= 5 && self.one_in(2) {
            let len = (message.len() - 1) as i32;
            message[1..5].copy_from_slice(&len.to_be_bytes());
        }
        message
    }
}
