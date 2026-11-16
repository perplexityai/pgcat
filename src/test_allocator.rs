//! The unit tests' allocator: the system's, recording the largest single
//! allocation each thread asks for, so a test can bound what code allocates.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct Recording;

thread_local! {
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn record(size: usize) {
    // Not during the thread's teardown, when the slot is gone.
    let _ = LARGEST.try_with(|largest| largest.set(largest.get().max(size)));
}

unsafe impl GlobalAlloc for Recording {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: Recording = Recording;

/// The largest single allocation this thread asked for since the last call.
pub fn take_largest() -> usize {
    LARGEST.with(|largest| largest.replace(0))
}
