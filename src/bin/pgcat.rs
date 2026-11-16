// Allocator selection belongs to the executable, never the reusable library:
// an embedding application must remain free to select its own global allocator.
// Only use jemalloc on x86_64; cross-compiling it with zig for ARM64 can crash.
#[cfg(all(not(target_env = "msvc"), target_arch = "x86_64"))]
use jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), target_arch = "x86_64"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Catch removal or mismatched platform cfgs on the allocator declaration.
    #[cfg(all(not(target_env = "msvc"), target_arch = "x86_64"))]
    let _: &Jemalloc = &GLOBAL;

    pgcat::runtime::run()
}
