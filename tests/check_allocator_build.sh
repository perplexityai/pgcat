#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
logs=$(mktemp -d)
trap 'rm -rf "$logs"' EXIT

# Positive controls exercise the real library, an embedding executable with its
# own allocator, and the standalone binary before testing invalid invocations.
cargo build --lib --features test-plugins
cargo build --features test-plugins --example registered_plugin
cargo build --bin pgcat --features test-plugins

# Simulate both a stale build root and an upstream sync restoring fn main().
# Work on a copy: the normal binary and the developer's checkout stay untouched.
cp -R src "$logs/src"
cp Cargo.lock "$logs/Cargo.lock"
ln -s "$PWD/tests" "$logs/tests"
sed 's|path = "src/bin/pgcat.rs"|path = "src/main.rs"|' Cargo.toml >"$logs/Cargo.toml"
printf '\nfn main() {}\n' >>"$logs/src/main.rs"
target_dir="${CARGO_TARGET_DIR:-target}"
mkdir -p "$target_dir"
target_dir=$(cd "$target_dir" && pwd)
if CARGO_TARGET_DIR="$target_dir" cargo check --manifest-path "$logs/Cargo.toml" \
    --bin pgcat --features test-plugins >"$logs/stale-root" 2>&1; then
    echo "ERROR: the retained startup module compiled as an executable" >&2
    exit 1
fi
if ! grep -q 'error\[E0433\]' "$logs/stale-root" ||
    ! grep -q 'upstream_main' "$logs/stale-root"; then
    cat "$logs/stale-root" >&2
    exit 1
fi

# Test the active allocator, not just the presence of a static with the right
# type. Cargo and external builds can use different jemalloc symbol prefixes.
case "$(rustc -vV | sed -n 's/^host: //p')" in
    x86_64-*-linux-*|x86_64-apple-darwin)
        MALLOC_CONF=confirm_conf:true _RJEM_MALLOC_CONF=confirm_conf:true \
            "${CARGO_TARGET_DIR:-target}/debug/pgcat" --help >"$logs/allocator-active" 2>&1
        if ! grep -q '<jemalloc>:.*confirm_conf:true' "$logs/allocator-active"; then
            cat "$logs/allocator-active" >&2
            echo "ERROR: the standalone executable did not initialize jemalloc" >&2
            exit 1
        fi
        ;;
esac

echo "Allocator build contract passed"
