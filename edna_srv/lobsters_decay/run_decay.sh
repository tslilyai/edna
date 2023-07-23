#curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

source $HOME/.cargo/env

set -e

# eventually loop decay disguise
echo "Running decay"

cd lobsters-decay
RUSTFLAGS=-Ctarget-feature=-crt-static
cargo run --release -- -h mariadb -d lobsters_development
