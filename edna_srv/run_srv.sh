url --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
PATH=$PATH:$HOME/.cargo/bin
RUSTFLAGS=-Ctarget-feature=-crt-static

# kill the current server
ps -ef | grep 'edna-server' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true

sleep 5

set -e

# start a new server
cargo run --release -- -h mariadb -d lobsters_development --user root --pass password \
    --schema ../applications/lobsters/schema.sql

echo "Server Running, wait a bit"
