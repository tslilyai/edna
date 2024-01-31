#!/bin/bash

cargo build --release
mkdir output &> /dev/null || true

# spin off proxy
sudo killall proxy
cd ../../proxy; ./run.sh
cd ../websubmit-rs/cryptdb-server/

crypto=true
schema="src/schema_nocrypto.sql"
BUILDDIR=/data/repository

if $crypto; then
	schema="src/schema.sql"
fi

l=20
u=2000
RUST_LOG=error $BUILDDIR/target/release/cryptdb-srv \
    -i myclass_cryptdb --schema $schema --config sample-config.toml \
    --benchmark true --crypto $crypto \
    --nusers $u --nlec $l --nqs 4 &> \
    output/${l}lec_${u}users_$crypto.out
echo "Ran test for $l lecture and $u users"
rm *txt
