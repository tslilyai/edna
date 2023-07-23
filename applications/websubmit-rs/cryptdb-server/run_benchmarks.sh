#!/bin/bash

cargo build --release
mkdir output &> /dev/null || true

# spin off proxy
(cd ../../proxy; ./run.sh)

crypto=true
schema="src/schema_nocrypto.sql"
BUILDDIR=/local/repository

if $crypto; then
	schema="src/schema.sql"
fi

for l in 20; do
    for u in 2000; do
	RUST_LOG=error BUILDDIR/target/release/cryptdb-srv \
		-i myclass_cryptdb --schema $schema --config sample-config.toml \
		--benchmark true --crypto $crypto \
    	--nusers $u --nlec $l --nqs 4 &> \
    	output/${l}lec_${u}users_$crypto.out
	echo "Ran test for $l lecture and $u users"
    done
done
rm *txt
