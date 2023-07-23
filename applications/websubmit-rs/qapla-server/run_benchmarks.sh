#!/bin/bash

cargo build --release
mkdir output &> /dev/null || true
set -x 

for l in 20; do
    for u in 2000; do
	RUST_LOG=error target/release/qapla-server \
		-i myclass_qapla --schema src/schema.sql --config sample-config.toml \
		--benchmark true \
		--nusers $u --nlec $l --nqs 4 &> \
	    output/${l}lec_${u}users.out
	echo "Ran test for $l lecture and $u users"
    done
done

rm *txt
