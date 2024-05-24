#!/bin/bash

cargo build --release
mkdir output &> /dev/null || true
BUILDDIR="/data/repository"

l=20
u=2000

for baseline in false true; do
    mysql -utester -ppass --execute='DROP DATABASE IF EXISTS myclass; CREATE DATABASE myclass;'
        RUST_LOG=error $BUILDDIR/target/release/websubmit-srv \
        -i myclass --schema src/schema.sql --config sample-config.toml --port 3306 \
        --benchmark true --prime true --baseline $baseline --proxy false \
        --nusers $u --nlec $l --nqs 4 &> \
        output/${l}lec_${u}users_baseline_${baseline}.out
        echo "Ran baseline $baseline test for $l lecture and $u users"
done

# run the dryrun
mysql -utester -ppass --execute='DROP DATABASE IF EXISTS myclass; CREATE DATABASE myclass;'
    RUST_LOG=error $BUILDDIR/target/release/websubmit-srv \
    -i myclass --schema src/schema.sql --config sample-config.toml --port 3306 \
    --benchmark true --prime true --baseline false --proxy false \
    --nusers $u --nlec $l --nqs 4 --dryrun true &> \
    output/${l}lec_${u}users_dryrun.out
    echo "Ran dryrun for $l lecture and $u users"

rm *txt &> /dev/null
