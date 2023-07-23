#!/bin/bash

BUILDDIR="/data/repository"

cargo build --release
mysql -utester -ppass --execute='DROP DATABASE IF EXISTS myclass; CREATE DATABASE myclass;'
RUST_LOG=debug $BUILDDIR/target/release/websubmit-srv -i myclass --schema src/schema.sql --config sample-config.toml --port 3306 --prime true --benchmark false
