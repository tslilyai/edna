#!/bin/sh
db="myclass_cryptdb"
mysql -uroot -ppass --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
RUST_LOG=error cargo run --release
