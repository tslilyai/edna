#!/bin/bash

cargo build --release
rm -rf output
mkdir output
set -x
RUST_LOG=error ../../target/release/hotcrp --prime \
		--nusers_nonpc 3000\
		--nusers_pc 80\
		--npapers_rej 500 \
		--npapers_acc 50 \
		--mysql_user 'tester' \
		--mysql_pass 'pass'\
		--dryrun \
		&> output/users-dryrun.out
echo "Ran dryrun test for users"

RUST_LOG=error ../../target/release/hotcrp --prime \
	--nusers_nonpc 3000\
	--nusers_pc 80\
	--npapers_rej 500 \
	--npapers_acc 50 \
	--mysql_user 'tester' \
	--mysql_pass 'pass'\
	&> output/users.out
echo "Ran test for users"

RUST_LOG=error ../../target/release/hotcrp --prime \
	--nusers_nonpc 3000\
	--nusers_pc 80\
	--npapers_rej 500 \
	--npapers_acc 50 \
	--mysql_user 'tester' \
	--mysql_pass 'pass'\
	--baseline \
	&> output/users-baseline.out
echo "Ran baseline test for users"


