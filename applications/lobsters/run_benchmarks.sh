#!/bin/bash

cargo build --release
mkdir output &> /dev/null
rm output/*

set -x
db=lobsters_edna
sql=/data/lobsters_edna_messages_and_tags.sql;
scale=2.75

# UPDATE TEST
mysql -utester -ppass --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_BACKTRACE=1 RUST_LOG=warn ../../target/release/lobsters \
    --test 'migrations' \
    --scale $scale \
    --txn \
    &> output/migrations-txn.out
echo "Ran reveal test with txn"
exit

mysql -utester -ppass --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_BACKTRACE=1 RUST_LOG=warn ../../target/release/lobsters \
    --test 'updates' \
    --scale $scale \
    --txn \
    &> output/updates-txn.out
echo "Ran updates test with txn"
exit

# CONCURRENT TEST
for u in 2 13; do
	for d in 'expensive' 'cheap' 'none'; do
    	for txn in '' '--txn'; do
		    mysql -utester -ppass --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
            	    mysql -utester -ppass --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
            #CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --no-inline -F99 -o 14users_cheap.svg -b lobsters -- \
		    RUST_LOG=error ../../target/release/lobsters \
			--scale $scale \
			--nconcurrent $u \
			--disguiser $d \
			--filename "${u}users_${d}${txn}" \
			$txn \
		    &> output/users-$u-${d}-$txn.out
		    echo "Ran concurrent test for $u users 0 sleep ${d}"
	    done
    done
done

##############################
# STATS TEST
mysql -utester -ppass  --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'stats' \
	--scale $scale \
	&> output/users_stats.out
echo "Ran stats test for users"

mysql -utester -ppass  --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'stats' \
	--scale $scale \
	--dryrun \
	&> output/users_stats_dryrun.out
echo "Ran dryrun stats test for users"

mysql -utester -ppass  --execute='DROP DATABASE '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'baseline_hobby_anon' \
	--scale $scale \
	&> output/users_stats_baseline_hobby_anon.out
echo "Ran baseline hobby stats test for users"

mysql -utester -ppass  --execute='DROP DATABASE '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'baseline_stats' \
	--scale $scale \
	&> output/users_stats_baseline_stats.out
echo "Ran baseline default stats test for users"

mysql -utester -ppass  --execute='DROP DATABASE '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'baseline_delete' \
	--scale $scale \
	&> output/users_stats_baseline_delete.out
echo "Ran baseline delete stats test for users"

mysql -utester -ppass  --execute='DROP DATABASE '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'baseline_decay' \
	--scale $scale \
	&> output/users_stats_baseline_decay.out
echo "Ran baseline decay stats test for users"

##############################
# STORAGE TEST
mysql -utester -ppass  --execute='DROP DATABASE IF EXISTS '$db'; CREATE DATABASE '$db';'
mysql -utester -ppass  --execute='use '$db'; set @@max_heap_table_size=4294967295; source '$sql';'
RUST_LOG=error ../../target/release/lobsters \
	--test 'storage' \
	--scale $scale \
	&> output/users_storage.out
echo "Ran storage test for users"
