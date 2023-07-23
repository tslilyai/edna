#!/bin/sh

mysql -utester -ppass -s -N -e "SELECT GROUP_CONCAT(CONCAT('DROP DATABASE ', schema_name, ';') SEPARATOR ' ') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema');" | grep -v "NULL" | mysql -utester -ppass
(cd applications/websubmit-rs/edna-server; ./run_benchmarks.sh)

mysql -utester -ppass -s -N -e "SELECT GROUP_CONCAT(CONCAT('DROP DATABASE ', schema_name, ';') SEPARATOR ' ') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema');" | grep -v "NULL" | mysql -utester -ppass
(cd applications/websubmit-rs/qapla-server; ./run_benchmarks.sh)

# additionally spins off another process that runs the cryptdb proxy server
mmysql -utester -ppass -s -N -e "SELECT GROUP_CONCAT(CONCAT('DROP DATABASE ', schema_name, ';') SEPARATOR ' ') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema');" | grep -v "NULL" | mysql -utester -ppass
(cd applications/websubmit-rs/cryptdb-server; ./run_benchmarks.sh)

mysql -utester -ppass -s -N -e "SELECT GROUP_CONCAT(CONCAT('DROP DATABASE ', schema_name, ';') SEPARATOR ' ') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema');" | grep -v "NULL" | mysql -utester -ppass
(cd applications/hotcrp/; ./run_benchmarks.sh)

mmysql -utester -ppass -s -N -e "SELECT GROUP_CONCAT(CONCAT('DROP DATABASE ', schema_name, ';') SEPARATOR ' ') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema');" | grep -v "NULL" | mysql -utester -ppass
(cd applications/lobsters/; ./run_benchmarks.sh)

cd results/plotters; ./plot_all.sh
