#!/bin/sh

sudo apt purge mysql-server mysql-client mysql-common
sudo apt autoremove
sudo rm -rf /var/lib/mysql*

sudo apt update
sudo apt install mysql-server
sudo cp /data/repository/mysqld.cnf /etc/mysql/mysql.conf.d/
sudo ln -s /etc/apparmor.d/usr.sbin.mysqld /etc/apparmor.d/disable/
sudo apparmor_parser -R /etc/apparmor.d/usr.sbin.mysqld
sudo service apparmor restart

sudo rm -rf /data/mysql
sudo mkdir /data/mysql
sudo chown -R mysql:mysql /data/mysql
sudo chmod 750 /data/mysql
sudo chown -R mysql:mysql /var/log/mysql
sudo chmod 755 /var/log/mysql/error.log
sudo chmod 755 /var/log/mysql/error.log
sudo mysqld --initialize-insecure --datadir=/data/mysql --secure-file-priv=NULL
sudo /etc/init.d/mysql restart

sudo apt install libmysqlclient-dev

sudo mysql -uroot -e "CREATE USER 'tester'@'localhost' IDENTIFIED BY 'pass'";
sudo mysql -uroot -e "GRANT ALL PRIVILEGES ON *.* TO 'tester'@'localhost' WITH GRANT OPTION";
sudo mysql -uroot -e "FLUSH PRIVILEGES";
