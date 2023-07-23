sudo apt purge mysql-server mysql-client mysql-common
sudo apt autoremove
sudo rm -rf /var/lib/mysql*

sudo apt update
sudo apt install mysql-server
sudo cp /data/repository/mysqld.cnf /etc/mysql/mysql.conf.d/
sudo /etc/init.d/mysql start
sudo apt install libmysqlclient-dev

sudo mysql -uroot -e "CREATE USER 'tester'@'localhost' IDENTIFIED BY 'pass'";
sudo mysql -uroot -e "GRANT ALL PRIVILEGES ON *.* TO 'tester'@'localhost' WITH GRANT OPTION";
sudo mysql -uroot -e "FLUSH PRIVILEGES";
