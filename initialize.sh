#!/bin/bash

# install everything necessary
sudo apt update
yes | sudo apt install libboost-all-dev libantlr3c-dev build-essential libglib2.0-dev cargo docker.io python3-pip
pip install matplotlib

# move files around to the blockstore with enough room...
sudo chmod ugo+rw -R /data
#git clone https://github.com/tslilyai/edna.git repository
mv /local/repository repository
sudo find /data -type f -name *.sh -exec chmod ugo+x {} \
cd /data
cd repository
yes | ./config_mysql.sh
cd related_systems/qapla
make; cd examples; make

# done with setup, run everything via ./run_all.sh
