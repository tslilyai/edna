#!/bin/bash

# install everything necessary
sudo apt update
yes | sudo apt install libboost-all-dev libantlr3c-dev build-essential libglib2.0-dev docker.io python3-pip
yes | sudo apt install texlive texlive-latex-extra texlive-fonts-recommended dvipng cm-super
pip install matplotlib

# move files around to the blockstore with enough room...
sudo chmod ugo+rw -R /data
sudo rm -rf /data/repository
git clone https://github.com/tslilyai/edna.git repository
cd /data/repository
yes | ./config_mysql.sh
cd related_systems/qapla
make; cd examples; make

# done with setup, run everything via ./run_all.sh
