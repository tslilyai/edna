#!/bin/bash

# install everything necessary
sudo apt update
yes | sudo apt install libboost-all-dev libantlr3c-dev build-essential libglib2.0-dev docker.io python3-pip
yes | sudo apt install texlive texlive-latex-extra texlive-fonts-recommended dvipng cm-super
pip install matplotlib

# install rust
yes | sudo apt remove rustc cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh #take the default options
source $HOME/.cargo/env

# move files around to the blockstore with enough room...
sudo chmod ugo+rw -R /data
sudo rm -rf /data/repository
git clone https://github.com/tslilyai/edna.git /data/repository
cd /data/repository
yes | .config/config_mysql.sh
cd related_systems/qapla
make; cd examples; make

# done with setup, run everything via ./run_all.sh
