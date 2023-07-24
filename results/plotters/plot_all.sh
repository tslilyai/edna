#!/bin/bash
python3 plot_websubmit_stats.py
python3 plot_hotcrp_stats.py
python3 plot_lobsters_stats.py
python3 plot_composition_stats.py
python3 plot_lobsters_concurrent.py

cp *.pdf ../result_graphs/
