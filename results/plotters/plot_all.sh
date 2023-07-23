#!/bin/bash
python3 plot_websubmit_stats.py _persist
python3 plot_hotcrp_stats.py _persist
python3 plot_lobsters_stats.py _persist
python3 plot_composition_stats.py _persist
python3 plot_lobsters_concurrent.py _persist

cp *.pdf result_graphs/
