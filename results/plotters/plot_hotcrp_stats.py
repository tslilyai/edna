import matplotlib
import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap


plt.style.use('seaborn-deep')
plt.figure(figsize = (6, 2.5))

# plot styling for paper
matplotlib.rc('font', family='serif', size=11)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=11)
matplotlib.rc('figure', figsize=(6,2.5))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)


def add_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        if y[i] < 0.1:
            label = "{0:.1g}".format(y[i])
        elif y[i] > 100:
            label = "{0:.0f}".format(y[i])
        else:
            label = "{0:.1f}".format(y[i])
        new_offset = offset
        if y[i] < 10 or y[i] > 80:
            new_offset = offset - 8
        plt.text(x[i], y[i]+new_offset, label, ha='center', color=color, size=11)

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset - 8, y[i], ha='center', color=color, size=11)

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

# positions
barwidth = 0.4
X = np.arange(7)
labels = [
        'Create\nAccount',
        'Get\nReviews',
        'Edit Undis-\nguised Data',
        'Remove\nAccount',
        'Anonym.\nAccount',
        'Edit Disguis-\ned Data',
        'Restore\nRem. Acct',
]

# HOTCRP RESULTS
account_durs = []
edit_durs_noanon = []
anon_durs = []
delete_durs = []
restore_durs = []
edit_durs = []
read_durs = []
delete_durs_noanon = []
restore_durs_noanon = []

account_durs_dryrun = []
edit_durs_dryrun_noanon = []
anon_durs_dryrun = []
delete_durs_dryrun = []
restore_durs_dryrun = []
edit_durs_dryrun = []
read_durs_dryrun = []
delete_durs_dryrun_noanon = []
restore_durs_dryrun_noanon = []

account_durs_baseline = []
anon_durs_baseline = []
edit_durs_baseline = []
delete_durs_baseline = []
read_durs_baseline = []

app = "hotcrp"
filename_baseline ="../hotcrp_results/hotcrp_disguise_stats_3080users_baseline.csv"
filename_dryrun="../hotcrp_results/hotcrp_disguise_stats_3080users_nocrypto.csv"
filename ="../hotcrp_results/hotcrp_disguise_stats_3080users.csv"
offset = 20
nusers = 80
with open(filename,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs = [int(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    edit_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
    delete_durs = [float(x)/1000 for x in rows[3].strip().split(',')]
    restore_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_durs_noanon = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_durs_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_durs_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]
    read_durs = [float(x)/1000 for x in rows[8].strip().split(',')]

with open(filename_baseline,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs_baseline = [int(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs_baseline = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    edit_durs_baseline = [float(x)/1000 for x in rows[2].strip().split(',')]
    delete_durs_baseline = [float(x)/1000 for x in rows[3].strip().split(',')]
    read_durs_baseline = [float(x)/1000 for x in rows[8].strip().split(',')]

with open(filename_dryrun,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs_dryrun = [int(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs_dryrun = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    edit_durs_dryrun = [float(x)/1000 for x in rows[2].strip().split(',')]
    delete_durs_dryrun = [float(x)/1000 for x in rows[3].strip().split(',')]
    restore_durs_dryrun = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_durs_dryrun_noanon = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_durs_dryrun_noanon = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_durs_dryrun_noanon = [float(x)/1000 for x in rows[7].strip().split(',')]
    read_durs_dryrun = [float(x)/1000 for x in rows[8].strip().split(',')]

###################### shading
plt.axvspan(-0.5, 2.45, color='white', alpha=0, lw=0)
plt.axvspan(2.45, 6.5, color='purple', alpha=0.08, lw=0)
plt.text(2.55, 168, '\emph{Disguise/Reveal Ops}',
         verticalalignment='top', horizontalalignment='left',
         color='purple', fontsize=11)
plt.margins(x=0.0)

################ add baseline closer to black line for anonymize
plt.bar((X-barwidth/2)[:5],
        [
            statistics.median(account_durs_baseline),
            statistics.median(read_durs_baseline),
            statistics.median(edit_durs_baseline),
            statistics.median(delete_durs_baseline),
            statistics.median(anon_durs_baseline),
        ],
        yerr=get_yerr([
            account_durs_baseline,
            read_durs_baseline,
            edit_durs_baseline,
            delete_durs_baseline,
            anon_durs_baseline,
        ]),
        error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='g', capsize=3, width=barwidth, label="Manual (No Edna)", edgecolor='black', linewidth=0.25)
add_labels((X-barwidth/2)[:5], [
    statistics.median(account_durs_baseline),
    statistics.median(read_durs_baseline),
    statistics.median(edit_durs_baseline),
    statistics.median(delete_durs_baseline),
    statistics.median(anon_durs_baseline),
], plt, 'g', offset)
add_text_labels((X-barwidth/2)[5:], ["N/A", "N/A"], plt, 'g', offset)

############### edna batched
plt.bar((X+barwidth/2), [
    statistics.median(account_durs),
    statistics.median(read_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
],
yerr=get_yerr([
    account_durs,
    read_durs,
    edit_durs_noanon,
    delete_durs_noanon,
    anon_durs,
    edit_durs,
    restore_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='y', capsize=3, width=barwidth, label="Edna", edgecolor='black', linewidth=0.25)

'''plt.bar((X+barwidth/2), [
    statistics.median(account_durs_baseline),
    statistics.median(read_durs_dryrun),
    statistics.median(edit_durs_dryrun_noanon),
    statistics.median(delete_durs_dryrun_noanon),
    statistics.median(anon_durs_dryrun),
    statistics.median(edit_durs_dryrun),
    statistics.median(restore_durs_dryrun_noanon),
],
color='tab:orange', capsize=0,
width=barwidth, label="Edna-NoCrypto", edgecolor='black', linewidth=.25)'''

add_labels((X+barwidth/2),
[
    statistics.median(account_durs),
    statistics.median(read_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
], plt, 'black', offset)
plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=175)
plt.yticks(range(0, 175, 50))
plt.xticks(X, labels=labels, rotation=90)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.tight_layout(h_pad=0)
plt.savefig("{}_op_stats.pdf".format(app))
plt.clf()

print(
    statistics.median(read_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon))

print(
    statistics.median(read_durs_dryrun)/
    statistics.median(read_durs),
    statistics.median(edit_durs_dryrun_noanon)/
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_dryrun_noanon)/
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs_dryrun)/
    statistics.median(anon_durs),
    statistics.median(edit_durs_dryrun)/
    statistics.median(edit_durs),
    statistics.median(restore_durs_dryrun_noanon)/
     statistics.median(restore_durs_noanon))

