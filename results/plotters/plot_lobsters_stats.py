import matplotlib
import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('seaborn-deep')
plt.figure(figsize = (3.33, 1.4))
barwidth = 0.4

# plot styling for paper
matplotlib.rc('font', family='serif', size=7)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=7)
matplotlib.rc('figure', figsize=(3.33,1.4))
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
        plt.text(x[i], y[i]+offset, label, ha='center', color=color, size=6,rotation=90)

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset, y[i], ha='center', color=color, size=6,rotation=90)

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

# LOBSTERS
account_durs = []
delete_durs = []
restore_durs = []
decay_durs = []
undecay_durs = []
hobby_durs = []
unhobby_durs = []
story_durs = []
frontpage_durs = []

account_durs_dryrun = []
delete_durs_dryrun = []
restore_durs_dryrun = []
decay_durs_dryrun = []
undecay_durs_dryrun = []
hobby_durs_dryrun = []
unhobby_durs_dryrun = []
story_durs_dryrun = []
frontpage_durs_dryrun = []

account_durs_baseline = []
delete_durs_baseline = []
decay_durs_baseline = []
hobby_durs_baseline = []
story_durs_baseline = []
frontpage_durs_baseline = []

with open("../lobsters_results/lobsters_disguise_stats_basic_dryrun.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:-1]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        vals = [x/1000 for x in vals]
        create_edna = vals[0]
        story = vals[1]
        frontpage = vals[2]

        account_durs_dryrun.append(create_edna);
        story_durs_dryrun.append(story);
        frontpage_durs_dryrun.append(frontpage);

with open("../lobsters_results/lobsters_disguise_stats_dryrun.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:-1]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        vals = [x/1000 for x in vals]
        decay = vals[2]
        undecay = vals[3]
        delete = vals[4]
        restore = vals[5]
        hobby= vals[6]
        unhobby= vals[7]

        delete_durs_dryrun.append(delete)
        restore_durs_dryrun.append(restore)
        decay_durs_dryrun.append(decay)
        undecay_durs_dryrun.append(undecay)
        hobby_durs_dryrun.append(hobby)
        unhobby_durs_dryrun.append(unhobby)

with open("../lobsters_results/lobsters_disguise_stats_basic.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:-1]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        vals = [x/1000 for x in vals]
        create_edna = vals[0]
        story = vals[1]
        frontpage = vals[2]

        account_durs.append(create_edna);
        story_durs.append(story);
        frontpage_durs.append(frontpage);

with open("../lobsters_results/lobsters_disguise_stats.csv", 'r') as csvfile:
    rows = csvfile.readlines()[1:-1]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        ndata = vals[1]
        vals = [x/1000 for x in vals]
        decay = vals[2]
        undecay = vals[3]
        delete = vals[4]
        restore = vals[5]
        hobby= vals[6]
        unhobby= vals[7]

        delete_durs.append(delete)
        restore_durs.append(restore)
        decay_durs.append(decay)
        undecay_durs.append(undecay)
        hobby_durs.append(hobby)
        unhobby_durs.append(unhobby)

with open("../lobsters_results/lobsters_disguise_stats_baseline_stats.csv",'r') as csvfile:
    rows = csvfile.readlines()[1:-1]
    for r in rows:
        vals = [int(x.strip()) for x in r.split(",")]
        vals = [x/1000 for x in vals]
        account_durs_baseline.append(vals[0]);
        story_durs_baseline.append(vals[1]);
        frontpage_durs_baseline.append(vals[2]);

with open("../lobsters_results/lobsters_disguise_stats_baseline_delete.csv",'r') as csvfile:
    rows = csvfile.readlines()
    for r in rows:
        baseline_delete = int(r.strip())/1000
        delete_durs_baseline.append(baseline_delete);

with open("../lobsters_results/lobsters_disguise_stats_baseline_decay.csv",'r') as csvfile:
    rows = csvfile.readlines()
    for r in rows:
        baseline_decay = int(r.strip())/1000
        decay_durs_baseline.append(baseline_decay);

with open("../lobsters_results/lobsters_disguise_stats_baseline_hobbyanon.csv",'r') as csvfile:
    rows = csvfile.readlines()
    for r in rows:
        baseline_hobby_anon = int(r.strip())/1000
        hobby_durs_baseline.append(baseline_hobby_anon);


X = np.arange(9)
labels = [
    'Create\nAccount',
    'Read\nStory',
    'Read\nFrontpage',
    'Remove\nAccount',
    'Decay\nAccount',
    'Anon.\nCateg.',
    'Restore\nRem. Acct',
    'Restore\nDec. Acct',
    'Reclaim\nCateg.'
]
offset = 10

###################### shading
plt.axvspan(-0.5, 2.5, color='white', alpha=0, lw=0)
plt.axvspan(2.5, 8.5, color='purple', alpha=0.08, lw=0)
plt.text(2.7, 210, '\emph{Disguise/Reveal Ops}',
         verticalalignment='top', horizontalalignment='left',
         color='purple', fontsize=7)
plt.margins(x=0.0)

######################## NO EDNA
plt.bar((X-barwidth/2)[:6], [
        statistics.median(account_durs_baseline),
        statistics.median(story_durs_baseline),
        statistics.median(frontpage_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline),
        statistics.median(hobby_durs_baseline)
    ],
    yerr=get_yerr([account_durs_baseline, story_durs_baseline, frontpage_durs_baseline, delete_durs_baseline, decay_durs_baseline, hobby_durs_baseline]),
    capsize=3,
    error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='g', width=barwidth, label="Manual (No Edna)", edgecolor='black', linewidth=0.25)
add_labels((X-barwidth/2)[:6], [
        statistics.median(account_durs_baseline),
        statistics.median(story_durs_baseline),
        statistics.median(frontpage_durs_baseline),
        statistics.median(delete_durs_baseline),
        statistics.median(decay_durs_baseline),
        statistics.median(hobby_durs_baseline),
    ], plt, 'g', offset)
add_text_labels((X-barwidth/2)[6:], ["N/A", "N/A", "N/A"], plt, 'g', offset)

######################## EDNA BATCH
plt.bar((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(story_durs),
        statistics.median(frontpage_durs),
        statistics.median(delete_durs),
        statistics.median(decay_durs),
        statistics.median(hobby_durs),
        statistics.median(restore_durs),
        statistics.median(undecay_durs),
        statistics.median(unhobby_durs)
    ],
    yerr=get_yerr([account_durs, story_durs, frontpage_durs, delete_durs, decay_durs, hobby_durs, restore_durs, undecay_durs, unhobby_durs]),
    capsize=3,
    error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='y', width=barwidth, label="Edna", edgecolor='black', linewidth=0.25)
add_labels((X+barwidth/2), [
        statistics.median(account_durs),
        statistics.median(story_durs),
        statistics.median(frontpage_durs),
        statistics.median(delete_durs),
        statistics.median(decay_durs),
        statistics.median(hobby_durs),
        statistics.median(restore_durs),
        statistics.median(undecay_durs),
        statistics.median(unhobby_durs)
    ], plt, 'black', offset)

'''plt.bar((X+barwidth/2), [
        statistics.median(account_durs_baseline),
        statistics.median(story_durs_dryrun),
        statistics.median(frontpage_durs_dryrun),
        statistics.median(delete_durs_dryrun),
        statistics.median(decay_durs_dryrun),
        statistics.median(hobby_durs_dryrun),
        statistics.median(restore_durs_dryrun),
        statistics.median(undecay_durs_dryrun),
        statistics.median(unhobby_durs_dryrun)
    ],
    capsize=0, color='tab:orange', width=barwidth, label="Edna-NoCrypto",
    edgecolor='black', linewidth=0.25)'''


plt.ylabel('Time (ms)')
#plt.ylim(ymin=0, ymax=np.percentile(restore_durs,95)*1.1)
matplotlib.rc('font', family='serif', size=6)
plt.ylim(ymin=0, ymax=225)
plt.yticks(range(0, 225, 50))
plt.xticks(X, labels=labels, rotation=90)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055,
           labelspacing=0.1);
plt.tight_layout(h_pad=0)
plt.savefig('lobsters_op_stats.pdf', dpi=300)

print(
        statistics.median(story_durs),
        statistics.median(frontpage_durs),
        statistics.median(delete_durs),
        statistics.median(decay_durs),
        statistics.median(hobby_durs),
        statistics.median(restore_durs),
        statistics.median(undecay_durs),
        statistics.median(unhobby_durs))
print(
        statistics.median(story_durs_dryrun),
        statistics.median(frontpage_durs_dryrun),
        statistics.median(delete_durs_dryrun),
        statistics.median(decay_durs_dryrun),
        statistics.median(hobby_durs_dryrun),
        statistics.median(restore_durs_dryrun),
        statistics.median(undecay_durs_dryrun),
        statistics.median(unhobby_durs_dryrun))

