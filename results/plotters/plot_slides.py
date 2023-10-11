import matplotlib
import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap

plt.style.use('dark_background')

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]


#plt.style.use('seaborn-deep')
# WEBSUBMIT RESULTS
account_durs = []
edit_durs_noanon = []
leclist_durs = []
answers_durs = []
questions_durs = []
anon_durs = []
delete_durs = []
restore_durs = []
edit_durs = []
delete_durs_noanon = []
restore_durs_noanon = []

account_durs_dryrun = []
edit_durs_dryrun_noanon = []
leclist_durs_dryrun = []
answers_durs_dryrun = []
questions_durs_dryrun = []
anon_durs_dryrun = []
delete_durs_dryrun = []
restore_durs_dryrun = []
edit_durs_dryrun = []
delete_durs_dryrun_noanon = []
restore_durs_dryrun_noanon = []

account_qapla_durs = []
edit_qapla_durs_noanon = []
leclist_qapla_durs = []
answers_qapla_durs = []
questions_qapla_durs = []
anon_qapla_durs = []
delete_qapla_durs = []
restore_qapla_durs = []
edit_qapla_durs = []
delete_qapla_durs_noanon = []
restore_qapla_durs_noanon = []

account_cryptdb_durs = []
edit_cryptdb_durs_noanon = []
leclist_cryptdb_durs = []
answers_cryptdb_durs = []
questions_cryptdb_durs = []
anon_cryptdb_durs = []
delete_cryptdb_durs = []
restore_cryptdb_durs = []
edit_cryptdb_durs = []
delete_cryptdb_durs_noanon = []
restore_cryptdb_durs_noanon = []
login_cryptdb_durs = []
logout_cryptdb_durs = []
admin_login_cryptdb_durs = []
admin_logout_cryptdb_durs = []

account_cryptdb_nocrypto_durs = []
edit_cryptdb_nocrypto_durs_noanon = []
leclist_cryptdb_nocrypto_durs = []
answers_cryptdb_nocrypto_durs = []
questions_cryptdb_nocrypto_durs = []
anon_cryptdb_nocrypto_durs = []
delete_cryptdb_nocrypto_durs = []
restore_cryptdb_nocrypto_durs = []
edit_cryptdb_nocrypto_durs = []
delete_cryptdb_nocrypto_durs_noanon = []
restore_cryptdb_nocrypto_durs_noanon = []

leclist_durs_baseline = []
answers_durs_baseline = []
questions_durs_baseline = []
account_durs_baseline = []
anon_durs_baseline = []
edit_durs_baseline = []
delete_durs_baseline = []

nusers = 2000
app = "websubmit"
filename_baseline='../websubmit_results/disguise_stats_{}lec_{}users_baseline.csv'.format(20,nusers)
filename_dryrun='../websubmit_results/disguise_stats_{}lec_{}users_dryrun.csv'.format(20,nusers)
filename_qapla = '../websubmit_results/qapla_stats_{}lec_{}users.csv'.format(20,nusers)
filename_cryptdb='../websubmit_results/cryptdb_stats_{}lec_{}users_crypto.csv'.format(20,nusers)
#filename_cryptdb_nocrypto='../websubmit_results/cryptdb_stats_{}lec_{}users.csv'.format(20,nusers)
filename='../websubmit_results/disguise_stats_{}lec_{}users.csv'.format(20,nusers)

with open(filename_qapla,'r') as csvfile:
    rows = csvfile.readlines()
    account_qapla_durs = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_qapla_durs = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    leclist_qapla_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_qapla_durs = [int(x)/1000 for x in rows[3].strip().split(',')]
    questions_qapla_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_qapla_durs = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_qapla_durs = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_qapla_durs = [float(x)/1000 for x in rows[7].strip().split(',')]
    edit_qapla_durs_noanon = [float(x)/1000 for x in rows[8].strip().split(',')]
    delete_qapla_durs_noanon = [float(x)/1000 for x in rows[9].strip().split(',')]
    restore_qapla_durs_noanon = [float(x)/1000 for x in rows[10].strip().split(',')]

with open(filename_baseline,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs_baseline = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs_baseline = [(float(x)/1000)/nusers for x in rows[1].strip().split(',')]
    leclist_durs_baseline = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_durs_baseline = [int(x)/1000 for x in rows[3].strip().split(',')]
    questions_durs_baseline = [int(x)/1000 for x in rows[4].strip().split(',')]
    edit_durs_baseline = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_durs_baseline = [float(x)/1000 for x in rows[6].strip().split(',')]

with open(filename_dryrun,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs_dryrun = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs_dryrun = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    leclist_durs_dryrun = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_durs_dryrun = [int(x)/1000 for x in rows[3].strip().split(',')]
    questions_durs_dryrun = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_durs_dryrun = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_durs_dryrun = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_durs_dryrun = [float(x)/1000 for x in rows[7].strip().split(',')]
    edit_durs_dryrun_noanon = [float(x)/1000 for x in rows[8].strip().split(',')]
    delete_durs_dryrun_noanon = [float(x)/1000 for x in rows[9].strip().split(',')]
    restore_durs_dryrun_noanon = [float(x)/1000 for x in rows[10].strip().split(',')]

with open(filename,'r') as csvfile:
    rows = csvfile.readlines()
    account_durs = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_durs = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    leclist_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_durs = [int(x)/1000 for x in rows[3].strip().split(',')]
    questions_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_durs = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_durs = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_durs = [float(x)/1000 for x in rows[7].strip().split(',')]
    edit_durs_noanon = [float(x)/1000 for x in rows[8].strip().split(',')]
    delete_durs_noanon = [float(x)/1000 for x in rows[9].strip().split(',')]
    restore_durs_noanon = [float(x)/1000 for x in rows[10].strip().split(',')]


############### qapla slides

plt.figure(figsize = (25, 10))
matplotlib.rc('font', family='serif', size=55)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=55)
matplotlib.rc('figure', figsize=(10,3))
matplotlib.rc('axes', linewidth=1)
matplotlib.rc('lines', linewidth=1)

barwidth = 0.3
labels = [
        'Get Ans\n(User)',
        'Edit\nDisguised Data',
        'Remove Acc.',
        'Restore\nRemoved Acc.',
]

def add_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        if y[i] < 0.1:
            label = "{0:.1g}".format(y[i])
        elif y[i] > 100:
            label = "{0:.0f}".format(y[i])
        else:
            label = "{0:.1f}".format(y[i])
        new_offset = offset
        if y[i] < 50:
            new_offset = offset - 6
        plt.text(x[i], y[i]+new_offset, label, ha='center', color=color, size=40,
                 )

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset - 6, y[i], ha='center', color=color,size=40)

X = np.arange(4)
offset = 10
plt.axvspan(-0.5, 3.5, color='white', alpha=0, lw=0)
#plt.axvspan(.5, 3.5, color='purple', alpha=0.08, lw=0)
#plt.text(.6, 55, '\emph{Disguise/Reveal Ops}',
         #verticalalignment='top', horizontalalignment='left',
         #color='purple', fontsize=50)

################ baseline
plt.bar((X-.5*barwidth)[:2],
        [
            statistics.median(questions_durs_baseline),
            statistics.median(delete_durs_baseline),
        ],
        yerr=get_yerr([
            questions_durs_baseline,
            delete_durs_baseline,
        ]),
        error_kw=dict(capthick=0.5, ecolor='w', lw=2), color='orange',
        capsize=3, width=barwidth, label="Manual (No Edna)", edgecolor='orange',
        linewidth=1)
add_labels((X-.5*barwidth)[0:1], [
    statistics.median(questions_durs_baseline),
], plt, 'white', offset)
add_text_labels((X-.5*barwidth)[1:2], ["N/A"], plt, 'w', offset)
add_text_labels((X-.5*barwidth)[3:4], ["N/A"], plt, 'w', offset)

add_labels((X-.5*barwidth)[2:3], [
   statistics.median(delete_durs_baseline),
], plt, 'white', offset)
############### edna
plt.bar((X+.5*barwidth), [
    statistics.median(questions_durs),
    statistics.median(edit_durs),
    statistics.median(delete_durs_noanon),
    statistics.median(restore_durs_noanon),
],
yerr=get_yerr([
    questions_durs,
    edit_durs,
    delete_durs_noanon,
    restore_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='w', lw=2), color='m', capsize=3,
        width=barwidth, label="Edna", edgecolor='m', linewidth=1)
add_labels((X+.5*barwidth),
[
    statistics.median(questions_durs),
    statistics.median(edit_durs),
    statistics.median(delete_durs_noanon),
    statistics.median(restore_durs_noanon),
], plt, 'white', offset)


plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=90)
plt.yticks(range(0, 90, 20))
plt.xticks(X, labels=labels)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.margins(x=0.0)
plt.gcf().subplots_adjust(bottom=0.25)
plt.tight_layout(h_pad=0)
plt.savefig("{}_op_stats_slides.png".format(app))
#plt.clf()

###### qapla
plt.bar((X+1*barwidth), [
    statistics.median(questions_qapla_durs),
    statistics.median(edit_qapla_durs),
    statistics.median(delete_qapla_durs_noanon),
    statistics.median(restore_qapla_durs_noanon),
],
yerr=get_yerr([
    questions_qapla_durs,
    edit_qapla_durs,
    delete_qapla_durs_noanon,
    restore_qapla_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='white', lw=2), color='c', capsize=3,
        width=barwidth, label="Qapla", edgecolor='c', linewidth=1)
add_labels((X+1*barwidth),
[
    statistics.median(questions_qapla_durs),
    statistics.median(edit_qapla_durs),
    statistics.median(delete_qapla_durs_noanon),
    statistics.median(restore_qapla_durs_noanon),
], plt, 'white', offset)

plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=90)
plt.yticks(range(0, 90, 20))
plt.xticks(X, labels=labels)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.margins(x=0.0)
plt.gcf().subplots_adjust(bottom=0.25)
plt.tight_layout(h_pad=0)
plt.savefig("{}_qapla_op_stats_slides.png".format(app))
plt.clf()
