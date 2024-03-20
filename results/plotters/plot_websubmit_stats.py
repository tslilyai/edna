import matplotlib
import matplotlib.pyplot as plt
import csv
import statistics
import sys
import numpy as np
from textwrap import wrap


plt.style.use('seaborn-deep')
plt.figure(figsize = (6, 2.5))
barwidth = 0.4

# plot styling for paper
matplotlib.rc('font', family='serif', size=11)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=11)
matplotlib.rc('figure', figsize=(6,2.5))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)

labels = [
        'Create\nAccount',
        'Get Ans\n(User)',
        'Get Ans\n(Admin)',
        'Edit Undis-\nguised Data',
        'Remove\nAccount',
        'Anonym.\nAccount',
        'Edit Disguis-\ned Data',
        'Restore\nRem. Acct',
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
        if y[i] < 17:
            new_offset = offset - 4
        plt.text(x[i], y[i]+new_offset, label, ha='center', color=color, size=11)

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset - 4, y[i], ha='center', color=color, size=11)

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

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

'''with open(filename_cryptdb_nocrypto,'r') as csvfile:
    rows = csvfile.readlines()
    account_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_cryptdb_nocrypto_durs = [(int(x)/1000)/nusers for x in rows[1].strip().split(',')]
    leclist_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_cryptdb_nocrypto_durs = [int(x)/1000 for x in rows[3].strip().split(',')]
    questions_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_cryptdb_nocrypto_durs = [float(x)/1000 for x in rows[7].strip().split(',')]
    edit_cryptdb_nocrypto_durs_noanon = [float(x)/1000 for x in rows[8].strip().split(',')]
    delete_cryptdb_nocrypto_durs_noanon = [float(x)/1000 for x in rows[9].strip().split(',')]
    restore_cryptdb_nocrypto_durs_noanon = [float(x)/1000 for x in rows[10].strip().split(',')]
    '''

with open(filename_cryptdb,'r') as csvfile:
    rows = csvfile.readlines()
    account_cryptdb_durs = [float(x)/1000 for x in rows[0].strip().split(',')]
    anon_cryptdb_durs = [(float(x)/1000)/nusers for x in rows[1].strip().split(',')]
    #leclist_cryptdb_durs = [float(x)/1000 for x in rows[2].strip().split(',')]
    answers_cryptdb_durs = [(int(x)/1000) for x in rows[3].strip().split(',')]
    questions_cryptdb_durs = [float(x)/1000 for x in rows[4].strip().split(',')]
    edit_cryptdb_durs = [float(x)/1000 for x in rows[5].strip().split(',')]
    delete_cryptdb_durs = [float(x)/1000 for x in rows[6].strip().split(',')]
    restore_cryptdb_durs = [float(x)/1000 for x in rows[7].strip().split(',')]
    edit_cryptdb_durs_noanon = [float(x)/1000 for x in rows[8].strip().split(',')]
    delete_cryptdb_durs_noanon = [float(x)/1000 for x in rows[9].strip().split(',')]
    restore_cryptdb_durs_noanon = [float(x)/1000 for x in rows[10].strip().split(',')]
    #admin_login_cryptdb_durs = [float(x)/1000 for x in rows[11].strip().split(',')]
    #admin_logout_cryptdb_durs = [float(x)/1000 for x in rows[12].strip().split(',')]
    #login_cryptdb_durs = [float(x)/1000 for x in rows[13].strip().split(',')]
    #logout_cryptdb_durs = [float(x)/1000 for x in rows[14].strip().split(',')]
print(answers_cryptdb_durs)

############################
# graph for edna functions
############################

# positions
X = np.arange(8)
offset = 14
plt.axvspan(-0.5, 3.5, color='white', alpha=0, lw=0)
plt.axvspan(3.5, 7.5, color='purple', alpha=0.08, lw=0)
plt.text(3.6, 168, '\emph{Disguise/Reveal Ops}',
         verticalalignment='top', horizontalalignment='left',
         color='purple', fontsize=11)

################ add baseline closer to black line for anonymize
plt.bar((X-0.5*barwidth)[:6],
        [
            statistics.median(account_durs_baseline),
            statistics.median(questions_durs_baseline),
            statistics.median(answers_durs_baseline),
            statistics.median(edit_durs_baseline),
            statistics.median(delete_durs_baseline),
            statistics.median(anon_durs_baseline),
        ],
        yerr=get_yerr([
            account_durs_baseline,
            questions_durs_baseline,
            answers_durs_baseline,
            edit_durs_baseline,
            delete_durs_baseline,
            anon_durs_baseline,
        ]),
        error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='g', capsize=3, width=barwidth, label="Manual (No Edna)", edgecolor='black', linewidth=0.25)
add_labels((X-.5*barwidth)[:6], [
    statistics.median(account_durs_baseline),
    statistics.median(questions_durs_baseline),
    statistics.median(answers_durs_baseline),
    statistics.median(edit_durs_baseline),
    statistics.median(delete_durs_baseline),
    statistics.median(anon_durs_baseline),
], plt, 'g', offset)
add_text_labels((X-.5*barwidth)[6:], ["N/A", "N/A"], plt, 'g', offset)

############### edna
plt.bar((X+0.5*barwidth), [
    statistics.median(account_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
],
yerr=get_yerr([
    account_durs,
    questions_durs,
    answers_durs,
    edit_durs_noanon,
    delete_durs_noanon,
    anon_durs,
    edit_durs,
    restore_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='y', capsize=3, width=barwidth, label="Edna", edgecolor='black', linewidth=0.25)

'''plt.bar((X+0.5*barwidth), [
    statistics.median(account_durs_baseline),
    statistics.median(questions_durs_dryrun),
    statistics.median(answers_durs_dryrun),
    statistics.median(edit_durs_dryrun_noanon),
    statistics.median(delete_durs_dryrun_noanon),
    statistics.median(anon_durs_dryrun),
    statistics.median(edit_durs_dryrun),
    statistics.median(restore_durs_dryrun_noanon),
],
color='tab:orange', capsize=0,
width=barwidth, label="Edna-NoCrypto", edgecolor=black', linewidth=.25)'''


add_labels((X+0.5*barwidth),
[
    statistics.median(account_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
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
plt.margins(x=0.0)
plt.tight_layout(h_pad=0)
plt.savefig("{}_op_stats.pdf".format(app))
plt.clf()

###############
# QAPLA GRAPH
###############

offset = 14
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
        plt.text(x[i], y[i]+new_offset, label, ha='center', color=color, size=11,
                 rotation = 90)

def add_text_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        plt.text(x[i], offset - 6, y[i], ha='center', color=color,size=11,rotation=90)

barwidth = 0.25
plt.figure(figsize = (6, 2.5))
X = np.arange(8)
offset = 25
plt.axvspan(-0.5, 3.5, color='white', alpha=0, lw=0)
plt.axvspan(3.5, 7.5, color='purple', alpha=0.08, lw=0)
plt.text(3.6, 330, '\emph{Disguise/Reveal Ops}',
         verticalalignment='top', horizontalalignment='left',
         color='purple', fontsize=11)

################ baseline
plt.bar((X-1*barwidth)[:6],
        [
            statistics.median(account_durs_baseline),
            #statistics.median(leclist_durs_baseline),
            statistics.median(questions_durs_baseline),
            statistics.median(answers_durs_baseline),
            statistics.median(edit_durs_baseline),
            statistics.median(delete_durs_baseline),
            statistics.median(anon_durs_baseline),
        ],
        yerr=get_yerr([
            account_durs_baseline,
            #leclist_durs_baseline,
            questions_durs_baseline,
            answers_durs_baseline,
            edit_durs_baseline,
            delete_durs_baseline,
            anon_durs_baseline,
        ]),
        error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='g', capsize=3, width=barwidth, label="Manual (No Edna)", edgecolor='black', linewidth=0.25)
add_labels((X-1*barwidth)[:6], [
    statistics.median(account_durs_baseline),
    #statistics.median(leclist_durs_baseline),
    statistics.median(questions_durs_baseline),
    statistics.median(answers_durs_baseline),
    statistics.median(edit_durs_baseline),
    statistics.median(delete_durs_baseline),
    statistics.median(anon_durs_baseline),
], plt, 'g', offset)
add_text_labels((X-1*barwidth)[6:], ["N/A", "N/A"], plt, 'g', offset)

############### edna
plt.bar((X+0*barwidth), [
    statistics.median(account_durs),
    #statistics.median(leclist_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
],
yerr=get_yerr([
    account_durs,
    #leclist_durs,
    questions_durs,
    answers_durs,
    edit_durs_noanon,
    delete_durs_noanon,
    anon_durs,
    edit_durs,
    restore_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='y', capsize=3, width=barwidth, label="Edna", edgecolor='black', linewidth=0.25)
add_labels((X+0*barwidth),
[
    statistics.median(account_durs),
    #statistics.median(leclist_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
], plt, 'black', offset)


############### qapla
plt.bar((X+1*barwidth), [
    statistics.median(account_qapla_durs),
    #statistics.median(leclist_qapla_durs),
    statistics.median(questions_qapla_durs),
    statistics.median(answers_qapla_durs),
    statistics.median(edit_qapla_durs_noanon),
    statistics.median(delete_qapla_durs_noanon),
    statistics.median(anon_qapla_durs),
    statistics.median(edit_qapla_durs),
    statistics.median(restore_qapla_durs_noanon),
],
yerr=get_yerr([
    account_qapla_durs,
    #leclist_qapla_durs,
    questions_qapla_durs,
    answers_qapla_durs,
    edit_qapla_durs_noanon,
    delete_qapla_durs_noanon,
    anon_qapla_durs,
    edit_qapla_durs,
    restore_qapla_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='b', capsize=3, width=barwidth, label="Qapla", edgecolor='black', linewidth=0.25)
add_labels((X+1*barwidth),
[
    statistics.median(account_qapla_durs),
    #statistics.median(leclist_qapla_durs),
    statistics.median(questions_qapla_durs),
    statistics.median(answers_qapla_durs),
    statistics.median(edit_qapla_durs_noanon),
    statistics.median(delete_qapla_durs_noanon),
    statistics.median(anon_qapla_durs),
    statistics.median(edit_qapla_durs),
    statistics.median(restore_qapla_durs_noanon),
], plt, 'blue', offset)

plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=340)
plt.yticks(range(0, 340, 75))
plt.xticks(X, labels=labels, rotation=90)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.margins(x=0.0)
plt.tight_layout(h_pad=0)
plt.savefig("{}_qapla_op_stats.pdf".format(app))
plt.clf()

###############
# CRYPTDB GRAPH
#############
plt.figure(figsize = (6, 2.5))
X = np.arange(8)
offset = 25
plt.axvspan(-0.5, 3.5, color='white', alpha=0, lw=0)
plt.axvspan(3.5, 7.5, color='purple', alpha=0.08, lw=0)
plt.text(3.6, 210, '\emph{Disguise/Reveal Ops}',
         verticalalignment='top', horizontalalignment='left',
         color='purple', fontsize=11)

################ baseline
plt.bar((X-1*barwidth)[:6],
        [
            statistics.median(account_durs_baseline),
            #statistics.median(leclist_durs_baseline),
            statistics.median(questions_durs_baseline),
            statistics.median(answers_durs_baseline),
            statistics.median(edit_durs_baseline),
            statistics.median(delete_durs_baseline),
            statistics.median(anon_durs_baseline),
        ],
        yerr=get_yerr([
            account_durs_baseline,
            #leclist_durs_baseline,
            questions_durs_baseline,
            answers_durs_baseline,
            edit_durs_baseline,
            delete_durs_baseline,
            anon_durs_baseline,
        ]),
        error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='g', capsize=3, width=barwidth, label="Manual (No Edna)", edgecolor='black', linewidth=0.25)
add_labels((X-1*barwidth)[:6], [
    statistics.median(account_durs_baseline),
    #statistics.median(leclist_durs_baseline),
    statistics.median(questions_durs_baseline),
    statistics.median(answers_durs_baseline),
    statistics.median(edit_durs_baseline),
    statistics.median(delete_durs_baseline),
    statistics.median(anon_durs_baseline),
], plt, 'g', offset)
add_text_labels((X-1*barwidth)[6:], ["N/A", "N/A"], plt, 'g', offset)

############### edna
plt.bar((X+0*barwidth), [
    statistics.median(account_durs),
    #statistics.median(leclist_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
],
yerr=get_yerr([
    account_durs,
    #leclist_durs,
    questions_durs,
    answers_durs,
    edit_durs_noanon,
    delete_durs_noanon,
    anon_durs,
    edit_durs,
    restore_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='y', capsize=3, width=barwidth, label="Edna", edgecolor='black', linewidth=0.25)
add_labels((X+0*barwidth),
[
    statistics.median(account_durs),
    #statistics.median(leclist_durs),
    statistics.median(questions_durs),
    statistics.median(answers_durs),
    statistics.median(edit_durs_noanon),
    statistics.median(delete_durs_noanon),
    statistics.median(anon_durs),
    statistics.median(edit_durs),
    statistics.median(restore_durs_noanon),
], plt, 'black', offset)

############### cryptdb
plt.bar((X+1*barwidth), [
    statistics.median(account_cryptdb_durs),
    #statistics.median(leclist_cryptdb_durs),
    statistics.median(questions_cryptdb_durs),
    statistics.median(answers_cryptdb_durs),
    statistics.median(edit_cryptdb_durs_noanon),
    statistics.median(delete_cryptdb_durs_noanon),
    statistics.median(anon_cryptdb_durs),
    statistics.median(edit_cryptdb_durs),
    statistics.median(restore_cryptdb_durs_noanon),
],
yerr=get_yerr([
    account_cryptdb_durs,
    #leclist_cryptdb_durs,
    questions_cryptdb_durs,
    answers_cryptdb_durs,
    edit_cryptdb_durs_noanon,
    delete_cryptdb_durs_noanon,
    anon_cryptdb_durs,
    edit_cryptdb_durs,
    restore_cryptdb_durs_noanon,
]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='red', capsize=3, width=barwidth, label="EdnaCryptDB", edgecolor='black', linewidth=0.25)
add_labels((X+1*barwidth),
[
    statistics.median(account_cryptdb_durs),
    #statistics.median(leclist_cryptdb_durs),
    statistics.median(questions_cryptdb_durs),
    statistics.median(answers_cryptdb_durs),
    statistics.median(edit_cryptdb_durs_noanon),
    statistics.median(delete_cryptdb_durs_noanon),
    statistics.median(anon_cryptdb_durs),
    statistics.median(edit_cryptdb_durs),
    statistics.median(restore_cryptdb_durs_noanon),
], plt, 'red', offset)

plt.ylabel('Time (ms)')
plt.ylim(ymin=0, ymax=225)
plt.yticks(range(0, 225, 50))
plt.xticks(X, labels=labels, rotation=90)
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.margins(x=0.0)
plt.tight_layout(h_pad=0)
plt.savefig("{}_cryptdb_op_stats.pdf".format(app))
plt.clf()

#print(
    #statistics.median(login_cryptdb_durs),
    #statistics.median(logout_cryptdb_durs),
    #statistics.median(admin_login_cryptdb_durs),
    #statistics.median(admin_logout_cryptdb_durs),
#)

#print(statistics.median(account_cryptdb_nocrypto_durs)/statistics.median(account_durs),
    #statistics.median(leclist_cryptdb_nocrypto_durs)/statistics.median(leclist_durs),
    #statistics.median(questions_cryptdb_nocrypto_durs)/statistics.median(questions_durs),
    #statistics.median(answers_cryptdb_nocrypto_durs)/statistics.median(answers_durs),
    #statistics.median(edit_cryptdb_nocrypto_durs_noanon)/statistics.median(edit_durs_noanon),
    #statistics.median(delete_cryptdb_nocrypto_durs_noanon)/statistics.median(delete_durs_noanon),
    #statistics.median(anon_cryptdb_nocrypto_durs)/statistics.median(anon_durs),
    #statistics.median(edit_cryptdb_nocrypto_durs)/statistics.median(edit_durs),
    #statistics.median(restore_cryptdb_nocrypto_durs_noanon)/statistics.median(restore_durs_noanon),
      #)

print(
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


