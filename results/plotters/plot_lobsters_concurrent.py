import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import csv
import statistics
import sys
from collections import defaultdict
import matplotlib.colors as mcolors

plt.style.use('seaborn-deep')

# plot styling for paper
matplotlib.rc('font', family='serif', size=8)
matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
matplotlib.rc('text', usetex=True)
matplotlib.rc('legend', fontsize=7)
matplotlib.rc('figure', figsize=(3.33,1.2))
matplotlib.rc('axes', linewidth=0.5)
matplotlib.rc('lines', linewidth=0.5)

def add_labels(x,y,plt,color,offset):
    for i in range(len(x)):
        if y[i] < 0.1:
            label = "{0:.1g}".format(y[i])
        elif y[i] > 100:
            label = "{0:.0f}".format(y[i])
        else:
            label = "{0:.2f}".format(y[i])
        new_offset = offset
        if y[i] < 1000:
            new_offset = offset - 300
        elif y[i] > 3400:
            new_offset = offset - 200
        plt.text(x[i], y[i]+new_offset, label, ha='center', color=color, size=6)


barwidth = 0.15
# positions
X = np.arange(2)
labels = ['Low Load', 'High Load']
HIGH_LOAD = 13
LOW_LOAD = 2
TOTAL_TIME = 100000.0
BUCKET_TIME = 25000

# collect all results
op_results = defaultdict(list)
op_results_txn = defaultdict(list)
delete_results = defaultdict(list)
delete_results_txn = defaultdict(list)
restore_results = defaultdict(list)
restore_results_txn = defaultdict(list)

fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(3.33, 1.2))

def get_yerr(durs):
    mins = []
    maxes = []
    for i in range(len(durs)):
        mins.append(statistics.median(durs[i]) - np.percentile(durs[i], 5))
        maxes.append(np.percentile(durs[i], 95)-statistics.median(durs[i]))
    return [mins, maxes]

def get_data_count(filename, results, i, u):
    vals = defaultdict(list)
    btime = BUCKET_TIME
    if "expensive" in filename:
        btime = BUCKET_TIME
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        ndisguises = int(rows[0].strip())
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        for (i, x) in enumerate(oppairs):
            bucket = int(int(x[0]) / btime)
            # only count when test has gotten underway; skip end buckets
            if bucket < 1 or bucket > 19 or ('none' in filename and bucket > 3):
                continue
            val = float(x[1])/1000
            vals[bucket].append(val)
        if len(vals) == 0:
            results[u].append(0)

        lvals = []
        for bucket, vs in vals.items():
            lvals.append((float(len(vs))/btime)*1000) # ops/ms * 1000ms/s
        results[u].append(lvals)

def get_data_values(filename, results, i, u):
    vals = []
    with open(filename,'r') as csvfile:
        rows = csvfile.readlines()
        ndisguises = int(rows[0].strip())
        oppairs = [x.split(':') for x in rows[i].strip().split(',')]
        for (i, x) in enumerate(oppairs):
            bucket = int(int(x[0]) / BUCKET_TIME)
            # only count when test has gotten underway; skip end buckets
            if bucket < 1 or bucket > 19:
                continue

            val = float(x[1])/1000
            vals.append(val)
        if len(vals) == 0:
            results[u].append([0,0])
        results[u].append(vals)

users = [LOW_LOAD, HIGH_LOAD]
disguiser = ['none', 'cheap', 'expensive']
for u in users:
    for d in disguiser:
        get_data_count('../lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                op_results, 1, u)
        get_data_count('../lobsters_results/concurrent_disguise_stats_{}users_{}--txn.csv'.format(u, d),
                op_results_txn, 1, u)
        if d != 'none':
            get_data_values('../lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                    delete_results, 2, u)
            get_data_values('../lobsters_results/concurrent_disguise_stats_{}users_{}--txn.csv'.format(u, d),
                    delete_results_txn, 2, u)
            get_data_values('../lobsters_results/concurrent_disguise_stats_{}users_{}.csv'.format(u, d),
                    restore_results, 3, u)
            get_data_values('../lobsters_results/concurrent_disguise_stats_{}users_{}--txn.csv'.format(u, d),
                    restore_results_txn, 3, u)
print("low nontxnal")
print(op_results[LOW_LOAD][0])
print(op_results[LOW_LOAD][1])
print(op_results[LOW_LOAD][2])

print("high nontxnal")
print(op_results[HIGH_LOAD][0])
print(op_results[HIGH_LOAD][1])
print(op_results[HIGH_LOAD][2])

print("low txnal")
print(op_results_txn[LOW_LOAD][0])
print(op_results_txn[LOW_LOAD][1])
print(op_results_txn[LOW_LOAD][2])

print("high txnal")
print(op_results_txn[HIGH_LOAD][0])
print(op_results_txn[HIGH_LOAD][1])
print(op_results_txn[HIGH_LOAD][2])

print(
    "high load disguise",
    int(np.percentile(delete_results[HIGH_LOAD][1], 5)),
    int(statistics.median(delete_results[HIGH_LOAD][1])),
    int(np.percentile(delete_results[HIGH_LOAD][1], 95)),
)
print(
    "high load restore",
    int(np.percentile(restore_results[HIGH_LOAD][1], 5)),
    int(statistics.median(restore_results[HIGH_LOAD][1])),
    int(np.percentile(restore_results[HIGH_LOAD][1], 95)),
)
print(
    "high load disguise txn",
    int(np.percentile(delete_results_txn[HIGH_LOAD][1], 5)),
    int(statistics.median(delete_results_txn[HIGH_LOAD][1])),
    int(np.percentile(delete_results_txn[HIGH_LOAD][1], 95)),
)
print(
    "high load restore txn",
    int(np.percentile(restore_results_txn[HIGH_LOAD][1], 5)),
    int(statistics.median(restore_results_txn[HIGH_LOAD][1])),
    int(np.percentile(restore_results_txn[HIGH_LOAD][1], 95)),
)
print(
    "expensive disguise",
    int(np.percentile(delete_results[LOW_LOAD][1], 5)),
    int(statistics.median(delete_results[LOW_LOAD][1])),
    int(np.percentile(delete_results[LOW_LOAD][1], 95)),
)
print(
    "expensive restore",
    int(np.percentile(restore_results[LOW_LOAD][1], 5)),
    int(statistics.median(restore_results[LOW_LOAD][1])),
    int(np.percentile(restore_results[LOW_LOAD][1], 95)),
)
print(
    "exp txn delete",
    int(np.percentile(delete_results_txn[LOW_LOAD][1], 5)),
    int(statistics.median(delete_results_txn[LOW_LOAD][1])),
    int(np.percentile(delete_results_txn[LOW_LOAD][1], 95)),
)
print(
    "exp txn restore",
    int(np.percentile(restore_results_txn[LOW_LOAD][1], 5)),
    int(statistics.median(restore_results_txn[LOW_LOAD][1])),
    int(np.percentile(restore_results_txn[LOW_LOAD][1], 95)),
)

offset = 560

################ none
print("none", op_results[HIGH_LOAD][0])

plt.bar((X-2*barwidth), [
    statistics.median(op_results[LOW_LOAD][0]),
    statistics.median(op_results[HIGH_LOAD][0]),
],
yerr=get_yerr([
    op_results[LOW_LOAD][0],
    op_results[HIGH_LOAD][0],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5), color='g', capsize=3,
        width=barwidth, label="No Disguising", edgecolor='black', linewidth=0.25)
add_labels((X-2*barwidth),
[
    statistics.median(op_results[LOW_LOAD][0]),
    statistics.median(op_results[HIGH_LOAD][0]),
], plt, 'g', offset)


################ cheap w/out txn
plt.bar((X-barwidth), [
    statistics.median(op_results[LOW_LOAD][1]),
    statistics.median(op_results[HIGH_LOAD][1]),
],
yerr=get_yerr([
    op_results[LOW_LOAD][1],
    op_results[HIGH_LOAD][1],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='y', capsize=3, width=barwidth, label="Disguise Random User", edgecolor='black', linewidth=0.25)
add_labels((X-barwidth),
[
    statistics.median(op_results[LOW_LOAD][1]),
    statistics.median(op_results[HIGH_LOAD][1]),
], plt, 'black', offset)

################ cheap w/txn
plt.bar((X), [
    statistics.median(op_results_txn[LOW_LOAD][1]),
    statistics.median(op_results_txn[HIGH_LOAD][1]),
],
yerr=get_yerr([
    op_results_txn[LOW_LOAD][1],
    op_results_txn[HIGH_LOAD][1],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='y', hatch='/////', capsize=3, width=barwidth, label="Disguise Random User (TX)",edgecolor='black', alpha=.99, linewidth=0.25)
add_labels((X),
[
    statistics.median(op_results_txn[LOW_LOAD][1]),
    statistics.median(op_results_txn[HIGH_LOAD][1]),
], plt, 'black', offset)

################ expensive
plt.bar((X+barwidth), [
    statistics.median(op_results[LOW_LOAD][2]),
    statistics.median(op_results[HIGH_LOAD][2]),
],
yerr=get_yerr([
    op_results[LOW_LOAD][2],
    op_results[HIGH_LOAD][2],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='b', capsize=3, width=barwidth, label="Disguise Expensive User", edgecolor='black', linewidth=0.25)
add_labels((X+barwidth),
[
    statistics.median(op_results[LOW_LOAD][2]),
    statistics.median(op_results[HIGH_LOAD][2]),
], plt, 'b', offset)


################ expensive (TX)
plt.bar((X+2*barwidth), [
    statistics.median(op_results_txn[LOW_LOAD][2]),
    statistics.median(op_results_txn[HIGH_LOAD][2]),
],
yerr=get_yerr([
    op_results_txn[LOW_LOAD][2],
    op_results_txn[HIGH_LOAD][2],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='b', hatch='/////', capsize=3, width=barwidth, label="Disguise Expensive User (TX)",alpha=.99, edgecolor='w', linewidth=0.25)
plt.bar((X+2*barwidth), [
    statistics.median(op_results_txn[LOW_LOAD][2]),
    statistics.median(op_results_txn[HIGH_LOAD][2]),
],
yerr=get_yerr([
    op_results_txn[LOW_LOAD][2],
    op_results_txn[HIGH_LOAD][2],

]),
error_kw=dict(capthick=0.5, ecolor='black', lw=0.5),color='none', width=barwidth, edgecolor='black', linewidth=0.25)
add_labels((X+2*barwidth),
[
    statistics.median(op_results_txn[LOW_LOAD][2]),
    statistics.median(op_results_txn[HIGH_LOAD][2]),
], plt, 'b', offset)

plt.ylabel('Time (sec)')
plt.ylim(ymin=0, ymax=10500)
plt.yticks(range(0, 10500, 2000))
#plt.tick_params(
#    axis='x',          # changes apply to the x-axis
#    which='both',      # both major and minor ticks are affected
#    bottom=False,      # ticks along the bottom edge are off
#    top=False,         # ticks along the top edge are off
#    labelbottom=False) # labels along the bottom edge are off
plt.xticks(X, labels=labels)
plt.rcParams.update({'hatch.color': 'w'})
plt.ylabel('Throughput (ops/sec)')
plt.legend(loc='upper left', frameon=False, handlelength=1, borderpad=-0.055, labelspacing=-0.05);
plt.tight_layout(h_pad=0)
plt.savefig('lobsters_concurrent_results.pdf')
