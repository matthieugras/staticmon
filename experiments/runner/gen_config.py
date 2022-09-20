import json
from itertools import product, permutations


def make_op_benchmark_tp(nts, tpts, bench):
    return {
        "numtpperts": tpts,
        "numts": nts,
        "config": bench
    }


def make_op_benchmark(nts, bench):
    return make_op_benchmark_tp(nts, 1, bench)


def make_temporal_bench(nts, tpts, lbound, ubound, bench):
    return make_op_benchmark_tp(nts, tpts, {
        "temporaloperator": {
            "lbound": lbound,
            "ubound": ubound,
            "suboperator": bench
        }
    })


andsizes = list(permutations([150, 300], 2)) + \
    [(0, i) for i in [1000]] + \
    [(i, 0) for i in [1000]]
nvars = [1, 5, 10]
commvars = [1, 2, 4]
andbench = []
for ((ls, rs), n1, n2) in product(andsizes, nvars, nvars):
    newopt = [{"distinct": []}]
    if n1 <= n2:
        newopt.append({"subset": True})
    if n2 < n1:
        newopt.append({"subset": False})
    for nc in commvars:
        if nc > n1 or nc > n2:
            break
        else:
            newopt.append({"fixedcommon": nc})
    for opt in newopt:
        newbench = {
            "andoperator": {
                "lsize": ls,
                "rsize": rs,
                "n1": n1,
                "n2": n2,
                "opts": opt
            }
        }
        andbench.append(make_op_benchmark(100, newbench))

antijoinsizes = list(permutations([300, 1000], 2)) + \
    [(0, i) for i in [1000]] + \
    [(i, 0) for i in [1000]]
matchprob = [0.0, 0.5, 1.0]
leftvars = [1, 4, 10]
rightvars = [1, 2, 4]
antijoinbench = []
for ((ls, rs), p, lv) in product(antijoinsizes,
                                 matchprob, leftvars):
    for rv in rightvars:
        if rv > lv:
            break
        else:
            seqs = [{"subsetsequence": [lv, rv]}, {"randomsubset": [lv, rv]}]
            for s in seqs:
                newbench = {
                    "antijoinoperator": {
                        "lsize": ls,
                        "rsize": rs,
                        "matchprobability": p,
                        "vars": s
                    }
                }
                antijoinbench.append(make_op_benchmark(100, newbench))


orsizes = [0, 1, 300, 1000]
nvars = [1, 5, 10]
oropts = ["samelayout", "shuffled"]
orbench = [
    make_op_benchmark(100, {
        "oroperator": {
            "lsize": ls,
            "rsize": rs,
            "nvars": n,
            "opts": op
        }
    })
    for (ls, rs, n, op) in product(orsizes, orsizes, nvars, oropts)
]

existsbench = []
for s in [100, 200, 1000]:
    for pn in [5, 10, 15]:
        for n in [1, 2, 4, 8]:
            if n > pn:
                break
            else:
                newbench = {
                    "existsoperator": {
                        "n": n,
                        "predn": pn,
                        "size": s
                    }
                }
                existsbench.append(make_op_benchmark(100, newbench))


def cstb(i):
    return {"cstbound": i}


def infb():
    return {"infbound": []}


bounds = [(cstb(i), cstb(10)) for i in [0, 1, 2, 4]]  \
    + [(cstb(0), cstb(i)) for i in [0, 2, 4]] \
    + [(cstb(i), infb()) for i in [0, 1, 2, 4]]
ops = ["prevoperator", "nextoperator"]
sizes = [0, 40, 300]
tpts = [1]
prevnxtbench = [
    make_temporal_bench(100, ntp, lb, ub, {
        op: {
            "size": s
        }
    }) for
    (ntp, (lb, ub), op, s) in product(tpts, bounds, ops, sizes)
]

tpts = [1]
evrates = [100, 1000]
bounds = [(cstb(i), cstb(10)) for i in [0, 1, 2, 4]]  \
    + [(cstb(0), cstb(i)) for i in [0, 2, 4]] \
    + [(cstb(i), infb()) for i in [0, 1, 2, 4]]
oncebench = [
    make_temporal_bench(100, ntp, lb, ub, {
        "onceoperator": {
            "eventrate": evr,
            "nvars": n
        }
    }) for
    (ntp, (lb, ub), evr, n) in product(tpts, bounds, evrates, nvars)
]

tpts = [1]
evrates = [100, 1000]
bounds = [(cstb(i), cstb(10)) for i in [0, 1, 2, 4]]  \
    + [(cstb(0), cstb(i)) for i in [0, 2, 4]]
eventuallybench = [
    make_temporal_bench(100, ntp, lb, ub, {
        "eventuallyoperator": {
            "eventrate": evr,
            "nvars": n
        }
    }) for
    (ntp, (lb, ub), evr, n) in product(tpts, bounds, evrates, nvars)
]

tpts = [1]
evrates = [350]
negate = [False, True]
bounds = [(cstb(i), cstb(i)) for i in [0, 8, 16]] + \
         [(cstb(i), cstb(j)) for (i, j) in [(2, 4), (2, 8), (2, 16)]]
subsetopts = [{"randomsubset": [i, 4]} for i in [1, 4]]
removeprob = [0.01, 0.5, 0.80]
removeprob_neg = [0.99, 0.5, 0.2]
sincebench = []
for (ntp, (lb, ub), evr, neg, s) in product(tpts, bounds,
                                            evrates, negate, subsetopts):
    probs = removeprob_neg if neg else removeprob
    for p in probs:
        newbench = make_temporal_bench(200, ntp, lb, ub, {
            "sinceoperator": {
                "eventrate": evr,
                "vars": s,
                "negate": neg,
                "removeprobability": p
            }
        })
        sincebench.append(newbench)

config = andbench + orbench + existsbench + \
    sincebench + oncebench + eventuallybench
print(len(config))
with open("config.yaml", 'w', encoding='utf8') as conf_out:
    json.dump(config, conf_out, ensure_ascii=True)
    conf_out.write("\n")
