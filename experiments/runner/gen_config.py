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


# nc | n1 | n2 | lsize | rsize
p1 = list(product(["fixedcommon"], [1, 2, 4, 8], [10], [10], [300], [300]))
p2 = list(product(["fixedcommon"], [1], [2, 4, 8, 16], [2, 4, 8, 16],
          [300], [300]))
p3 = list(product(["fixedcommon"], [1], [3], [3], [0, 150, 300, 600],
                  [0, 150, 300, 600]))

# nl | nr | sizel | sizer
p4 = list(product(["distinct"], [1, 2, 4, 8], [1, 2, 4, 8], [300], [300]))
p5 = list(product(["distinct"], [3], [3], [
          0, 150, 300, 600], [0, 150, 300, 600]))

andbench = []
for b in (p1 + p2 + p3 + p4 + p5):
    if b[0] == "fixedcommon":
        (nc, n1, n2, ls, rs) = b[1:]
        opt = {"fixedcommon": nc}
    else:
        (n1, n2, ls, rs) = b[1:]
        opt = {"distinct": []}
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

# lsize | rsize | nvars | mode
p1 = list(product([0, 1, 300, 1000], [0, 1, 300, 1000],
          [10], ["shuffled", "samelayout"]))
p2 = list(product([1000], [1000], [1, 5, 15], ["samelayout", "shuffled"]))
orbench = [
    make_op_benchmark(100, {
        "oroperator": {
            "lsize": ls,
            "rsize": rs,
            "nvars": n,
            "opts": op
        }
    })
    for (ls, rs, n, op) in (p1 + p2)
]

existsbench = [
    make_op_benchmark(100, {
        "existsoperator": {
            "n": 1,
            "predn": 5,
            "size": s
        }
    })
    for s in [100, 200, 1000]
]


def cstb(i):
    return {"cstbound": i}


def infb():
    return {"infbound": []}


ops = ["prevoperator", "nextoperator"]
sizes = [(10000, 1), (5000, 2), (2500, 4),
         (1250, 8), (500, 16), (250, 32), (125, 64)]
prevnxtbench = [
    make_temporal_bench(logs, 1, cstb(0), infb(), {
        op: {
            "size": s
        }
    }) for
    (op, (logs, s)) in product(ops, sizes)
]

p1 = list(product([100], [200], [1], [cstb(0)], [cstb(i)
          for i in [0, 2, 8, 16, 32]]))
p2 = [(evr, s, 1, cstb(0), cstb(10))
      for (evr, s) in
      [(100, 200), (50, 400), (25, 800), (12, 1600), (6, 3200)]]
p3 = list(product([50], [200, 400, 800, 1600, 3200], [1], [cstb(0)], [infb()]))
p4 = [(100, 200, 1, cstb(b), cstb(b)) for b in [1, 4, 8, 16, 32]]
p5 = [(100, 200, n, cstb(0), cstb(10)) for n in [2, 4, 8, 16]]

oncebench = [
    make_temporal_bench(s, 1, lb, ub, {
        "onceoperator": {
            "eventrate": evr,
            "nvars": n
        }
    }) for
    (evr, s, n, lb, ub) in (p1 + p2 + p3 + p4 + p5)
]

eventuallybench = [
    make_temporal_bench(s, 1, lb, ub, {
        "eventuallyoperator": {
            "eventrate": evr,
            "nvars": n
        }
    }) for
    (evr, s, n, lb, ub) in (p1 + p2 + p4 + p5)
]

# evr | length | subsetopt | lb | ub | rmprob | neg
p1_2 = [(100, 200, {"randomsubset": [1, 3]}, cstb(0),
         cstb(i), p, neg) for i in [0, 2, 8, 16, 32]
        for (p, neg) in [(0.001, False), (0.999, True)]]
p3_4 = [(evr, s, {"randomsubset": [1, 3]}, cstb(0), cstb(10), p, neg)
        for (evr, s) in
        [(100, 200), (50, 400), (25, 800), (12, 1600), (6, 3200)]
        for (p, neg) in [(0.001, False), (0.999, True)]]
p5_6 = [(100, 200, {"randomsubset": [1, 3]}, cstb(b), cstb(b), p, neg)
        for b in [1, 4, 8, 16, 32]
        for (p, neg) in [(0.001, False), (0.999, True)]]
p7_8 = [(100, 200, {"randomsubset": [i, 10]}, cstb(0), cstb(10), p, neg)
        for i in [1, 2, 4, 8]
        for (p, neg) in [(0.001, False), (0.999, True)]]
p9_10 = [(100, 200, {"randomsubset": [1, 3]}, cstb(0), cstb(10), p, neg)
         for (ps, neg) in [([0.1, 0.15, 0.3], False), ([0.9, 0.85, 0.7], True)]
         for p in ps]
sincebench = [
    make_temporal_bench(s, 1, lb, ub, {
        "sinceoperator": {
            "eventrate": evr,
            "vars": opt,
            "negate": neg,
            "removeprobability": p
        }
    })
    for (evr, s, opt, lb, ub, p, neg) in (p1_2 + p3_4 + p5_6 + p7_8 + p9_10)
]

onceandbench = [
    make_op_benchmark(50000, {"onceandeqoperator": {
        "eventrate": 1
    }})
]

# config = andbench + orbench + existsbench + \
#     sincebench + oncebench + eventuallybench + prevnxtbench

config = onceandbench

print(len(config))
with open("config.yaml", 'w', encoding='utf8') as conf_out:
    json.dump(config, conf_out, ensure_ascii=True)
    conf_out.write("\n")
