import pandas as pd
import re

data = pd.read_csv("out.csv")


def id_f(x):
    return x


configs = [
    {
        "name": "and_common",
        "regex": r"(\d*)_(\d*)_and_(\d*)_(\d*)_common_(\d*)_(\d*)_(\d*)",
        "cols": ["numtp", "numts", "lsize", "rsize", "nc", "n1", "n2"],
        "conv_fs": [int] * 7

    },
    {
        "name": "or",
        "regex": r"(\d*)_(\d*)_or_(\d*)_(\d*)_(same|shuffled)_(\d*)",
        "cols": ["numtp", "numts", "lsize", "rsize", "order", "nvars"],
        "conv_fs": ([int] * 4) + [id_f, int]
    },
    {
        "name": "exists",
        "regex": r"(\d*)_(\d*)_exists_(\d*)_(\d*)_(\d*)",
        "cols": ["numtp", "numts", "exvars", "pvars", "size"],
        "conv_fs": [int] * 5
    },
    {
        "name": "once",
        "regex": r"(\d*)_(\d*)_once_(\d*)_(\d*|inf)_(\d*)_(\d*)",
        "cols": ["numtp", "numts", "lbound", "ubound", "evr", "nvars"],
        "conv_fs": ([int] * 3) + [id_f] + ([int] * 2)
    }
]

for conf in configs:
    data_subset = data[data.benchmark.str.match(conf["regex"])]
    new_col_dict = {
        col: [] for col in conf["cols"]
    }

    for e in data_subset.benchmark:
        m = re.match(conf["regex"], e)
        for i, col in enumerate(conf["cols"]):
            new_col_dict[col].append(conf["conv_fs"][i](m[i + 1]))

    new_col_dict = pd.DataFrame(new_col_dict)
    data_subset.reset_index(drop=True, inplace=True)
    new_col_dict.reset_index(drop=True, inplace=True)
    out_dict = pd.concat(
        [new_col_dict, data_subset], axis=1)
    out_dict.drop(columns="benchmark", inplace=True)
    out_dict.to_csv(f"{conf['name']}_data.csv", index=False)
