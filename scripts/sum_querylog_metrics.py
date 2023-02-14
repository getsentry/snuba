import json
from collections import defaultdict

import numpy as np


def sum_querylog_metrics(source_file: str, interval_secs: int, outfile: str) -> None:
    source_data = json.load(open(source_file))
    COLS = source_data["column_names"]

    RESULT_DICT = defaultdict(lambda: np.zeros(len(COLS) - 1))

    for row in source_data["rows"]:
        time = row[COLS.index("time")]
        rollup_time = time - (time % 5)
        RESULT_DICT[rollup_time] += np.array(row[1:])

    with open(outfile, "w") as out:
        for rollup_time in sorted(RESULT_DICT.keys()):
            cols = RESULT_DICT[rollup_time]
            out.write(",".join([str(rollup_time), *[str(c) for c in cols]]))
            out.write("\n")
