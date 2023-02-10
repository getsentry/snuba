import json
from collections import defaultdict

import numpy as np

SOURCE_FILE = "querylog_training_data_second.json"
SOURCE_DATA = json.load(open(SOURCE_FILE))
COLS = SOURCE_DATA["column_names"]


RESULT_DICT = defaultdict(lambda: np.zeros(len(COLS) - 1))

for row in SOURCE_DATA["rows"]:
    time = row[COLS.index("time")]
    rollup_time = time - (time % 5)
    RESULT_DICT[rollup_time] += np.array(row[1:])


with open("querylog_training_data_5_second.json", "w") as out:
    for rollup_time in sorted(RESULT_DICT.keys()):
        cols = RESULT_DICT[rollup_time]
        out.write(",".join([str(rollup_time), *[str(c) for c in cols]]))
        out.write("\n")
