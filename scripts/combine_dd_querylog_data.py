# NOTE: don't add rollup to the features
import json


def create_datadog_timeseries(datadog_data):
    res = {}
    values = datadog_data["data"]["attributes"]["values"][0]
    for i, t in enumerate(datadog_data["data"]["attributes"]["times"]):
        # divide by 1000 becase DD in milliseconds
        res[t / 1000] = values[i]
    return res


def combine_dd_querylog_data(
    querylog_csv_file, datadog_json_file, offset_secs, outfile
):
    dd_data = json.load(open(datadog_json_file))
    qlog_data = open(querylog_csv_file)
    dd_times = create_datadog_timeseries(dd_data)

    with open(outfile, "w") as out:
        out.write(
            ",".join(
                [
                    "duration",
                    "num_final",
                    "num_invalid_requests",
                    "timeouts",
                    "sum_max_threads",
                    "sum_num_days",
                    "sum_num_cols",
                    "sum_bytes_scanned",
                    "load",
                ]
            )
        )
        out.write("\n")
        for i, row_text in enumerate(qlog_data.readlines()):
            row = row_text.strip().split(",")
            time = int(row[0])
            load_val = dd_times.get(time + offset_secs, None)
            if not load_val:
                print(f"no load val for time {time}")
                continue
            try:
                out.write(",".join([*row[1:], str(load_val)]))
                out.write("\n")
            except IndexError:
                import pdb

                pdb.set_trace()
                print(i)
