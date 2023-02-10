# NOTE: don't add rollup to the features
import json

QLOG_DATA = open("querylog_training_data_5_second.csv")
DD_DATA = json.load(open("dd_results_edited.json"))

with open("training_corpus.csv", "w") as out:
    for i, row_text in enumerate(QLOG_DATA.readlines()):
        row = row_text.strip().split(",")
        try:
            out.write(
                ",".join([*row[1:], str(DD_DATA["data"]["attributes"]["values"][0][i])])
            )
            out.write("\n")
        except IndexError:
            import pdb

            pdb.set_trace()
            print(i)
