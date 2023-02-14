from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from combine_dd_querylog_data import combine_dd_querylog_data
from get_normalized_load import get_normalized_load
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sum_querylog_metrics import sum_querylog_metrics

QUERYLOG_QUERY = """
SELECT
    toUnixTimestamp(timestamp) AS time,
    sum(duration_ms) AS duration,
    sum(arrayCount(clickhouse_queries.final)) AS num_final,
    sum(arrayCount(x -> x='invalid_request', clickhouse_queries.status)) as num_invalid_requests,
    sum(arrayCount(x -> x='timeout', clickhouse_queries.status)) as timeouts,
    sum(arraySum(clickhouse_queries.max_threads)) as sum_max_threads,
    sum(arraySum(clickhouse_queries.num_days)) as sum_num_days,
    sum(length(clickhouse_queries.all_columns)) as sum_num_cols,
    --sum(arraySum(q -> length(q), clickhouse_queries.sql)) as num_query_chars,
    sum(arraySum(clickhouse_queries.bytes_scanned)) as sum_bytes_scanned
FROM querylog_local
WHERE
status != 'rate_limited'
AND time >= 1675989900
AND time <= 1675996200
GROUP BY
    time
ORDER BY
    time ASC
"""


# START_DATETIME = datetime(2023, 1, 31, 2, 7, tzinfo=timezone.utc) + timedelta(
#     seconds=30
# )
# END_DATETIME = datetime(2023, 1, 31, 2, 16, tzinfo=timezone.utc) + timedelta(seconds=30)

START_DATETIME = datetime(2023, 2, 10, 0, 45, tzinfo=timezone.utc) + timedelta(
    seconds=30
)
END_DATETIME = datetime(2023, 2, 10, 2, 30, tzinfo=timezone.utc) + timedelta(seconds=30)
QUERYLOG_JSON_FILE = f"querylog_data_{START_DATETIME.strftime('%Y-%m-%d')}.json"
DATADOG_JSON_FILE = f"datadog_data_{START_DATETIME.strftime('%Y-%m-%d')}.json"
TRAINING_CORPUS_FILE = f"training_corpus_{START_DATETIME.strftime('%Y-%m-%d')}.csv"


def train_model(corpus_file_name):
    # Load the data
    data = pd.read_csv(corpus_file_name)

    # Split the data into features (X) and target (y)
    X = data.drop(["load", "sum_max_threads"], axis=1)
    y = data["load"]

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # Create a linear regression object
    reg = LinearRegression()

    # Train the model on the training data
    reg.fit(X_train, y_train)

    # Evaluate the model on the test data
    score = reg.score(X_test, y_test)
    print("Test R^2 Score:", score)

    print("=" * 10, "Coefficiencts", "=" * 10)
    for col, coef in zip(X.head(), reg.coef_):
        print(f"{col}: {coef}")


def main():
    # get_normalized_load(START_DATETIME, END_DATETIME, DATADOG_JSON_FILE)
    print("retrieved load from datadog")
    querylog_rollup_data_file = QUERYLOG_JSON_FILE.replace("json", "csv")
    sum_querylog_metrics(QUERYLOG_JSON_FILE, 5, querylog_rollup_data_file)
    print("rolled up querylog data")
    combine_dd_querylog_data(
        querylog_rollup_data_file, DATADOG_JSON_FILE, 30, TRAINING_CORPUS_FILE
    )
    print("created corpus ", TRAINING_CORPUS_FILE)

    train_model(TRAINING_CORPUS_FILE)


if __name__ == "__main__":
    main()
