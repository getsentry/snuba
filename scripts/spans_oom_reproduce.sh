#!/bin/bash
set -euo pipefail

# Add trap to handle Ctrl+C. This will kill the consumer being monitored and the consumer causing rebalances.
cleanup() {
    echo "Cleaning up..."
    if [ -n "${CONSUMER_PID:-}" ]; then
        kill -9 $CONSUMER_PID || true
    fi

    if [ -n "${CONSUMER_PID2:-}" ]; then
        kill -9 $CONSUMER_PID2 || true
    fi
    exit 0
}
trap cleanup SIGINT SIGTERM

# Check if -g or --generate flag is provided. If so, generate spans.
if [[ "$*" == *"-g"* ]] || [[ "$*" == *"--generate"* ]]; then
    count=0
    while [ "$count" -lt 1000 ]; do
        python send_spans.py -o kafka
        count=$((count + 1))
    done
fi

# Docker exec into kafka container and reset the consumer group offset
docker exec -it sentry_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group spans_group --reset-offsets --to-earliest --topic snuba-spans --execute

# Run the standard spans consumer in the background
snuba rust-consumer --storage=spans --consumer-group=spans_group --use-rust-processor --auto-offset-reset=earliest --no-strict-offset-reset --log-level=info > /dev/null 2>&1 &

# Get the PID of the consumer. We'll use this PID to monitor memory usage.
CONSUMER_PID=$!

# Dump the memory usage of the consumer and keep causing rebalances by starting another instance of the consumer.
while true; do
    echo "Memory usage of consumer $CONSUMER_PID: $(ps -o rss= -p $CONSUMER_PID | awk '{printf "%.2fMB", $1/1024}')"
    sleep 1

    snuba rust-consumer --storage=spans --consumer-group=spans_group --use-rust-processor --auto-offset-reset=earliest --no-strict-offset-reset --log-level=info > /dev/null 2>&1 &
    CONSUMER_PID2=$!
    sleep 5
    kill -TERM $CONSUMER_PID2

    # Dump the consumer offset of the consumer group
    docker exec -it sentry_kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group spans_group --describe
done
