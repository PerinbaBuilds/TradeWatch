#!/usr/bin/env bash
# Run the batch anomaly detector on a real Hadoop cluster via Hadoop Streaming,
# reading and writing HDFS.
#
#   # load data into HDFS first
#   hdfs dfs -mkdir -p /tradewatch/trades
#   hdfs dfs -put data/trades.jsonl /tradewatch/trades/
#   hadoop/run_streaming.sh /tradewatch/trades /tradewatch/anomalies
set -euo pipefail

INPUT="${1:-/tradewatch/trades}"
OUTPUT="${2:-/tradewatch/anomalies}"
HERE="$(cd "$(dirname "$0")" && pwd)"
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"

STREAMING_JAR="$(find "$HADOOP_HOME" -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)"
if [[ -z "$STREAMING_JAR" ]]; then
  echo "hadoop-streaming jar not found under HADOOP_HOME=$HADOOP_HOME" >&2
  exit 1
fi

# Overwrite any previous output (MapReduce refuses to write into an existing dir).
hdfs dfs -rm -r -f "$OUTPUT" || true

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name=tradewatch-anomaly-mapreduce \
  -D mapreduce.job.reduces=4 \
  -D stream.num.map.output.key.fields=1 \
  -files "$HERE/mapper.py,$HERE/reducer.py" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input "$INPUT" \
  -output "$OUTPUT"

echo "anomalies written to hdfs://$OUTPUT"
hdfs dfs -cat "$OUTPUT/part-*" | head -20
