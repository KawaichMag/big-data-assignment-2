#!/bin/bash
echo "store the index and others to Cassandra/ScyllaDB tables"

source .venv/bin/activate

# Usage: ./store_index.sh [HDFS_BASE_PATH]
HDFS_PATH=${1:-/indexer}

echo "Starting Cassandra ingestion..."

# Index files
echo "Storing inverted index..."
hdfs dfs -cat $HDFS_PATH/index/* | python3 /app/store_index.py --type index

# BM25 stats
echo "Storing BM25 statistics..."
hdfs dfs -cat $HDFS_PATH/bm25_stats/* | python3 /app/store_index.py --type bm25

# Document lengths
echo "Storing document lengths..."
hdfs dfs -cat $HDFS_PATH/doc_lengths/* | python3 /app/store_index.py --type doc_lengths

echo "All data loaded into Cassandra!"