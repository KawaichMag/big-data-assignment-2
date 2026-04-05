#!/bin/bash

echo "Create index using MapReduce pipelines"

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input $1 \
  -output /tmp/out \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file ./mapreduce/mapper1.py \
  -file ./mapreduce/reducer1.py

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input /tmp/out \
  -output /indexer/index \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -file ./mapreduce/mapper2.py \
  -file ./mapreduce/reducer2.py

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input /indexer/index \
  -output /indexer/bm25_stats \
  -mapper "python3 mapper3.py" \
  -reducer "python3 reducer3.py" \
  -file ./mapreduce/mapper3.py \
  -file ./mapreduce/reducer3.py

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
  -input /tmp/out \
  -output /indexer/doc_lengths \
  -mapper "python3 mapper4.py" \
  -reducer "python3 reducer4.py" \
  -file ./mapreduce/mapper4.py \
  -file ./mapreduce/reducer4.py
