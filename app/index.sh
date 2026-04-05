#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input path is :"
echo $1

echo "Creating Indeces.."
bash create_index.sh $1

echo "Storing Indces and other info in Cassandra.."
bash store_index.sh /indexer

hdfs dfs -ls /
