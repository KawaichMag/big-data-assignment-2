#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from cassandra.cluster import Cluster
import math
import sys

query = sys.argv[1:]
query = [term.lower() for term in query]

spark = (
    SparkSession.builder.appName("BM25Query")
    .config("spark.cassandra.connection.host", "cassandra-server")
    .getOrCreate()
)
sc = spark.sparkContext

k1 = 1.5
b = 0.75

cluster = Cluster(["cassandra-server"])
session = cluster.connect("bm25_index")

# Format of inverted_index: word, doc_id, tf
# Format of bm25_stats: word, doc_id, tf, df
# Format of doc_lengths: doc_id, length

# Load data into Spark RDDs
inverted_index_rows = session.execute("SELECT word, doc_id, tf FROM inverted_index")
bm25_stats_rows = session.execute("SELECT word, doc_id, tf, df FROM bm25_stats")
doc_lengths_rows = session.execute("SELECT doc_id, length FROM doc_lengths")

# Convert to RDDs
inverted_index_rdd = sc.parallelize(list(inverted_index_rows)).map(
    lambda r: (r.word, (r.doc_id, r.tf))
)

bm25_stats_rdd = sc.parallelize(list(bm25_stats_rows)).map(
    lambda r: (r.word, (r.doc_id, r.tf, r.df))
)

doc_lengths_rdd = sc.parallelize(list(doc_lengths_rows)).map(
    lambda r: (r.doc_id, r.length)
)

# Broadcast average document length
avg_doc_length = doc_lengths_rdd.map(lambda x: x[1]).mean()
avg_doc_length_bc = sc.broadcast(avg_doc_length)

# Total number of documents
N = doc_lengths_rdd.count()
N_bc = sc.broadcast(N)

# Create doc_lengths dictionary
doc_lengths_dict = dict(doc_lengths_rdd.collect())
doc_lengths_bc = sc.broadcast(doc_lengths_dict)


# For each query term, get (doc_id, tf, df)
def bm25_score_for_term(term):
    # Filter BM25 stats for this term
    docs = bm25_stats_rdd.filter(lambda x: x[0] == term).map(lambda x: x[1]).collect()
    scores = []
    for doc_id, tf, df in docs:
        dl = doc_lengths_bc.value.get(doc_id, avg_doc_length_bc.value)
        idf = math.log((N_bc.value - df + 0.5) / (df + 0.5) + 1)
        score = idf * (
            (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avg_doc_length_bc.value))
        )
        scores.append((doc_id, score))
    return scores


# Accumulate BM25 scores across all query terms
bm25_scores = {}
for term in query:
    term_scores = bm25_score_for_term(term)
    for doc_id, score in term_scores:
        bm25_scores[doc_id] = bm25_scores.get(doc_id, 0) + score

top_docs = sorted(bm25_scores.items(), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 relevant documents:")
for rank, (doc_id, score) in enumerate(top_docs, start=1):
    print(f"{rank}. {doc_id}  (BM25: {score:.4f})")
