#!/usr/bin/env python3
import sys
import argparse
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "bm25_index"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--type",
    choices=["index", "bm25", "doc_lengths"],
    required=True,
    help="Type of data to load",
)
args = parser.parse_args()

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

# Create keyspace if not exists
session.execute(f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
""")
session.set_keyspace(KEYSPACE)

# Create tables
if args.type == "index":
    session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        word text,
        doc_id text,
        tf int,
        PRIMARY KEY (word, doc_id)
    )
    """)
elif args.type == "bm25":
    session.execute("""
    CREATE TABLE IF NOT EXISTS bm25_stats (
        word text,
        doc_id text,
        tf int,
        df int,
        PRIMARY KEY (word, doc_id)
    )
    """)
elif args.type == "doc_lengths":
    session.execute("""
    CREATE TABLE IF NOT EXISTS doc_lengths (
        doc_id text PRIMARY KEY,
        length int
    )
    """)

# Read stdin and insert
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    batch = BatchStatement()

    if args.type == "index":
        # Format: <word>\t<doc_id>:<tf>\t<doc_id>:<tf>...
        parts = line.split("\t")
        word = parts[0]
        for doc_tf in parts[1:]:
            if ":" not in doc_tf:
                continue
            doc_id, tf = doc_tf.split(":", 1)
            batch.add(
                SimpleStatement(
                    "INSERT INTO inverted_index (word, doc_id, tf) VALUES (%s, %s, %s)"
                ),
                (word, doc_id, int(tf)),
            )

    elif args.type == "bm25":
        # Format: <word>\t<doc_id>\t<tf>\t<df>|<doc_id>\t<tf>\t<df>|...
        try:
            word, docs_str = line.split("\t", 1)
        except ValueError:
            continue
        docs = docs_str.split("|")
        for doc in docs:
            try:
                doc_id, tf, df = doc.split("\t")
                batch.add(
                    SimpleStatement(
                        "INSERT INTO bm25_stats (word, doc_id, tf, df) VALUES (%s, %s, %s, %s)"
                    ),
                    (word, doc_id, int(tf), int(df)),
                )
            except ValueError:
                continue

    elif args.type == "doc_lengths":
        # Format: <doc_id>\t<doc_length>
        try:
            doc_id, length = line.split("\t")
            batch.add(
                SimpleStatement(
                    "INSERT INTO doc_lengths (doc_id, length) VALUES (%s, %s)"
                ),
                (doc_id, int(length)),
            )
        except ValueError:
            continue

    # Execute batch per line
    session.execute(batch)

print(f"{args.type} data loaded successfully!")
