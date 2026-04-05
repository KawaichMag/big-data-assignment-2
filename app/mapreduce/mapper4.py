import sys

# Input format: word<TAB>doc_id<TAB>tf
# Output format: doc_id<TAB>tf

for line in sys.stdin:
    line = line.strip()
    if not line or "\t" not in line:
        continue
    parts = line.split("\t")
    if len(parts) != 3:
        continue
    word, doc_id, tf = parts
    print(f"{doc_id}\t{tf}")
