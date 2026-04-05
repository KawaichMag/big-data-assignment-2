import sys

# Input format from reducer1.py:
# word<TAB>doc_id<TAB>tf

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    word, doc_id, tf = line.split("\t")
    # Emit key=word, value=doc_id:tf
    print(f"{word}\t{doc_id}:{tf}")
