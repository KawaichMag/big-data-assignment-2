import sys

current_doc = None
total_terms = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue
    doc_id, tf = parts
    tf = int(tf)

    if current_doc != doc_id and current_doc is not None:
        print(f"{current_doc}\t{total_terms}")
        total_terms = 0

    current_doc = doc_id
    total_terms += tf

# Output last doc
if current_doc:
    print(f"{current_doc}\t{total_terms}")
