import sys

# Input: <word>\t<doc1>:<tf>\t<doc2>:<tf>...

for line in sys.stdin:
    line = line.strip()
    if not line or "\t" not in line:
        continue
    parts = line.split("\t")
    word = parts[0]
    doc_list = parts[1:]
    df = len(doc_list)
    for doc_tf in doc_list:
        if ":" not in doc_tf:
            continue
        doc_id, tf = doc_tf.split(":", 1)
        print(f"{word}\t{doc_id}:{tf}:{df}")
