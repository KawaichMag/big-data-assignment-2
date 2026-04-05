import sys

current_word_doc = None
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    word, doc_id, val = line.split("\t")
    key = f"{word}\t{doc_id}"
    val = int(val)

    if current_word_doc != key and current_word_doc is not None:
        print(f"{current_word_doc}\t{count}")
        count = 0

    current_word_doc = key
    count += val

# output last key
if current_word_doc:
    print(f"{current_word_doc}\t{count}")
