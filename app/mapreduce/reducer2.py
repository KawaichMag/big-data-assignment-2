import sys

current_word = None
doc_list = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    word, doc_tf = line.split("\t")

    if current_word != word and current_word is not None:
        # Output inverted index for the previous word
        print(current_word + "\t" + "\t".join(doc_list))
        doc_list = []

    current_word = word
    doc_list.append(doc_tf)

# Output last word
if current_word:
    print(current_word + "\t" + "\t".join(doc_list))
