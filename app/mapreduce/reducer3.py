import sys

current_word = None
docs = []

for line in sys.stdin:
    line = line.strip()
    if not line or "\t" not in line:
        continue

    word, doc_info = line.split("\t", 1)

    # Split from the right to handle filenames containing ":"
    try:
        filename, tf, df = doc_info.rsplit(":", 2)
    except ValueError:
        # Skip malformed lines
        continue

    if current_word is None:
        current_word = word

    if word != current_word:
        # Output previous word
        print(f"{current_word}\t{'|'.join(docs)}")
        docs = []

    docs.append(f"{filename}\t{tf}\t{df}")
    current_word = word

# Print last word
if current_word and docs:
    print(f"{current_word}\t{'|'.join(docs)}")
