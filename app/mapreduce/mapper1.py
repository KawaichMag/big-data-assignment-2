import sys
import os
import re

# Get the current input file name from Hadoop environment variable
doc_id = os.environ.get("map_input_file", "unknown_file")
# Keep only the filename, remove any path prefix
doc_id = os.path.basename(doc_id)

# Read each line from stdin
for line in sys.stdin:
    # Split line into words
    words = re.findall(r"\w+", line.lower())  # lowercase, remove punctuation
    for word in words:
        print(f"{word}\t{doc_id}\t1")
