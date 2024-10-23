# Riverse_meta_data_reader.py
import re

def find_line_with_metadata(lines, metadata_line):
    for line in lines:
        if re.search(re.escape(metadata_line), line):
            return False
    return True
