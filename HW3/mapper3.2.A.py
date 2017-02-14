#!/usr/bin/env python
# START STUDENT CODE HW32AMAPPER

import sys
from collections import defaultdict

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.2.A Mapper Counters,Calls,1\n")

wordCounts = defaultdict(int)

for line in sys.stdin:
    words = line.split()
    for word in words:
            print '%s\t%s' % (word, 1)
#         if word in wordCounts:
#             wordCounts[word] += 1
#         else:
#             wordCounts[word] += 1

# for key in sorted(wordCounts):
#     print '%s\t%s' % (key, wordCounts[key])

# END STUDENT CODE HW32AMAPPER