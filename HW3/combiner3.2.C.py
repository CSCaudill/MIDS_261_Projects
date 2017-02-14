#!/usr/bin/env python
# START STUDENT CODE HW32CCOMBINER
import sys
from collections import defaultdict

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.2.C Combiner Counters,Calls,1\n")

issueDict = defaultdict(int)

for line in sys.stdin:
    word,count = line.split("\t")
    if word in issueDict:
        issueDict[word] += int(count)
    else:
        issueDict[word] = int(count)        

for key in sorted(issueDict):
    print '%s\t%s' % (key, issueDict[key])

# END STUDENT CODE HW32CCOMBINER