#!/usr/bin/env python
# START STUDENT CODE HW32BMAPPER

import sys
import csv
from collections import defaultdict
import re

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.2.B Mapper Counters,Calls,1\n")

issueList = defaultdict(int)

ccd = csv.reader(sys.stdin)
for row in ccd:
    issue = row[3]
    if len(issue) > 0 and issue != 'Issue':
        words = re.findall(r'[a-z]+', issue.lower())
        for word in words:
            if word in issueList:
                issueList[word] +=1
            else:
                issueList[word] = 1
    
for key in sorted(issueList):
    print '%s\t%s' % (key, issueList[key])



# END STUDENT CODE HW32BMAPPER