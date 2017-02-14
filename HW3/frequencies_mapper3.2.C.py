#!/usr/bin/env python
# START STUDENT CODE HW32CFREQMAPPER

import sys, csv, re

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.2.C Mapper Counters,Calls,1\n")

ccd = csv.reader(sys.stdin)
for row in ccd:
    issue = row[3]
    if len(issue) > 0 and issue != 'Issue':
        words = re.findall(r'[a-z]+', issue.lower())
        for word in words:
            print '%s\t%s\t%s' % (word, 1, 10)

# END STUDENT CODE HW32CFREQMAPPER