#!/usr/bin/env python
# START STUDENT CODE HW32CFREQMAPPER

import sys

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.3 Mapper Counters,Calls,1\n")

for line in sys.stdin:
    prods = line.split()
    for item in prods:
        print '%s\t%s\t%s' % (item, 1,10)

# END STUDENT CODE HW32CFREQMAPPER