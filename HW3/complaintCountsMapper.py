#!/usr/bin/env python

# # START STUDENT CODE HW31MAPPER

import sys
from collections import defaultdict

# Set up counters to monitor/understand the number of times a mapper task is run

prods = defaultdict(int)

for line in sys.stdin:
    if 'debt collection' in line.lower():
        prods['debt collection'] += 1
        sys.stderr.write("reporter:counter:HW3.1 Mapper Debt Collection Counters,Calls,1\n")
        
    elif 'mortgage' in line.lower():
        prods['mortgage'] += 1
        sys.stderr.write("reporter:counter:HW3.1 Mapper Mortgage Counters,Calls,1\n")
        
    else:
        prods['other'] += 1
        sys.stderr.write("reporter:counter:HW3.1 Mapper Other Counters,Calls,1\n")
        
for key in sorted(prods):
    print '%s\t%s' % (key, prods[key])

# END STUDENT CODE HW31MAPPER