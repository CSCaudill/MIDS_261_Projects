#!/usr/bin/env python

import sys
from collections import defaultdict

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.4 Combiner Counters,Calls,1\n")

d1 = defaultdict(int)

for line in sys.stdin:
    prod1,prod2,count = line.split('\t',2)
    prod1 = prod1.strip()
    prod2 = prod2.strip()
    count = int(count)
    
    if (prod1, prod2) in d1:
        d1[(prod1,prod2)] += count
    elif (prod2, prod1) not in d1:
        d1[(prod1,prod2)] = count       

        
for key, value in d1.iteritems():
    print '%s\t%s\t%s' % (key[0], key[1], value)