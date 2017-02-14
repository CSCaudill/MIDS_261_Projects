#!/usr/bin/env python

import sys
from collections import defaultdict
# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.4 Mapper Counters,Calls,1\n")
d = {}
for line in sys.stdin:
    line.strip()
    prods = line.split()
    
    # The outer loop runs through each individual item in a basket
    for item1 in prods:
        if item1 not in d:
            d[item1] = {}
        
        # The inner loop sorts through all of the other basket items and creates an interaction
        for item2 in prods:
            if item1 != item2:
                if item2 in d[item1]:
                    d[item1][item2] += 1
                else:
                    d[item1][item2] = 1

                
# the '10' is just a placeholder because the reducer is going to have 4 outputs
for key in d.iteritems():
#     s = ''
#     for k in d[key]:
#         s = s + '(' + k + ',' + d[key][key2] + ')'
    print '%s\t%s' % (key[0], key[1])