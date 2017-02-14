#!/usr/bin/env python

import sys

# Set up counters to monitor/understand the number of times a mapper task is run
sys.stderr.write("reporter:counter:HW3.4 Mapper Counters,Calls,1\n")

for line in sys.stdin:
    prods = line.split()
    # The outer loop runs through each individual item in a basket
    for item1 in prods:
        # The inner loop sorts through all of the other basket items and creates an interaction
        for item2 in prods:
            if item1 != item2:
                
                # the '10' is just a placeholder because the reducer is going to have 4 outputs
                print '%s\t%s\t%s\t%s' % (item1, item2, 1,10)