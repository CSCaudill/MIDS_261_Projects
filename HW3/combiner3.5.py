#!/usr/bin/env python

import sys
from collections import defaultdict
import ast

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.5 Reducer Counters,Calls,1\n")

d = defaultdict(int)

for line in sys.stdin:
    
    prod,stripe = line.split('\t')
    prod = prod.strip()
    stripe = ast.literal_eval(stripe)
    

    # add the count to the dictionary where appropriate
    if prod in d:
        for key in stripe.iteritems():
            if key[0] in d[prod]:
                d[prod][key[0]] += stripe[key[0]]
            else:
                d[prod][key[0]] = stripe[key[0]]
    else:
        d[prod] = stripe

for key in d.iteritems():
    print '%s\t%s' % (key[0], key[1])