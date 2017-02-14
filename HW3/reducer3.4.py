#!/usr/bin/env python

import sys
from collections import defaultdict

# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.4 Reducer Counters,Calls,1\n")

d = defaultdict(int)
total = 0

for line in sys.stdin:
    
    # the x is actually only a placeholder because the true input only has 3 relevant fields,
    # but the output has 4
    prod1,prod2,count,x = line.split('\t',3)
    prod1 = prod1.strip()
    prod2 = prod2.strip()
    count = int(count)

    # add the count to the dictionary where appropriate
    if (prod1, prod2) in d:
        d[(prod1,prod2)] += count
    else:
    #elif (prod2, prod1) not in d:
        d[(prod1,prod2)] = count

# Calculate the total number of product interactions
for key, value in d.iteritems():
    if value >= 100:
        total += int(value)
        
# As mentioned in the prompt, calculate the relative frequency of each product combination, 
# but only for ones that have at least 100 co-occurrences
for key, value in d.iteritems():
    if value >= 100:
        freq = 0
        freq = round((float(value) / float(total)),4)
        print '%s\t%s\t%s\t%s' % (key[0], key[1], value, freq)