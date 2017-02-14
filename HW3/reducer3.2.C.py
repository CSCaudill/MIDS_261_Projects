#!/usr/bin/env python
# START STUDENT CODE HW32CREDUCER

import sys
# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.2.C Reducer Counters,Calls,1\n")

d = {}

# For each line from the mapper output, split the three attributes 
# Spam/Ham classification, key term, value count
for line in sys.stdin:
    key,value = line.split("\t",1)
    
    # check if docClass + key term combination exist in dictionary
    # if so, update the term count value
    if key in d:
        d[key] += int(value)
        
    # if combination does not exist, add it to the dictionary
    else:
        d[key] = int(value)
        
# for each value in the dictionary, print the 3 attributes in a 
# way that they can later be sorted appropriately
for key, value in d.iteritems():
    print '%s\t%s' % (key, value)

# END STUDENT CODE HW32CREDUCER