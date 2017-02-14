#!/usr/bin/env python
# START STUDENT CODE HW32CFREQREDUCER

import sys
# Set up counters to monitor/understand the number of times a reducer task is run
sys.stderr.write("reporter:counter:HW3.2.C Reducer Counters,Calls,1\n")

d = {}
total = 0
# For each line from the mapper output, split the three attributes 
# Spam/Ham classification, key term, value count
for line in sys.stdin:
    key,value,x = line.split("\t",2)
    
    # check if docClass + key term combination exist in dictionary
    # if so, update the term count value
    if key in d:
        d[key] += int(value)
        
    # if combination does not exist, add it to the dictionary
    else:
        d[key] = int(value)

for key, value in d.iteritems():
    total += int(value)
        
# for each value in the dictionary, print the 3 attributes in a 
# way that they can later be sorted appropriately
for key, value in d.iteritems():
    freq = 0
    freq = round((float(value) / float(total)),4)
    print '%s\t%s\t%s' % (key, value, freq)

# END STUDENT CODE HW32CFREQREDUCER