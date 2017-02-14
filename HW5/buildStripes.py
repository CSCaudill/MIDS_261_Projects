#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import re
import mrjob
import json
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRbuildStripes(MRJob):
            
    def mapper(self, _, line):
        ngram,count,page_count,book_count = line.split('\t',3)
        ngram = ngram.strip()
        count = int(count)
        
        # lowercase and parse out each word
        words = ngram.lower().split()
        
        d = {}
        
        # Create a dictionary within a dictionary
        # For example: d[biography] = {"a": 92, "of": 92, "george": 92, "general": 92}
        
        for term in sorted(words):
            if term not in d.keys():
                d[term] = {}

            for term2 in sorted(words):
                if term != term2:
                    if term2 in d[term]:
                        d[term][term2] += count
                    else:
                        d[term][term2] = count
                        
        # iterate through the dictionary and yield the top level term, the second term, and the cooccurrence count
        # Example: "biography, (general, 92)"
        
        for k,v in d.iteritems():
            for k2,v2 in d[k].iteritems():
                yield k, (k2, v2)
            
    
    def reducer(self, key, line):
        
        red_d = {}
        term1 = key
        
        # Combine the various term cooccurrence counts into a single dictionary
        
        for term,count in line:
            count = int(count)
            term2 = term
            if term1 not in red_d.keys():
                red_d[term1] = {}
            if term2 in red_d[term1]:
                red_d[term1][term2] += count
            else:
                red_d[term1][term2] = count

        for k,v in red_d.iteritems():
            yield k,v
  
  #END SUDENT CODE531_STRIPES
if __name__ == '__main__':
    MRbuildStripes.run()