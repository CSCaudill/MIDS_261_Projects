#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-


from __future__ import division
import collections
import re
import json
import math
# import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRinvertedIndex(MRJob):
    
#START STUDENT CODE531_INV_INDEX

    def mapper(self,_,line):
        
        line = line.strip()
        key_term, words = line.split("\t")
        
        # 'words' are coming in with the structure of a dictionary, but formatted as a string
        # ast.literal_eval converts it to the dictionary that it should be
        words = ast.literal_eval(words)
        _len = len(words)
        
        # for each word, output the cooccurring terms and the number of associated cooccurring terms
        for word in words:
            yield word, (key_term, _len)
        
    def reducer(self,key,value):
        
        d = collections.defaultdict(list)
        for v in value:
            d[key].append(v)
        yield key,d[key] 

#END STUDENT CODE531_INV_INDEX
        
if __name__ == '__main__':
    MRinvertedIndex.run() 