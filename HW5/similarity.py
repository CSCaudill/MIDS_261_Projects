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

class MRsimilarity(MRJob):
  
  #START SUDENT CODE531_SIMILARITY
    
    MRJob.SORT_VALUES = True 
    def steps(self):

        JOBCONF_STEP1 = {}
        JOBCONF_STEP2 = { 
          ######### IMPORTANT: THIS WILL HAVE NO EFFECT IN -r local MODE. MUST USE -r hadoop FOR SORTING #############
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1nr',
            
        }
        
        return [MRStep(jobconf=JOBCONF_STEP1,
                    mapper=self.mapper_pair_sim,
                    reducer=self.reducer_pair_sim)
                ,
                MRStep(jobconf=JOBCONF_STEP2,
                    mapper=None,   
                    reducer=self.reducer_sort)
                ]
            
    def mapper_pair_sim(self,_,line):
        line = line.strip()
        term,coterm = line.split("\t")
        coterm = json.loads(coterm)
        
        X = map(lambda x: x[0]+"."+str(x[1]) , coterm)
        
        # taking advantage of symmetry, output only (a,b), but not (b,a)
        # 'set' will output only the unique occurrences
        for subset in itertools.combinations(sorted(set(X)), 2):
            yield subset[0]+"."+subset[1], 1


    def reducer_pair_sim(self,key,value):
        Doc1, Doc1_len, Doc2, Doc2_len = key.split(".")
        doc1_len = int(Doc1_len)
        doc2_len = int(Doc2_len)
        t = sum(value)
        
        
        # calculate the similarity values
        jaccard = t / ( doc1_len + doc2_len - t )
        cosine = t * ((1/math.sqrt(doc1_len)) * (1/math.sqrt(doc2_len)))
        dice = (2*t) / (doc1_len + doc2_len)
        overlap = t / min(doc1_len, doc2_len)
        
        # Average the 4 similarities 
        avg = sum([jaccard,cosine,dice,overlap]) / 4
        
        yield [avg,jaccard,cosine,overlap,dice], (Doc1+" - "+Doc2)
    
    
    def reducer_sort(self,key,value):
        for v in value:
            yield key,v
            
#END SUDENT CODE531_SIMILARITY
  
if __name__ == '__main__':
    MRsimilarity.run()