#!/usr/bin/env python
#START STUDENT CODE45
from numpy import argmin, array, random
import numpy as np
from sklearn import preprocessing
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import chain
import os

# find the nearest centroid for data point 
def MinDist(datapoint, centroid_points):
    datapoint = array(datapoint)
    centroid_points = array(centroid_points)
    diff = datapoint - centroid_points 
    diffsq = diff*diff
    # Get the nearest centroid for each instance
    minidx = argmin(list(diffsq.sum(axis = 1))) 
    return minidx

# check whether centroids converge
def stop_criterion(centroid_points_old, centroid_points_new,T):
    oldvalue = list(chain(*centroid_points_old))
    newvalue = list(chain(*centroid_points_new))
    Diff = [abs(x-y) for x, y in zip(oldvalue, newvalue)]
    Flag = True
    for i in Diff:
        if(i>T):
            Flag = False
            break
    return Flag

class MRKmeans(MRJob):
    centroid_points=[]  
    def steps(self):
        return [
            MRStep(
                mapper_init = self.mapper_init, 
                mapper=self.mapper,
                combiner = self.combiner,
                reducer=self.reducer
            )     
        ]
    
    
    #load centroids info from file
    def mapper_init(self):
        print "Current path:", os.path.dirname(os.path.realpath(__file__))
        self.centroid_points = [map(float,s.split('\n')[0].split(',')) for s in open("Centroids.txt").readlines()]
        print "Centroids: ", self.centroid_points

    def mapper(self, _, line):
        words = (map(float,line.split(',')))
        norm_words = np.array([float(x) / int(words[2])  for x in words[3:]])
        yield int(MinDist(norm_words,self.centroid_points)), (list(norm_words),{int(words[1]):1})

    def combiner(self, idx, inputdata):
        d_code = {}
        sumx = []
        
        #combine counts per code
        for x,code in inputdata:
            for key, value in code.iteritems():
                d_code[key] = d_code.get(key, 0) + value
    
            # convert list to an array
            x = np.array(x)
            
            # aggregate normalized counts
            if len(sumx) == 0:
                sumx = np.zeros(x.size)
            sumx += x
            
        yield idx,(list(sumx),d_code)

    # aggregate sum for each cluster and then calculate the new centroids
    def reducer(self, idx, inputdata):
        d_code = {}
        sumx = []
        
        #combine counts per code
        for x,code in inputdata:
            for key, value in code.iteritems():
                d_code[key] = d_code.get(key, 0) + value
            
            # convert list to an array
            x = np.array(x)
            
            # aggregate normalized counts
            if len(sumx) == 0:
                sumx = np.zeros(x.size)
            sumx += x

        # new centroids
        centroids = sumx / sum(d_code.values())

        yield idx,(list(centroids),d_code)

if __name__ == '__main__':
    MRKmeans.run()