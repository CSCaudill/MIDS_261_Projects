#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import os

class innerJoin(MRJob):
    
    def steps(self):
        countries=[]
        return [
            MRStep(
                mapper_init = self.mapper_init,
                mapper = self.mapper,
                mapper_final = self.mapper_final
            )]
    
    def mapper_init(self):
        countries = open(str(os.path.dirname(os.path.realpath(__file__)))+"/countries.txt","r")
        self.inner=0
        self.ct = {}
        for line in countries:
            ct_long, ct_short  = line.split('|',1)
            self.ct[ct_short.strip()] = ct_long.strip()         
 
    def mapper(self, _, line):
        cust,price,country = line.split('|',2)
        if country in self.ct:
            self.inner += 1
            yield None, self.ct[country]+","+cust+","+price
            
    def mapper_final(self):
        yield None, "inner-joined "+str(self.inner)+" rows."
        
        
if __name__ == '__main__':
    innerJoin.run()