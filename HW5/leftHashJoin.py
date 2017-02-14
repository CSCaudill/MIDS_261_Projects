#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import os

class leftJoin(MRJob):
    
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
        self.left=0
        self.nomatch=0
        self.ct = {}
        for line in countries:
            ct_long, ct_short  = line.split('|',1)
            self.ct[ct_short.strip()] = ct_long.strip()         
 
    def mapper(self, _, line):
        cust,price,country = line.split('|',2)
        
        # In this case, we are left joining with the transactions file on the left. 
        # Therefore, we will output 9 rows regardless of how many matches show up in the countries file.
        
        if country in self.ct:
            self.left += 1
            yield None, country+","+self.ct[country]+","+cust+","+price
        else:
            self.nomatch += 1
            yield None, country+",NULL,"+cust+","+price
            
            
    # Mapper_Final will output the total joined rows
    def mapper_final(self):
        yield None, "left-joined "+str(self.left)+" row(s)."
        yield None, "No left match for "+str(self.nomatch)+" row(s)."
        yield None, "Total Rows: "+str(self.left + self.nomatch)
        

if __name__ == '__main__':
    leftJoin.run()