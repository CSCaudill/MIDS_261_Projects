#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import os

class rightJoin(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper_init = self.mapper_init,
                mapper = self.mapper,
                mapper_final = self.mapper_final
            )]
    
    def mapper_init(self):
        countries = open(str(os.path.dirname(os.path.realpath(__file__)))+"/countries.txt","r")
        self.right=0
        self.nomatch=0
        self.ct = {}
        self.matched_ct={}
        for line in countries:
            ct_long, ct_short  = line.split('|',1)
            self.ct[ct_short.strip()] = ct_long.strip()         
#             self.matched_ct[ct_short.strip()] = ct_long.strip()
            
    def mapper(self, _, line):
        cust,price,country = line.split('|',2)
        
        # In this case, we are right joining with the transactions file on the left. 
        # Therefore, we will output all of the rows from the countries table, and then any rows
        # from the transactions table that have a country match
        
        if country in self.ct:
            self.right += 1
            yield None, country+","+self.ct[country]+","+cust+","+price
            self.matched_ct[country] = self.ct[country]

            
            
    # Mapper_Final will output the total joined rows
    def mapper_final(self):
        for key,value in self.ct.iteritems():
            if key not in self.matched_ct:
                self.nomatch += 1
                yield None, key+","+value+",NULL,NULL"
        yield None, "right-joined "+str(self.right)+" row(s)."
        yield None, "No right match for "+str(self.nomatch)+" row(s)."
        yield None, "Total Rows: "+str(self.right + self.nomatch)        

if __name__ == '__main__':
    rightJoin.run()