#!/usr/bin/env python
#START STUDENT CODE43

from mrjob.job import MRJob
import csv

def csv_readline(line):
#     """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class SiteVisit(MRJob):

    def mapper(self, line_no, line):
#         """Extracts the site that was visited"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[1],1

    def reducer(self, site, visit_counts):
#         """Sumarizes the visit counts by adding them together."""
        total = sum(i for i in visit_counts)
        yield site, total
        
if __name__ == '__main__':
    SiteVisit.run()

#END STUDENT CODE43