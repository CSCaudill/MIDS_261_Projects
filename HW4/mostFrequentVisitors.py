#!/usr/bin/env python
#START STUDENT CODE44

from mrjob.job import MRJob
import csv

d={}

def csv_readline(line):
#     """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row

class FrequentVisitor(MRJob):

    def mapper(self, line_no, line):
#         """Extracts the site that was visited"""
        cell = csv_readline(line)
        if cell[0] == 'A':
            d[cell[1]] = cell[3]
        if cell[0] == 'V':
            key = (cell[1],cell[4], d[cell[1]])
            yield key,1

    def reducer(self, key, visit_counts):
#         """Sumarizes the visit counts by adding them together."""
        total = sum(i for i in visit_counts)
        yield key, total
        
if __name__ == '__main__':
    FrequentVisitor.run()

#END STUDENT CODE44