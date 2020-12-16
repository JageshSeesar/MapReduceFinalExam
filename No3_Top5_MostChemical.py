from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'CDPHId,ProductName,CSFId,CSF,CompanyId,CompanyName,BrandName,PrimaryCategoryId,PrimaryCategory,SubCategoryId,SubCategory,CasId,CasNumber,ChemicalId,ChemicalName,InitialDateReported,MostRecentDateReported,DiscontinuedDate,ChemicalCreatedAt,ChemicalUpdatedAt,ChemicalDateRemoved,ChemicalCount'.split(',')

class Top5MostChemical(MRJob):
 
 def mapper(self, _, line):
   # Convert each line into a dictionary
   row = dict(zip(cols, [a.strip()
         for a in csv.reader([line]).__next__()]))
 
  if row['ChemicalCount'] and row['ChemicalName']:
    #take ChemicalName as key and ChemicalCount as value
    yield (row['ChemicalName']), int(row['ChemicalCount'])
 
 def reducer_count_product(self, key, values):
    # send all (num_occurrences, word) pairs to the same reducer.
     # num_occurrences is so we can easily use Python's sum() function.
   yield None, ('%04d'%int(sum(values)), key)
 
 def secondreducer(self,key,values):
   self.alist = []
   for value in values:
     self.alist.append(value)
   self.blist = []
   for i in range(5):			# top 5 chemical ingredients
     self.blist.append(max(self.alist))
     self.alist.remove(max(self.alist))
   for i in range(5):			# top 5 chemical ingredients
     yield self.blist[i]

 def steps(self):
   return [
     MRStep(mapper=self.mapper,
       #combiner=self.combine,
       reducer=self.reducer_count_product),
     MRStep(reducer=self.secondreducer)
     ]

if __name__ == '__main__':
 Top5MostChemical.run()