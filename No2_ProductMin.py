from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'CDPHId,ProductName,CSFId,CSF,CompanyId,CompanyName,BrandName,PrimaryCategoryId,PrimaryCategory,SubCategoryId,SubCategory,CasId,CasNumber,ChemicalId,ChemicalName,InitialDateReported,MostRecentDateReported,DiscontinuedDate,ChemicalCreatedAt,ChemicalUpdatedAt,ChemicalDateRemoved,ChemicalCount'.split(',')

class ProductMin(MRJob):
 def mapper(self, _, line):
 	# Convert each line into a dictionary
 	row = dict(zip(cols, [a.strip()
 	  for a in csv.reader([line]).__next__()]))
	
 	if row['ProductName'] and row['ChemicalCount']:
	# ProductName as key(string) and ChemicalCount as value
 	  yield row['ProductName'], int(row['ChemicalCount'])

 def combine(self, key, values):
	yield (key, min(values))

 def reducer_count_chemical(self, key, values):
 	# send all (num_occurrences, word) pairs to the same reducer.
 	# num_occurrences is so we can easily use Python's sum() function.
	yield None, (sum(values), key)

 def reducer_min_chemical(self, _, key_values_pairs):
 	# each item of word_count_pairs is calculated (count, word),
 	# so yielding one results in key=counts, value=word
 	try:
 	    yield min(key_values_pairs)
 	except ValueError:
 	    pass

 def steps(self):
   return [
     MRStep(mapper=self.mapper,
        combiner=self.combine,
        reducer=self.reducer_count_chemical),
     MRStep(reducer=self.reducer_min_chemical)
    ]

if __name__ == '__main__':
  ProductMin.run()