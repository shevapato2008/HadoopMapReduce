from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors = "ignore")      # avoids issues in mrjob 5.0
            yield word.lower(), 1

    def combiner(self, key, values):    # combiner and reducer are only allowed to differ in the output
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()

'''
import os
os.chdir("..")
cd 7. Word Frequency With Combiners

!python WordFrequencyWithCombiner.py Book.txt > wccombiner.txt
'''
