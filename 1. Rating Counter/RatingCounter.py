from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRRatingCounter.run()



'''
Download the ml-100k.zip file from http://grouplens.org/datasets/movielens/.
Unzip it and put it in the same folder as python code.
Execute the following command in the Python Console.
    !python RatingCounter.py ml-100k/u.data
'''