from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperHero(MRJob):

    def configure_options(self):
        super(MostPopularSuperHero, self).configure_options()
        self.add_file_option('--names', help = 'Path to Marvel-names.txt')
        # This will send Marvel-names.txt to all the nodes running this job.

    def steps(self):
        return [
            MRStep(mapper = self.mapper_count_friends_per_line,
                   reducer = self.reducer_combine_friends),
            MRStep(mapper = self.mapper_prep_for_sort,
                   mapper_init = self.load_name_dictionary,
                   reducer = self.reducer_find_max_friends)
        ]

    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)                      # count number of friends for each line

    def reducer_combine_friends(self, heroID, friendCounts):
        yield heroID, sum(friendCounts)                         # sum of number of friends for each superhero

    def mapper_prep_for_sort(self, heroID, friendCounts):
        heroName = self.heroNames[heroID]                       # convert hero ID to hero name
        yield None, (friendCounts, heroName)                    # by a dictionary defined below

    def reducer_find_max_friends(self, key, value):
        yield max(value)

    def load_name_dictionary(self):
        self.heroNames = {}                                     # use self.heroNames makes heroNames
        with open("Marvel-names.txt") as f:                     # dictionary usable everywhere in this class.
            for line in f:
                fields = line.split('"')                        # split based on quotes
                heroID = int(fields[0])
                self.heroNames[heroID] = unicode(fields[1], errors = 'ignore')

if __name__ == '__main__':
    MostPopularSuperHero.run()

'''
import os
os.chdir("..")
cd 9. Most Popular Superhero
!python MostPopularSuperhero.py --names=Marvel-names.txt Marvel-Graph.txt > MostPopularSuperhero.txt
'''
