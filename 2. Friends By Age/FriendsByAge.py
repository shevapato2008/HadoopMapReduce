from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)    # Cast numFriends as a float.
                                        # This tells python we can do arithmetic operations on it.

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
        yield age, total / numElements

if __name__ == '__main__':
    MRFriendsByAge.run()

'''
Run the following command in Python Console.
!python FriendsByAge.py fakefriends.csv > friendsbyage.txt
'''