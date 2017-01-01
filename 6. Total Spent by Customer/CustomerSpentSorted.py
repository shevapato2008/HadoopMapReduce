from mrjob.job import MRJob
from mrjob.step import MRStep

class SpendByCustomerSorted(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_getSpent,
                   reducer = self.reducer_totalSpent),
            MRStep(mapper = self.mapper_makeSpentKey,
                   reducer = self.reducer_outputTopSpent)
        ]

    def mapper_getSpent(self, _, line):
        (customerID, itemID, order) = line.split(',')
        yield customerID, float(order)

    def reducer_totalSpent(self, customerID, orders):
        yield customerID, sum(orders)

    def mapper_makeSpentKey(self, customerID, totalSpent):
        yield '%04.02f'%float(totalSpent), customerID

    def reducer_outputTopSpent(self, spent, customerIDList):
        for customerID in customerIDList:
            yield customerID, spent

if __name__ == '__main__':
    SpendByCustomerSorted.run();

'''
!python CustomerSpentSorted.py customer-orders.csv > customerspentsorted.txt
'''