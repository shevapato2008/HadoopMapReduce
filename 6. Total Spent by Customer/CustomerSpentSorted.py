from mrjob.job import MRJob

class MRCustomerSpent(MRJob):

    def mapper(self, _, line):
        (customerID, itemID, spent) = line.split(',')
        yield customerID, float(spent)

    def reducer(self, customerID, spent):
        yield customerID, sum(spent)

if __name__ == '__main__':
    MRCustomerSpent.run();