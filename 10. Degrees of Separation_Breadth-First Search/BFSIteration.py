from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node:
    def __init__(self):
        self.characterID = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'

    # Format is ID|EDGES|DISTANCE|COLOR
    def fromLine(self, line):                       # fromLine converts a line of file to a node
        fields = line.split('|')
        if (len(fields) == 4):
            self.characterID = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]

    def getLine(self):                              # getLine converts node to a line of output file
        connections = ','.join(self.connections)
        return '|'.join((self.characterID, connections, str(self.distance), self.color))


class MRBFSIteration(MRJob):

    INPUT_PROTOCOL = RawValueProtocol               # input and output will be untouched using RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRBFSIteration, self).configure_options()
        self.add_passthrough_option(
            '--target', help = "ID of character we are searching for")

    # In general, mapper conduct BFS to mark the distance of all nodes from the starting node.
    # Each line has a starting node.
    def mapper(self, _, line):
        node = Node()
        node.fromLine(line)
        # If this node needs to be expanded...
        if (node.color == 'GRAY'):                          # if we are processing (visiting) the current node
            for connection in node.connections:             # we loop through all its neighbors
                neighbor = Node()
                neighbor.characterID = connection
                neighbor.distance = int(node.distance) + 1  # neighboring node distance = current node distance + 1
                neighbor.color = 'GRAY'                     # mark as processing (visiting), color it grey
                if (self.options.target == connection):
                    counterName = ("Target ID " + connection + " was hit with distance " + str(neighbor.distance))
                    self.increment_counter('Degrees of Separation', counterName, 1)
                yield connection, neighbor.getLine()

            node.color = 'BLACK'                            # We've processed (visited) current node. Color it black.

        yield node.characterID, node.getLine()              # Emit the input node so we don't lose it.

    def reducer(self, key, values):
        edges = []
        distance = 9999
        color = 'WHITE'

        for value in values:
            node = Node()
            node.fromLine(value)

            if (len(node.connections) > 0):
                edges.extend(node.connections)              # edges = node.connections

            if (node.distance < distance):
                distance = node.distance                    # find the shortest distance

            if (node.color == 'BLACK'):
                color = 'BLACK'

            if (node.color == 'GRAY' and color == 'WHITE'):
                color = 'GRAY'

        node = Node()
        node.characterID = key
        node.distance = distance
        node.color = color
        # There's a bug in mrjob for Windows where sorting fails
        # with too much data. As a workaround, we're limiting the
        # number of edges to 500 here. You'd remove the [:500] if you
        # were running this for real on a Linux cluster.
        node.connections = edges[:500]

        yield key, node.getLine()

if __name__ == '__main__':
    MRBFSIteration.run()

'''
!python BFSIteration.py --target=100 BFS-iteration-0.txt > BFS-iteration-1.txt
!python BFSIteration.py --target=100 BFS-iteration-1.txt > BFS-iteration-2.txt
'''
