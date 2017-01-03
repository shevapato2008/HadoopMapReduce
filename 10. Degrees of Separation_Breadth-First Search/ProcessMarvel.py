# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.

import sys

print 'Creating BFS starting input for character ' + sys.argv[1]    # sys.argv[1] is command line argument

with open("BFS-iteration-0.txt", 'w') as out:       # output file

    with open("Marvel-graph.txt") as f:             # input file

        for line in f:
            fields = line.split()
            heroID = fields[0]
            numConnections = len(fields) - 1        # number of neighbors for each node
            connections = fields[-numConnections:]  # use a list "connections" to store all the neighbors

            color = 'WHITE'                         # the node initially is marked unvisited (white)
            distance = 9999                         # all nodes (except starting node) has a starting distance 9999

            if (heroID == sys.argv[1]):             # starting node has color gray and distance 0
                color = 'GRAY'
                distance = 0

            if (heroID != ''):
                edges = ','.join(connections)                               # separate all neighbors with ','
                outStr = '|'.join((heroID, edges, str(distance), color))    # join
                out.write(outStr)
                out.write("\n")

    f.close()

out.close()