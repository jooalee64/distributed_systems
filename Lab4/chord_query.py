"""
CPSC 5520 Lab4 
Jooa Lee
chord_query python file 
this file allows querying the Chord ring for a specific key or node. 
retrieve a user-specified value from the running Chord DHT

Example:
    To query a node running on port 44444 with the key ("russelwilson/25322975", 1947):
    python chord_query.py 44444 russelwilson/25322975 1947
"""
import sys 
from chord_node import ChordNode

if len(sys.argv) != 4:
    print("Usage: python chord_query.py PORT QB YEAR")
    print("Example: ")
    port = 44444 # any running node 
    # key = (russelwilson/25322975, 2016)
    key = ("russelwilson/25322975", 1947)
    print("python chord_query.py {} {} {}".format(port, key[0], key[1]))
else:
    port = int(sys.argv[1])
    key = (sys.argv[2], int(sys.argv[3]))

node = ChordNode.lookup_node(port)
print(node)
# print('looking up', key, 'from node', node)
# print(ChordNode.get(key))
