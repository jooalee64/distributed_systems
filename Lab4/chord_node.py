"""
CPSC 5520 Lab4 
Jooa Lee
chord_node python file 

chord_node takes a port number an existing node (or 0/None to indicate it should start a new network)
the program joins a new node into the network using a system-assigned port number for itself.
the node joins then listens for incoming connections (other nodes or queries)
use blocking TCP for this and pickle for the marshaling.
"""

import socket
import threading
import time
import random
import json
import hashlib
from typing import Dict, Optional
import sys

M = 7  # 2^7 = 128 nodes
NODES = 2**M
BUF_SZ = 4096
BACKLOG = 100
TEST_BASE = 43544

def generate_node_id(input_string: str) -> int:
    """
    Generate a hased node ID using SHA-1 hash of the input
    Returns an integer in the range [0, 2^M - 1].
    """
    sha1_hash = hashlib.sha1(str(input_string).encode()).hexdigest()
    # Take first M bits by converting to int and using modulo
    node_id = int(sha1_hash[:M], 16) % NODES
    return node_id

class ModRange(object):
    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __contains__(self, id):
        if id is None:
            return False
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        return sum(len(interval) for interval in self.intervals)

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

class ModRangeIter(object):
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            self.i += 1
            self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError(f'Invalid finger entry values: n={n}, k={k}')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __contains__(self, id):
        return id in self.interval

class ChordNode(object):
    def __init__(self, input_string: str, buddy_port: Optional[int] = None):
        """initialize a Chord node, setting up its unique ID, finger table, and networking
        :param input_string: the input string used to generate the node's unqiue identifier 
        :param buddy_port (Optional): the port of an existing node in the network to join
        """
        self.node = generate_node_id(input_string)
        self.input_string = input_string
        
        # Initialize finger table with M entries
        self.finger = [None] + [FingerEntry(self.node, k) for k in range(1, M+1)]
        self._predecessor = None
        self.keys = {}
        
        # Network setup
        self.port = TEST_BASE + self.node
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('localhost', self.port))
        self.socket.listen(BACKLOG)
        self.running = True

        
        # Start RPC server
        self.rpc_thread = threading.Thread(target=self._listen)
        self.rpc_thread.daemon = True
        self.rpc_thread.start()


        # give the RPC server a moment to start
        time.sleep(0.1)

        if buddy_port is not None:
            buddy_node_id = generate_node_id(buddy_port)
            # buddy_node_id = buddy_port - TEST_BASE
            self.join_network(buddy_node_id)
        else:
            print(f"Starting new network with node")
            self.start_new_network()


        print(f"Node {self.node} initialized with SHA-1 hash of '{input_string}'")

    # @staticmethod
    def lookup_node(n):
        """Resolves the address of a node based on its ID
        :param n: the node ID
        :return: a tuple containing the hostname ('localhost) and the corresponding port
        """
        return 'localhost', TEST_BASE + n

    def put(self, key, value):
        """
        stores a key-valye pair in the appropirate node in the chord network
        :param key: the key to store 
        :param value: the value associated with the key 
        :return: True if the key-value pair was stored successfully. otherwise return False 
        """
        try:
            responsible_node = self.find_successor(key)
            if responsible_node == self.node.id:
                self.keys[key] = value 
                return True 
            else:
                self.keys[key] = value 
                return True 
        except Exception as e:
            return False 
                
    def get(self,key):
        """
        Retrieves the value associated with a given key from the node's key store.

        :param key: The key to look up.
        :return: The value associated with the key if found, None otherwise.
        """
        try:
            return self.keys.get(key)
        except Exception as e:
            return None
        
    def _listen(self):
        """
        Starts a loop to listen for incoming connections and spawns a thread to handle each client.

        :return: None
        """
        while self.running:
            try:
                client, _ = self.socket.accept()
                threading.Thread(target=self._handle_client, args=(client,)).start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection on node {self.node}: {e}")

    def _handle_client(self, client_socket: socket.socket):
        """
        Handles an incoming client request over a socket, processes the request, 
        and sends a response back to the client.

        :param client_socket: The socket object representing the client connection.
        :return: None
        """
        try:
            data = b""
            while True:
                chunk = client_socket.recv(BUF_SZ)
                if not chunk:
                    break
                data += chunk
            
            if data:
                request = json.loads(data.decode())
                method = request['method']
                args = request.get('args', [])
                kwargs = request.get('kwargs', {})
                
                if method == 'get_predecessor':
                    result = self._predecessor
                elif method == 'set_predecessor':
                    self._predecessor = args[0]
                    result = None
                elif method == 'get_successor':
                    result = self.get_successor()
                elif method == 'set_successor':
                    self.set_successor(args[0])
                    result = None
                else:
                    func = getattr(self, method)
                    result = func(*args, **kwargs)
                
                response = json.dumps({
                    'status': 'success',
                    'result': result
                }).encode()
                client_socket.sendall(response)
        except Exception as e:
            print(f"Error handling RPC on node {self.node}: {e}")
        finally:
            client_socket.close()

    def call_rpc(self, target_node_id: int, method: str, *args, **kwargs) -> Optional[any]:
        """
        Makes an RPC call to another node in the network to invoke a specified method.

        :param target_node_id: The ID of the target node to call the method on.
        :param method: The method name to invoke on the target node.
        :param args: Positional arguments to pass to the method.
        :param kwargs: Keyword arguments to pass to the method.
        :return: The result of the method call, or None if the call fails or no result is returned.
        """
        if target_node_id is None or target_node_id == self.node:
            return getattr(self, method)(*args, **kwargs) if method not in ['predecessor', 'successor'] else None
            
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            target_port = TEST_BASE + target_node_id
            # s.connect(self.lookup_node(target_node_id))
            s.connect(('localhost', target_port))
            
            if method == 'predecessor' and not args:
                method = 'get_predecessor'
            elif method == 'predecessor' and args:
                method = 'set_predecessor'
            elif method == 'successor' and not args:
                method = 'get_successor'
            elif method == 'successor' and args:
                method = 'set_successor'
            
            request = json.dumps({
                'method': method,
                'args': args,
                'kwargs': kwargs
            }).encode()
            
            s.sendall(request)
            s.shutdown(socket.SHUT_WR)
            
            data = b""
            while True:
                chunk = s.recv(BUF_SZ)
                if not chunk:
                    break
                data += chunk
            
            if data:
                response = json.loads(data.decode())
                return response.get('result')
            return None
            
        except Exception as e:
            print(f"RPC call failed from node {self.node} to {target_node_id}: {e}")
            return None
        finally:
            s.close()

    def get_predecessor(self):
        """
        Returns the predecessor of the current node in the network.

        :return: The predecessor node ID, or None if not set.
        """
        return self._predecessor

    def set_predecessor(self, value):
        """
        Sets the predecessor of the current node in the network.

        :param value: The ID of the predecessor node.
        :return: None
        """
        self._predecessor = value
        
    def get_successor(self):
        """
        Returns the successor of the current node in the network.

        :return: The successor node ID.
        """
        return self.finger[1].node

    def set_successor(self, id):
        """
        Sets the successor of the current node in the network.

        :param id: The ID of the successor node.
        :return: None
        """
        self.finger[1].node = id

    predecessor = property(get_predecessor, set_predecessor)
    successor = property(get_successor, set_successor)

    def join_network(self, buddy_node_id):
        """
        Joins the Chord network through a specified buddy node.

        :param buddy_node_id: The ID of the buddy node to join through.
        :return: None
        """
        print(f"Node {self.node} joining network through buddy node {buddy_node_id}")
        self.init_finger_table(buddy_node_id)
        self.update_others()
        print(f"Node {self.node} successfully joined the network")


    def start_new_network(self):
        """
        Starts a new Chord network with the current node as the sole member.

        :return: None
        """
        for i in range(1, M + 1):
            self.finger[i].node = self.node
        self.predecessor = self.node

    def init_finger_table(self, buddy_node_id):
        """
        Initializes the finger table by contacting a buddy node.

        :param buddy_node_id: The ID of the buddy node used to initialize the finger table.
        :return: None
        """
        print(f"Initializing finger table through buddy node {buddy_node_id}")
        
        # Find successor for first finger entry
        successor_id = self.call_rpc(buddy_node_id, 'find_successor', self.finger[1].start)
        if successor_id is None:
            raise Exception(f"Could not find successor through buddy node {buddy_node_id}")
        
        self.finger[1].node = successor_id
        print(f"Found successor: {successor_id}")

        # Get predecessor from successor
        pred_id = self.call_rpc(successor_id, 'get_predecessor')
        self.predecessor = pred_id
        
        # Update successor's predecessor to be this node
        self.call_rpc(successor_id, 'set_predecessor', self.node)
        
        # Initialize remaining entries
        for i in range(1, M):
            if self.finger[i + 1].start in ModRange(self.node, self.finger[i].node, NODES):
                self.finger[i + 1].node = self.finger[i].node
            else:
                next_id = self.call_rpc(buddy_node_id, 'find_successor', self.finger[i + 1].start)
                if next_id is not None:
                    self.finger[i + 1].node = next_id
            print(f"Initialized finger[{i+1}] = {self.finger[i+1].node}")

        
    def find_successor(self, id):
        """
        Finds the successor node for a given ID by looking up the finger table.

        :param id: The ID for which the successor is to be found.
        :return: The ID of the successor node.
        """
        if id is None:
            return None
        np = self.find_predecessor(id)
        if np is None or np == self.node:
            return self.successor
        return self.call_rpc(np, 'successor')

    def find_predecessor(self, id):
        """
        Finds the predecessor node for a given ID by looking up the finger table.

        :param id: The ID for which the predecessor is to be found.
        :return: The ID of the predecessor node.
        """
        if id is None:
            return None
        n_prime = self.node
        while True:
            succ = self.call_rpc(n_prime, 'successor')
            if succ is None:
                return n_prime
            if id in ModRange((n_prime + 1) % NODES, succ, NODES):
                break
            n_next = self.call_rpc(n_prime, 'closest_preceding_finger', id)
            if n_next == n_prime:
                break
            n_prime = n_next
        return n_prime

    def closest_preceding_finger(self, id):
        """
        Finds the closest preceding finger for a given ID in the finger table.

        :param id: The ID to find the closest preceding finger for.
        :return: The closest preceding finger node ID.
        """
        if id is None:
            return self.node
        for i in range(M, 0, -1):
            if (self.finger[i].node is not None and 
                self.finger[i].node in ModRange((self.node + 1) % NODES, id, NODES)):
                return self.finger[i].node
        return self.node

    def update_others(self):
        """
        Updates all nodes whose finger tables should refer to the current node.

        This ensures that the current node's ID is included in the finger tables of other nodes,
        maintaining the consistency of the Chord network.

        :return: None
        """
        print(f"Node {self.node} updating other nodes' finger tables")
        for i in range(1, M + 1):
            # Find last node p whose ith finger might be this node
            p = self.find_predecessor((self.node - 2**(i-1) + NODES) % NODES)
            if p is not None:
                self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """
        Updates the finger table entries when a new node joins the network.

        :param s: The new node that has joined the network.
        :param i: The index of the finger table entry to update.
        :return: None
        """
        if s in ModRange(self.node, self.finger[i].node, NODES):
            old_finger = self.finger[i].node
            self.finger[i].node = s
            print(f"Node {self.node} updated finger[{i}] = {s}")
            
            # Propagate the update
            p = self.predecessor
            if p is not None and p != s:
                self.call_rpc(p, 'update_finger_table', s, i)
            return True
        return False

    def start_maintenance(self):
        """
        Starts background maintenance tasks to keep the Chord network consistent.

        This typically involves tasks like periodic finger table updates, checking predecessor
        and successor consistency, and other necessary maintenance operations.

        :return: None
        """
        def run_periodically(func, interval):
            while self.running:
                try:
                    func()
                except Exception as e:
                    if self.running:
                        print(f"Error in {func.__name__} on node {self.node}: {e}")
                time.sleep(interval)
        
        tasks = [
            (self.stabilize, 1),
            (self.fix_fingers, 2),
            (self.check_predecessor, 3)
        ]
        
        for func, interval in tasks:
            thread = threading.Thread(target=run_periodically, args=(func, interval))
            thread.daemon = True
            thread.start()

    def stabilize(self):
        """
        Verifies and updates the immediate successor of the current node.

        This method checks the successor's status and ensures the consistency of the successor reference,
        updating the finger table if necessary.

        :return: None
        """
        x = self.call_rpc(self.successor, 'predecessor')
        if x is not None and x in ModRange((self.node + 1) % NODES, self.successor, NODES):
            self.successor = x
        self.call_rpc(self.successor, 'notify', self.node)

    def notify(self, n_prime):
        """
        Updates the predecessor if appropriate.

        This method checks if the provided node (n_prime) should become the new predecessor
        and updates the current node's predecessor accordingly.

        :param n_prime: The node that may become the new predecessor.
        :return: None
        """
        if (self.predecessor is None or 
            n_prime in ModRange(self.predecessor, self.node, NODES)):
            self.predecessor = n_prime

    def fix_fingers(self):
        """
        Periodically refreshes finger table entries to ensure the network's consistency.

        This method is typically called at regular intervals to update finger table entries
        and ensure that the node's finger table accurately reflects the current state of the network.

        :return: None
        """
        i = random.randint(1, M)
        self.finger[i].node = self.find_successor(self.finger[i].start)

    def check_predecessor(self):
        """
        Checks if the predecessor has failed.

        This method determines whether the predecessor node is still alive and reachable.
        If the predecessor has failed, the current node takes appropriate actions to update
        its predecessor reference.

        :return: None
        """
        if self.predecessor is not None:
            try:
                self.call_rpc(self.predecessor, 'ping')
            except Exception:
                self.predecessor = None

    def ping(self):
        """simple check
        :return: True
        """
        return True

    def stop(self):
        """Clean shutdown of the node"""
        self.running = False
        try:
            self.socket.close()
        except Exception as e:
            print(f"Error stopping node {self.node}: {e}")

def create_network(node_strings: list[str]) -> Dict[int, ChordNode]:
    """
    Creates a Chord network with the given node strings.

    This function initializes a set of ChordNode instances based on the provided list of node strings.
    It sets up each node and links them together to form a consistent Chord network.

    :param node_strings: A list of strings representing the node identifiers.
    :return: A dictionary mapping node IDs to their corresponding ChordNode instances.
    """
    nodes: Dict[int, ChordNode] = {}
    used_ids = set()
    
    print(f"\nCreating Chord network with {len(node_strings)} nodes (M={M}, NODES={NODES})")
    
    for string in node_strings:
        node_id = generate_node_id(string)
        
        if node_id in used_ids:
            print(f"Warning: Hash collision detected for '{string}' (ID: {node_id})")
            continue
        
        print(f"\nCreating node for '{string}' with ID {node_id}")
        
        new_node = ChordNode(string)
        nodes[node_id] = new_node
        used_ids.add(node_id)

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <node_name> [buddy_port]")
        sys.exit(1)

    print("\n=== Starting Chord Network Tests ===")
    print(f"Network configuration: M={M} (allows {NODES} nodes)")
    print(f"Base port number: {TEST_BASE}")
    
    # first_string = node_strings[0]
    # first_id = generate_node_id(first_string)
    node_name = sys.argv[1]
    existing_port = int(sys.argv[2]) if len(sys.argv) > 2 else None #buddy port

    try:
        node = ChordNode(node_name, existing_port)
        node.start_maintenance()

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down node...")
        node.stop()
        sys.exit(0)

if __name__ == "__main__":
    main()
