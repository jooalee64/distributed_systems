"""
Jooa Lee 
CPSC 5520 lab2
program a node in the group in python3
"""

# threaded server is a server program that usees multiple threads 
# to handle multiple connections simultaneously 

"""
# SU_ID = 4235445 (but tried different SU_ID on terminal for each node(process))
# port number = 5555

# host = localhost

python3 lab2.py localhost 5555 4235399 119 &
python3 lab2.py localhost 5555 4235300 120 
python3 lab2.py localhost 5555 4235333 130

Updated group members: [{(119, 4235399): ('127.0.0.1', 34249), (120, 4235300): ('127.0.0.1', 43614), (130, 4235333): ('127.0.0.1', 44659)}]
Starting election...
I won the election. Sending COORDINATOR message.
Sending COORDINATOR message to (119, 4235399) at ('127.0.0.1', 34249)
Sent COORDINATOR message to (119, 4235399) at ('127.0.0.1', 34249)
message: ('COORDINATOR', (130, 4235333))
New leader is (130, 4235333)
"""

import sys
import socket # for network communication
import threading # for handling multiple nodes simulataneously
import pickle
import random
from enum import Enum



class State(Enum):
    """
    Enum class to keep track of various peer states in the election process

    IDLE: the member is currently not participating in any activities
    ELECTION_IN_PROGRESS: the member is actively involved in an ongoing election process (candidate or voter)
    WAIT: the member is waiting for the outcome of an election 
    LEADER: the peer has been elected as the leader 
    """
    IDLE = 'IDLE' 
    ELECTION_IN_PROGRESS = 'ELECTION_IN_PROGRESS'
    WAIT = 'WAIT'
    LEADER = 'LEADER' 

# class Node
class Lab2(object):
    def __init__(self, host, port, su_id, days_to_birthday):
        """
        :param host: host for communication with gcd
        :param port: port for communicatio with gcd
        :param days_to_birthday: number of days until next birthday
        :param su_id: Seattle U student id
        """

        self.host = host 
        self.port = port

        self.su_id = su_id
        self.days_to_birthday = days_to_birthday
        

        self.process_id = (int(days_to_birthday), int(su_id))
        self.listen_address = (host, random.randint(30000, 50000)) 

        # peers a list to store other peers
        self.group_members = []
        # a dictionary to store open sockets to peers
        self.connections = {}

        
        self.leader = None
        self.election_in_progress = False
        self.state = State.IDLE

    # call functions to start a listener thread, join peers
    # via the GCD, and then initiate the election
    def run(self):
        """
        start the thread for listening to incoming messages
        1. set up the listener thread
        2. join group_members via the GCD
        3. after we have a list of group members then start and election
        
        """
        listener_thread = threading.Thread(target=self.start_listener) # server only listens
        print("Thread name:", threading.current_thread().name)
        listener_thread.start()
        
        new_member = self.connect_to_gcd(self.host, self.port) # return data (which is the new_member)
        self.add_group_members(new_member) # add initial member to group and start the election
        self.start_election()


    
    def start_election(self):
        """
        1. sets state to State.WAITING_FOR_OK to indicate election participation
        2. iterates through the list of known peers and check if their identity 
        is stronger based on days_on_birthday and su_id
            # if the stronger peer is found, sends "ELECTION" message along with
            # its own list of known peers to that peer using a seaprate thread.
        3. if no stronger peeres ard found, it declares itself the winner by calling
        declare_victory()
        """
        self.election_in_progress = True 
        self.state = State.ELECTION_IN_PROGRESS
        print("Starting election...")
        
        self.leader = True 

        process_ids = [list(member.keys())[0] for member in self.group_members]

        # Filter for higher process ids
        higher_processes = [
            member for member in process_ids 
            if member[0] > self.process_id[0] or (member[0] == self.process_id[0] and member[1] > self.process_id[1])
        ]
        if not higher_processes:
            self.declare_victory()
        else:
            for member in self.group_members:
                process_id, address = list(member.items())[0]
                if process_id[0] < self.process_id[0] or (process_id[0] == self.process_id[0] and process_id[1] < self.process_id[1]):
                    self.leader = False 
                    sender = threading.Thread(target=self.send_election_message, args=((process_id, address), 'ELECTION',  self.group_members))
                    sender.start()
        

    def add_group_members(self, data):
        """
        """
        self.group_members.append(data)
        print(f"Updated group members: {self.group_members}")

    def start_listener(self):
        """
        starts a thread for handling incoming messages
        """
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(self.listen_address)
        server_socket.listen(5)

        print(f"Listening for connections on {self.listen_address}")
        while True:
            client_conn, client_addr = server_socket.accept()
            # we can just pass connection to handle messages
            threading.Thread(target=self.handle_incoming_messages, args=(client_conn, )).start()


    def handle_incoming_messages(self, conn):
        """Handling incoming ELECTION or COORDINATOR messages"""
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break 
                message = pickle.loads(data)
                print("message:", message)
                if message[0] == 'ELECTION':
                    self.handle_election_message(message)
                elif message[0] == "COORDINATOR":
                    self.handle_coordinator_message(message)
            except:
                break 

    # handling election and coordinator messages
    def handle_election_message(self, message):
        _, new_members_list = message
        # Add any new members from the election message
        for member in new_members_list:
            if member not in self.group_members:
                self.group_members.append(member)

        # Send OK response if we are not in an election
        if not self.election_in_progress:
            self.send_ok(message[1])

        # Initiate new election if not already doing so
        if self.state != State.ELECTION_IN_PROGRESS:
            self.start_election()

    def handle_coordinator_message(self, message):
        _, new_leader = message
        self.leader = new_leader
        self.state = State.IDLE
        self.election_in_progress = False 
        print(f"New leader is {new_leader}")


    def send_ok(self, sender_address):
        """Send OK message back to the sender of the ELECTION message."""
        print("Sending OK message.")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(sender_address)  # Connect to the sender's address
                s.send(pickle.dumps(('OK',)))
                print(f"Sent OK message to {sender_address}")
        except Exception as e:
            print(f"Error sending OK message: {e}")
        print("Sending OK message.")



    def declare_victory(self):
        """
        declares itself as the leader and sends COORDIANTOR message
        sends "COORDINATOR" message to all known peers (except itself) to inform that it's the leader 
        sets leader to its own identity and state back to the State.IDLE
        """
        print("I won the election. Sending COORDINATOR message.") 
        self.state = State.LEADER
        self.election_in_progress = False
        self.leader = True

        # send coordinator message to all members 
        for member in self.group_members:
            process_id = list(member.keys())[0]
            address = member[process_id]
            print(f"Sending COORDINATOR message to {process_id} at {address}")
            self.send_coordinator_message(process_id, address)
           

    def send_election_message(self, member, group_members):
        """sends election message to a process (member)"""
        try:
            # create a socket connection to the member
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # extract the member address from the member tuple
                member_address = member[1]
                s.connect(member_address)
                election_message = ('ELECTION', group_members)
                s.send(pickle.dumps(election_message))
                print(f"Sent ELECTION message to {member}")
        except Exception as e:
            print(f"Error sending ELECTION message to {member}: {e}")


    def send_coordinator_message(self, process_id, address):
        """sends COORDINATOR message to all members"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(address)
                message = ('COORDINATOR', self.process_id)  # Include the process_id of the coordinator
                s.send(pickle.dumps(message))
                print(f"Sent COORDINATOR message to {process_id} at {address}")
        except Exception as e:
            print(f"Error sending COORDINATOR message: {e}")
    
    def get_identity(self):
        """
        return the identity of the group member, which is the pair of
        (days until next birthday, SU ID)
        """
        return self.identity
    
    def connect_to_gcd(self, host, port):
        """
        connects to gcd server using a socket and sends "BEGIN" request
        receives a list of members from the gcd, and stores them in the
        group_members list 
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                # connect to GCD
                s.connect((host, port))
                
                # days_to_next_birthday = int(self.days_to_birthday(self.birthday))  # Ensure this returns an integer
                process_id = (int(self.days_to_birthday), int(self.su_id)) # Correct process_id format
                # listen_address = (host, port)
            
                # send initial 'BEGIN' message to the GCD           
                message = ('BEGIN', (process_id, self.listen_address)) ## ('BEGIN', ((days_to_bd, su_id), (host, port)))
                s.send(pickle.dumps(message))
                data = pickle.loads(s.recv(1024))
                # data: {(130, 1239587): ('127.0.0.1', 50423)}
                return data 

                
        except Exception as e:
            print(f"Error connecting to GCD server: {e}")
            return []
    



# initialize the Node or Group member class 
# connect to the GCD to retrieve the group members 
# start a thread to handle incoming messages (election or coordinator messages)
# start the election process when the node starts 
def main():
    print(f"Arguments received: {sys.argv}")
    if len(sys.argv) != 5:
        # the script will be 4,if one is missing and print message and exit
        print("Usage:\tpython3 lab2.py <host> <port> <su_id>")
        exit(1)

    host, port = sys.argv[1], int(sys.argv[2])
    su_id = int(sys.argv[3])
    days_to_birthday = int(sys.argv[4])

    # create an instance of the lab2 class with parsed arugments
    lab2 = Lab2(host, port, su_id, days_to_birthday)
    lab2.run()

    
    

if __name__ == "__main__":
    main()
