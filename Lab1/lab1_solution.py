import pickle
import socket
import sys 

class Lab1(object):
    """
    class to perform the specified behavior for lab1 
    a simple client that connects a Group Coordinator Daemon (GCD) which 
    responds with a list of potential group members. this client then sends 
    a message to each of the group members, prints out their response, then exits
    """

    def __init__(self, gcd_host, gcd_port):
        """
        construct a lab1 object to talk to the given group coordinator daemon
        :param gcd_host: host name of GCD
        :param gcd_port: port number of GCD
        """
        self.host = gcd_host
        self.port = int(gcd_port)
        self.members = []
        self.peer_timeout = 1.5 


    def join_group(self):
        """
        Does what is specified for lab1 -- send a pickled 'BEGIN'
        and expect a pickled list of members in return 
        list is of members, with 'host' and 'port' keys filled in. 
        also verbosely prints out what it is going on 
        """
        # create socket communicate with gcd daemon
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gcd:
            address = (self.host, self.port)
            print('BEGIN {}'.format(address))
            gcd.connect(address) # connect socket to gcd address 
            self.members = self.message(gcd, 'BEGIN') #send begin message to GCD with gcd socket (from client) 
            # get returned group_members

    @staticmethod
    def message(sock, msg, buffer_size=1024):
        """
        pickles data and sends the given message to the given socket and unpickles the returned value and returns it
        :param sock: socket to send messag/recv
        :param send_data: message data (anyting pickle-able)
        :param buffer_size: number of bytes in receive buffer 
        :return: message response (unpiled--pickled data must fit in)
        """
        sock.sendall(pickle.dumps(msg))
        return pickle.loads(sock.recv(buffer_size))
    
    def meet_members(self):
        """
        sends a pickled 'HELLO' to all group members
        also verbosely prints out what it is going on./doing
        """
        for member in self.members:
            print('HELLO to {}'.format(member))
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer:
                peer.settimeout(self.peer_timeout)
                try:
                    peer.connect((member['host'], member['port']))
                except Exception as err:
                    print('failed to connect: {}', err)

                else:
                    message = self.message(peer, 'HELLO')
                    print(message)    

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python lab1.py GCDHOST GCDPORT")
        exit(1)
    host, port = sys.argv[1:]
    lab1 = Lab1(host, port)
    lab1.join_group()



