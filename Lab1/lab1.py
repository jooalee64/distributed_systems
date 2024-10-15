import socket
import sys
import pickle 

"""
Jooa Lee
CPSC 5510 Lab1
Simple Client in a Client-Server System
"""

def connect_to_gcd(host, port):
    """ Initiates communication with Group Communication Daemon (GCD) server
    by sending the 'BEGIN' message.
    Connects to the GCD server hosted at the given host and port
    
    Args:
        host (str): The hostname of IP address of the GCD server (e.g. 'cs2.seattleu.edu')
        port (int): The port number to connect to on the GCD server
    
    Returns:
        list: A list of dictionaries, where each dictionary contains details of a group member.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.send(pickle.dumps("BEGIN"))
            print(f"BEGIN ({host}, {port})")
            data = pickle.loads(s.recv(1024))
            return data 
    except Exception as e:
        print(f"Error connecting to GCD server: {e}")
        return []


# creat a separate socket (peer) in the meet_members
# individual connections: each member of the group may be hosted on different machines or processes.
# by creating a new socket for each member, you establish an individual connection to each one, allowing for direct
# communication without interference from other connections  - this is essential in distributed systemes where group memebrs 
# operate independently
def connect_to_member(member):
    """ Sends a "HELLO" message specified group member and prints the response
    as a response
    Args:
        member (str): The identifier of the group member to send the 'HELLO' message to

    """
    # since return value was list of dictionary
    # should access member's host and port number with key,value pairs
    m_host = member['host']
    m_port = member['port']
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # set the connection timeout to 1500ms for the group members
        s.settimeout(1.5)
        try:
            s.connect((m_host, m_port)) #tuple 
            s.sendall(pickle.dumps("HELLO"))
            print(f"HELLO to {member}")
            # receive and print respnse from the group member
            # print('Received', repr(connect_to_member(member)))
            response = pickle.loads(s.recv(1024))
        except ConnectionRefusedError as e:
            print(f"Failed to connect to {member}: {e}")
            return
        except Exception as e:
            print(f"An error occurred: {e}")
            return
    print(response)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python lab1.py <host> <port>")
        exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])

    # get group members from gcd servers
    group_members = connect_to_gcd(host, port)
    if group_members:
        for member in group_members:
            connect_to_member(member)    
    else:
        print("No group members received.")
