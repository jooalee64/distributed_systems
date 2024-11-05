# Echo client program
import socket
import sys

if len(sys.argv) != 3:
    print("Usage: python client.py HOST PORT")
    exit(1)
    
host = sys.argv[1]
port = int(sys.argv[2])

# typeofsocket, protocol, socket.SOCK_STREAM) as s:
# socket.socket(socket.AF_INET, )
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # host and port
    # host - refers to any device that is connected to a network and can send
    # or receive data. (includes computers, servers, routers, and even IoT devices)
    # port - numerical identifier 
    s.connect((host, port)) # connect with socket(연결단자느낌)
    s.sendall(b'Hello, world')
    data = s.recv(1024)
print('Received', repr(data))