import socket
import time 

# create a UDP socket
"""
using UDP socket, UDP is connectionless protocol, meaning it does not requires a connection to be established before data can be sent

"""
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
for n in range(10_000):
    message = 'message {}'.format(n).encode('utf-8') # encoding the message to bytes
    print('sending {!r} (even if nobody is listening)'.format(message))
    print(sock) # printing the socket object
    time.sleep(1.0) # pausing for 1 second between messages



