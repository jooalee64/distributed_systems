import socket
server_address = ('localhost', 10000)
print('starting up on {} port {}'.format(*server_address))

# create a UDP socket
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.bind(server_address) # subscriber binds the socket to the publisher's address
    while True:
        print('\nblocking, waiting to receive message')
        data = sock.recv(4096)
        print('received {} bytes'.format(len(data)))
        print(data)
