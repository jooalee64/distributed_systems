import socket
import pickle

"""
summary of simple server
1. create a socket using socket()
2. bind the socket to a specific host and port with bind()
3. listen for incoming connections with listen()
4. accept an incoming connection with accept(), which creates a new socket 
for communication with the client 
5. send data to the client using sendall()
"""

def start_server(host='localhost', port=33313):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # before a server can accept incoming connections, it must specify 
        # the address and port where it will listen for incoming requests. 

        server_socket.bind((host, port)) # associates the socket with a specific IP address
        # and port number on the host machine

        server_socket.listen(1) # enables the server to accept incoming connectios
        # it tells the operating system that the server is ready to listen 
        # for connections on the specified port
        print(f'Server listening on {host}:{port}')
        
        while True:
            conn, addr = server_socket.accept() # accept an incoming connection 
            # when a client establishes a connection, this method returns a 
            # new socket object(for communication with the client) and the address
            # of the client 
            # client_socket, address = server_socket.accept()
            with conn:
                print(f'Connection from {addr}')
                data = pickle.loads(conn.recv(1024))
                if data == "HELLO":
                    response = ('OK', f"Happy to meet you, {addr}")
                    conn.sendall(pickle.dumps(response))
                    # send all - is used to send data to a connected socket. 
                    # it ensures that all the data is sent, handling any interruptions 
                    # or partial sends


if __name__ == "__main__":
    start_server()
