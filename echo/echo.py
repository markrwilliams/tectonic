import os
import socket
from contextlib import closing
from tectonic.bureaucrat import request_socket

bound_sock = request_socket(socket.AF_INET, socket.SOCK_STREAM, 0,
                            '0.0.0.0', 9998)


while True:
    client, addr = bound_sock.accept()
    print 'ECHO', os.getpid(), 'accepted', addr
    with closing(client):
        while True:
            read = client.recv(1024)
            if not read:
                break
            client.sendall(read)
