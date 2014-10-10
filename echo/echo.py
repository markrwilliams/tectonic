import os
from contextlib import closing
from tectonic.client import (request_tcp_listener,
                             start_stdout_stderr_rotation_thread)

start_stdout_stderr_rotation_thread(1)

bound_sock = request_tcp_listener(host='0.0.0.0', port=9998, listen=128)


while True:
    client, addr = bound_sock.accept()
    print 'ECHO', os.getpid(), 'accepted', addr
    with closing(client):
        while True:
            read = client.recv(1024)
            if not read:
                break
            client.sendall(read)
