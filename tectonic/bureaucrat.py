from collections import namedtuple
from contextlib import closing
import traceback
import socket
from . import messages
from passage.connections import bind, connect
from passage.way import Passageway


BUREAUCRAT_PATH = 'bureaucrat.sock'


def request_tcp_listener(host, port, listen, path=BUREAUCRAT_PATH):
    with closing(connect(path)) as uds:
        passage_way = Passageway()

        request = messages.WantTCPListener(host, port, listen)
        messages._sendall(uds, request)

        response, _ = messages._recvall(uds)

        assert isinstance(response, messages.HaveTCPListener)
        assert response == (host, port)

        return passage_way.obtain(uds, socket.socket)


def request_channel(identity, partner, path=BUREAUCRAT_PATH):
    with closing(connect(path)) as uds:
        passage_way = Passageway()

        messages._sendall(uds,
                          messages.WantChannel(identity, partner))

        response, _ = messages._recvall(uds)

        assert isinstance(response, messages.HaveChannel)
        assert response == (identity, partner)
        return passage_way.obtain(uds, socket.socket)


class Bureaucrat(object):

    def __init__(self, path=BUREAUCRAT_PATH):
        self.sock = None
        self.path = path
        self.passage_way = Passageway()
        self.tcp_listeners = {}
        self.channels = {}

    def bind(self, path=None):
        if path is None:
            path = self.path
        self.sock = bind(path)

    def _handle_request(self, client):
        try:
            with closing(client):
                request, pid = messages._recvall(client)
                print 'BUREAUCRAT heard from', pid
                handler_name = '_handle_' + request.__class__.__name__
                handler = getattr(self, handler_name)
                handler(client, request)
        except Exception:
            traceback.print_exc()

    def _handle_WantTCPListener(self, client, request):
        host, port, listen = request
        sock = self.tcp_listeners.get(request)
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen(listen)
            self.tcp_listeners[request] = sock

        response = messages.HaveTCPListener(host, port)
        messages._sendall(client, response)
        self.passage_way.transfer(client, sock)

    def _handle_WantChannel(self, client, request):
        pair = self.channels.get(request.normalized)
        if pair is None:
            pair = dict(zip(request, socket.socketpair()))
            self.channels[request.normalized] = pair
        partner = pair[request.partner]
        messages._sendall(client, messages.HaveChannel(*request))
        self.passage_way.transfer(client, partner)

    def listen(self):
        while True:
            client, _ = self.sock.accept()
            self._handle_request(client)


if __name__ == '__main__':
    b = Bureaucrat()
    b.bind()
    b.listen()
