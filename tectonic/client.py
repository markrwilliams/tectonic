from contextlib import closing
import socket
from passage.way import Passageway
from passage.connections import connect
from . import messages
import os
import threading
import time
import random
import stat


BUREAUCRAT_PATH = 'bureaucrat.sock'


def sleep_until_ready(path):
    while not (os.path.exists(path)
               and stat.S_ISSOCK(os.stat(path).st_mode)):
        time.sleep(.1)


def request_tcp_listener(host, port, listen, path=BUREAUCRAT_PATH):
    sleep_until_ready(path)
    with closing(connect(path)) as uds:
        passage_way = Passageway()

        request = messages.WantTCPListener(host, port, listen)
        messages._sendall(uds, request)

        response, _ = messages._recvall(uds)

        assert isinstance(response, messages.HaveTCPListener)
        assert response == (host, port)

        return passage_way.obtain(uds, socket.socket)


def request_channel(identity, partner, path=BUREAUCRAT_PATH):
    sleep_until_ready(path)
    with closing(connect(path)) as uds:
        passage_way = Passageway()

        messages._sendall(uds,
                          messages.WantChannel(identity, partner))

        response, _ = messages._recvall(uds)

        assert isinstance(response, messages.HaveChannel)
        assert response == (identity, partner)
        return passage_way.obtain(uds, socket.socket)


def request_worker_stdpair(path=BUREAUCRAT_PATH, passage_way=None):
    sleep_until_ready(path)
    if passage_way is None:
        passage_way = Passageway([messages.WorkerStandardPairMessage()])

    with closing(connect(path)) as uds:
        messages._sendall(uds,
                          messages.WantWorkerStandardPair(True))

        response, _ = messages._recvall(uds)

        assert isinstance(response, messages.HaveWorkerStandardPair)
        return passage_way.obtain(uds, messages.WorkerStandardPair)


def start_stdout_stderr_rotation_thread(interval=30):
    # some jitter
    interval += random.random()

    def monitor():
        while True:
            stdpair = request_worker_stdpair()
            os.dup2(stdpair.stdout, 1)
            os.dup2(stdpair.stderr, 2)
            os.close(stdpair.stdout)
            os.close(stdpair.stderr)
            time.sleep(interval)

    rotation_thread = threading.Thread(target=monitor)
    rotation_thread.daemon = True
    rotation_thread.start()
    return rotation_thread
