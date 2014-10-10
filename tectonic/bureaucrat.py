import argparse
from contextlib import closing
import traceback
import socket
from . import messages
from .client import BUREAUCRAT_PATH
from .log_rotate import (LogRotation,
                         bureaucrat_log_directory,
                         workers_log_directory,
                         ensure_log_directories,
                         start_stdouterr_rotation_thread,
                         open_log_fd)
from passage.connections import bind
from passage.way import Passageway
import threading
import time
import signal


class Bureaucrat(object):

    def __init__(self, log_dir, rotate_interval=30, path=BUREAUCRAT_PATH):
        self.sock = None
        self.log_dir = log_dir
        self.path = path
        self.rotate_interval = rotate_interval
        self.passage_way = Passageway([messages.WorkerStandardPairMessage()])
        self.tcp_listeners = {}
        self.channels = {}
        self.worker_std_pair = None
        self.running = True

    def bind(self, path=None):
        if path is None:
            path = self.path
        self.sock = bind(path)

    def _handle_request(self, client):
        try:
            with closing(client):
                request, pid = messages._recvall(client)
                print 'BUREAUCRAT heard that', pid, 'wants', request
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

    def _handle_WantWorkerStandardPair(self, client, request):
        messages._sendall(client, messages.HaveWorkerStandardPair(True))
        self.passage_way.transfer(client, self.worker_std_pair)

    def setup_worker_logs(self):
        stdout_path = workers_log_directory(self.log_dir, 'stdout')
        stderr_path = workers_log_directory(self.log_dir, 'stderr')

        self.worker_std_pair = messages.WorkerStandardPair(
            stdout=open_log_fd(stdout_path),
            stderr=open_log_fd(stderr_path))

        self.worker_stdout_rotator = LogRotation(
            path=stdout_path, fd=self.worker_std_pair.stdout,
            max_size=1024)

        self.worker_stderr_rotator = LogRotation(
            path=stderr_path, fd=self.worker_std_pair.stderr,
            max_size=1024)

        def monitor():
            while True:
                stdout, stderr = None, None
                try:
                    stdout = self.worker_stdout_rotator.rotate()
                except:
                    traceback.print_exc()
                try:
                    stderr = self.worker_stderr_rotator.rotate()
                except:
                    traceback.print_exc()

                if stdout and stderr:
                    self.worker_std_pair = messages.WorkerStandardPair(
                        stdout, stderr)

                time.sleep(self.rotate_interval)

        self.worker_rotator_thread = threading.Thread(target=monitor)
        self.worker_rotator_thread.daemon = True
        self.worker_rotator_thread.start()

    def shutdown(self, *args, **kwargs):
        self.running = False

    def listen(self):
        signal.signal(signal.SIGTERM, self.shutdown)
        logs = bureaucrat_log_directory(self.log_dir)
        self.rotator_thread = start_stdouterr_rotation_thread(logs)
        self.setup_worker_logs()
        while self.running:
            client, _ = self.sock.accept()
            self._handle_request(client)


argument_parser = argparse.ArgumentParser(
    description='The Bureaucrat manages shared resources such'
    ' as log files and sockets')
argument_parser.add_argument('--log-dir', '-l',
                             default='logs',
                             help='where to open logs')
argument_parser.add_argument('--rotate-interval', '-r',
                             type=int,
                             help='how often to rotate logs',
                             default=1)


if __name__ == '__main__':
    args = argument_parser.parse_args()
    ensure_log_directories(args.log_dir)
    b = Bureaucrat(log_dir=args.log_dir, rotate_interval=args.rotate_interval)
    b.bind()
    b.listen()
