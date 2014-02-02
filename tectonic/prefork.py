import errno
import os
import multiprocessing
import socket
import signal
import sys
import pysigset
import gevent

# ugh
IGNORE_SIGS = ('SIGKILL', 'SIGSTOP', 'SIG_DFL', 'SIG_IGN')
SIGNO_TO_NAME = {no: name for name, no in signal.__dict__.iteritems()
                 if name.startswith('SIG')
                 and name not in IGNORE_SIGS}
DEFAULT_SIGNAL_HANDLERS = {signo: signal.getsignal(signo)
                           for signo in SIGNO_TO_NAME}


class Master(object):
    BACKLOG = 128

    def __init__(self, server_class, socket_factory, wsgi, address, logpath,
                 pidfile='prefork.pid', num_workers=None):
        self.server_class = server_class
        self.socket_factory = socket_factory
        self.wsgi = wsgi
        self.address = address
        self.logpath = logpath
        self.pidfile = pidfile

        self.listener = None
        if num_workers is None:
            num_workers = multiprocessing.cpu_count()
        self.num_workers = num_workers
        self.children = set()

    def log(self):
        self.logfile = open(self.logpath, 'w')

    def bind(self):
        self.listener = self.socket_factory()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(self.address)
        self.listener.listen(self.BACKLOG)

    def daemonize(self):
        # for steps see TLPI 37.2
        if os.fork():
            # 1
            sys.exit(0)
        # 2
        os.setsid()
        # 3
        if os.fork():
            sys.exit(0)
        # 4
        os.umask(0)
        # 5 -- skip for now
        # os.chdir('/')
        # 6/7
        os.dup2(self.logfile.fileno(), 0)
        os.dup2(self.logfile.fileno(), 1)
        os.dup2(self.logfile.fileno(), 2)

        with open(self.pidfile, 'w') as f:
            f.write(str(os.getpid()))

    def spawn_worker(self):
        pid = os.fork()
        if pid:
            return pid
        self.set_signal_handlers(DEFAULT_SIGNAL_HANDLERS)
        self.server.serve_forever()
        sys.exit(0)

    def spawn_workers(self, number):
        for _ in xrange(number):
            self.children.add(self.spawn_worker())

    def set_signal_handlers(self, signal_handlers):
        return {signo: signal.signal(signo, handler)
                for signo, handler in signal_handlers.iteritems()}

    def master_signals(self):

        def handler(signo, frame):
            handler_name = SIGNO_TO_NAME[signo] + '_handler'
            handler_meth = getattr(self, handler_name, None)
            if handler_meth:
                handler_meth(signo, frame)

        return self.set_signal_handlers({signo: handler
                                         for signo, name
                                         in SIGNO_TO_NAME.iteritems()})

    def run(self, daemonize=True):
        self.bind()
        if daemonize:
            self.log()
            self.daemonize()

        self.master_signals()
        self.server = self.server_class(self.listener, self.wsgi)
        self.spawn_workers(self.num_workers)

        while True:
            pysigset.sigsuspend(pysigset.SIGSET())
            if self.num_workers > len(self.children):
                self.spawn_workers(self.num_workers - len(self.children))

    def SIGCLD_handler(self, signo, frame):
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
                self.children.remove(pid)
            except OSError as e:
                if e.errno == errno.ECHILD:
                    break

    SIGCHLD_handler = SIGCLD_handler

    def SIGTERM_handler(self, signo, frame):
        for child in self.children:
            try:
                os.kill(child, signal.SIGTERM)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    continue
        while True:
            try:
                os.wait()
            except OSError as e:
                if e.errno == errno.ECHILD:
                    break
        sys.exit(0)

    SIGINT_handler = SIGTERM_handler


if __name__ == '__main__':
    import argparse
    import gevent.pywsgi

    a = argparse.ArgumentParser()
    a.add_argument('address')
    a.add_argument('port', type=int)
    a.add_argument('--logpath', default='log')
    a.add_argument('--pidfile', default='pidfile')
    a.add_argument('--daemonize', '-d', default=False, action='store_true')

    def wsgi(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/html')])
        pid = str(os.getpid())
        return ['<html><body><h1>ok</h1><br/>from ' + pid]

    args = a.parse_args()

    Master(server_class=gevent.pywsgi.WSGIServer,
           socket_factory=gevent.socket.socket,
           wsgi=wsgi,
           address=(args.address, args.port),
           logpath=args.logpath,
           pidfile=args.pidfile).run(args.daemonize)
