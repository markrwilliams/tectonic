import errno
import fcntl
import os
import multiprocessing
import socket
import signal
import select
import sys
import resource
import time
import gevent

IGNORE_SIGS = ('SIGKILL', 'SIGSTOP', 'SIG_DFL', 'SIG_IGN')
SIGNO_TO_NAME = dict((no, name) for name, no in signal.__dict__.iteritems()
                     if name.startswith('SIG')
                     and name not in IGNORE_SIGS)
DEFAULT_SIGNAL_HANDLERS = dict((signo, signal.getsignal(signo))
                               for signo in SIGNO_TO_NAME)


def set_nonblocking(*fds):
    for fd in fds:
        fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK)
    return fds


def _ignore_interrupts(e):
    try:
        en, _ = e.args
    except ValueError:
        # This can happen in certain cases where the error only
        # has one piece
        raise e
    if en not in (errno.EINTR, errno.EAGAIN):
        raise e


def safe_syscall(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        _ignore_interrupts(e)


def restart_syscall(func, *args, **kwargs):
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            _ignore_interrupts(e)


class WriteAndFlushFile(file):

    def write(self, str):
        full = len(str) == os.write(self.fileno(), str)
        self.flush()
        return full

    def writelines(self, sequence_of_strings):
        return self.write(''.join(sequence_of_strings))


class WorkerMetadata(object):

    def __init__(self, pid, health_check_read, last_seen):
        self.pid = pid
        self.health_check_read = health_check_read
        self.last_seen = last_seen


class Master(object):
    BACKLOG = 128
    DEFAULT_NUM_WORKERS = multiprocessing.cpu_count() - 1
    CHILD_HEALTH_INTERVAL = 1.0
    SELECT_TIMEOUT = CHILD_HEALTH_INTERVAL * 5
    MURDER_WAIT = 30
    PLATFORM_RSS_MULTIPLIER = 1
    PROC_FDS = '/proc/self/fd'

    def __init__(self, server_class, socket_factory, sleep, wsgi, address,
                 logpath, pidfile, num_workers=None):
        self.server_class = server_class
        self.socket_factory = socket_factory
        self.sleep = sleep
        self.wsgi = wsgi
        self.address = address
        self.logpath = logpath
        self.pidfile = pidfile

        self.listener = None
        if num_workers is None:
            num_workers = self.DEFAULT_NUM_WORKERS
        self.num_workers = num_workers
        self.pid_to_workers = {}
        self.pipe_to_workers = {}

    def add_worker(self, w):
        self.pid_to_workers[w.pid] = w
        self.pipe_to_workers[w.health_check_read] = w

    def remove_worker(self, w):
        # we may have gotten interrupted by a signal
        if w:
            self.pid_to_workers.pop(w.pid, None)
            self.pipe_to_workers.pop(w.health_check_read, None)

    def log(self):
        self.logfile = WriteAndFlushFile(self.logpath, 'ab')

    def bind(self):
        self.listener = self.socket_factory()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(self.address)
        self.listener.listen(self.BACKLOG)

    def selfpipes(self):
        # r, w
        self.pipe_select, self.pipe_signal = set_nonblocking(*os.pipe())

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
        fd = os.open('/dev/null', os.O_RDWR)
        os.dup2(fd, 0)
        os.dup2(self.logfile.fileno(), 1)
        os.dup2(self.logfile.fileno(), 2)

        with open(self.pidfile, 'w') as f:
            f.write(str(os.getpid()))

    def health_check(self, fd_limit, maxrss_limit):
        # parent alive?
        if os.getppid() == 1:
            # we were orphaned and adopted by init
            sys.stderr.write('parent died!\n')
            sys.exit(1)
        # memory usage?
        usage = resource.getrusage(resource.RUSAGE_SELF)

        memory_usage = usage.ru_maxrss * self.PLATFORM_RSS_MULTIPLIER
        if memory_usage > maxrss_limit:
            sys.stderr.write('memory usage exceeded: %s\n' % memory_usage)
            sys.exit(1)

        fd_count = len(os.listdir(self.PROC_FDS))
        if fd_count > fd_limit - 10 or fd_count > fd_limit * 0.9:
            sys.stderr.write('file limit too close to limit %s\n' % fd_count)
            sys.exit(1)

    def spawn_worker(self):
        health_check_read, health_check_write = set_nonblocking(*os.pipe())
        pid = os.fork()
        if pid:
            return WorkerMetadata(pid=pid,
                                  health_check_read=health_check_read,
                                  last_seen=time.time())

        self.set_signal_handlers(DEFAULT_SIGNAL_HANDLERS)
        self.server.start()

        nofile_soft_limit = max(resource.getrlimit(resource.RLIMIT_NOFILE)[0],
                                1024)
        maxrss_soft_limit = max(resource.getrlimit(resource.RLIMIT_RSS)[0],
                                2 ** 30)

        while True:
            self.health_check(nofile_soft_limit, maxrss_soft_limit)
            os.write(health_check_write, '\x00')
            self.sleep(self.CHILD_HEALTH_INTERVAL)

        sys.exit(0)

    def spawn_workers(self, number):
        for _ in xrange(number):
            self.add_worker(self.spawn_worker())

    def kill_workers(self, pids):
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    continue
            self.remove_worker(self.pid_to_workers.get(pid))

    def set_signal_handlers(self, signal_handlers):
        return dict((signo, signal.signal(signo, handler))
                    for signo, handler in signal_handlers.iteritems())

    def master_signals(self):

        def handler(signo, frame):
            safe_syscall(os.write, self.pipe_signal, chr(signo))

        handlers = dict((signo, handler)
                        for signo, name in SIGNO_TO_NAME.iteritems())
        return self.set_signal_handlers(handlers)

    def handle_signals(self, signos):
        for signo in signos:
            signo = ord(signo)
            handler_name = SIGNO_TO_NAME[signo] + '_handler'
            handler_meth = getattr(self, handler_name, None)
            if handler_meth:
                # no frame, sorry
                handler_meth(signo, None)

    def run(self, daemonize=True):
        self.bind()

        if daemonize:
            self.log()
            self.daemonize()

        self.selfpipes()
        self.master_signals()

        self.server = self.server_class(self.listener, self.wsgi)
        self.spawn_workers(self.num_workers)

        while True:
            read = [c.health_check_read for c in self.pid_to_workers.values()]
            read.append(self.pipe_select)
            read, write, exc = restart_syscall(select.select, read, [], [],
                                               self.SELECT_TIMEOUT)
            now = time.time()

            for r in read:
                if r == self.pipe_select:
                    self.handle_signals(os.read(r, 4096))
                    continue
                os.read(r, 4096)
                worker = self.pipe_to_workers.get(r)
                if not worker:
                    continue
                worker.last_seen = now

            self.kill_workers(w.pid for w in self.pid_to_workers.values()
                              if now - w.last_seen >= self.MURDER_WAIT)

            if self.num_workers > len(self.pid_to_workers):
                self.spawn_workers(self.num_workers - len(self.pid_to_workers))

    def SIGCLD_handler(self, signo, frame):
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if not pid:
                    break
                self.remove_worker(self.pid_to_workers.get(pid))
            except OSError as e:
                if e.errno == errno.ECHILD:
                    break

    SIGCHLD_handler = SIGCLD_handler

    def SIGTERM_handler(self, signo, frame):
        for child in self.pid_to_workers:
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

    import string
    chrs = string.lowercase[:Master.DEFAULT_NUM_WORKERS]

    def wsgi(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/html')])
        pid = os.getpid()
        spid = str(pid)
        sys.stderr.write('''\
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus
eleifend a metus quis sollicitudin. Aenean nec dolor iaculis, rhoncus
turpis sit amet, interdum quam. Nunc rhoncus magna a leo interdum
luctus. Vestibulum nec sapien diam. Aliquam rutrum venenatis
mattis. Etiam eget adipiscing risus. Vestibulum ante ipsum primis in
faucibus orci luctus et ultrices posuere cubilia Curae; Fusce nibh
nulla, lacinia quis dignissim vel, condimentum at odio. Nunc et diam
mauris. Fusce sit amet odio sagittis, convallis urna a, blandit
urna. Phasellus mattis ligula sed tincidunt pellentesque. Nullam
tempor convallis dapibus.

Duis vitae vulputate sem, nec eleifend orci. Donec vel metus
fringilla, ultricies nunc at, ultrices quam. Donec placerat nisi quis
fringilla facilisis. Fusce eget erat ut magna consectetur
elementum. Aenean non vulputate nulla. Aliquam eu dui nibh. Vivamus
mollis suscipit neque, quis aliquam ipsum auctor non. Nulla cursus
turpis turpis, nec euismod urna placerat at. Nunc id sapien
nibh. Vestibulum condimentum luctus placerat. Donec vitae posuere
arcu.''' + '\n')
        return ['<html><body><h1>ok</h1><br/>from ' + spid]

    args = a.parse_args()

    Master(server_class=gevent.pywsgi.WSGIServer,
           socket_factory=gevent.socket.socket,
           sleep=gevent.sleep,
           wsgi=wsgi,
           address=(args.address, args.port),
           logpath=args.logpath,
           pidfile=args.pidfile).run(args.daemonize)
