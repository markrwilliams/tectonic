import sys
import os
import traceback
import time
import threading
import random


MAX_SIZE = 2 ** 32
ITERATIONS = 8
# todo - honor umask?
MODE = 0100644


def open_log_fd(path, mode=MODE):
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
    return os.open(path, flags, mode)


def rotate_path(path, max_size=MAX_SIZE, iterations=ITERATIONS):
    fmt = '%s.%d'
    if os.path.getsize(path) > max_size:
        for target_num in xrange(iterations, 1, -1):
            source_num = target_num - 1
            source, target = fmt % (path, source_num), fmt % (path, target_num)
            if os.path.exists(source):
                os.rename(source, target)
        if os.path.exists(path):
            os.rename(path, fmt % (path, 1))
        return True
    return False


class LogRotation(object):
    def __init__(self, path, fd=None,
                 max_size=MAX_SIZE, iterations=ITERATIONS, mode=MODE):
        self.path = path
        self.fd = fd
        self.max_size = max_size
        self.iterations = iterations
        self.mode = mode

        if not os.path.isfile(self.path):
            self.reopen()

        if os.stat(self.path).st_mode != self.mode:
            os.chmod(self.path, self.mode)

    def reopen(self):
        new_fd = open_log_fd(self.path, self.mode)
        if self.fd is not None:
            needs_close = new_fd
            try:
                os.dup2(new_fd, self.fd)
                new_fd = self.fd
            finally:
                os.close(needs_close)
        return new_fd

    def rotate(self):
        rotated = rotate_path(self.path, self.max_size, self.iterations)
        new_fd = None
        if rotated:
            new_fd = self.reopen()
        return new_fd


NO_FD = 'NO_FD'


class StandardRotation(LogRotation):
    name = None
    fd = None

    def __init__(self, directory, no_fd=False,
                 max_size=MAX_SIZE, iterations=ITERATIONS, mode=MODE):
        fd = None if no_fd else self.fd
        super(StandardRotation, self).__init__(path=os.path.join(directory,
                                                                 self.name),
                                               fd=fd)


class StandardOutRotation(StandardRotation):
    name = 'stdout'
    fd = 1


class StandardErrorRotation(StandardRotation):
    name = 'stderr'
    fd = 2


def ensure_directory(path):
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            traceback.print_exc()
            msg = 'Could not create log dir: %r' % path
            print >> sys.stderr, msg
            return False
        else:
            return True
    elif not os.access(path, os.W_OK | os.R_OK | os.X_OK):
        msg = ('Insufficient permissions for log dir: %r.'
               ' Need read, write and execute (list).' % path)
        print >> sys.stderr, msg
        return False
    else:
        return True


def proctor_log_directory(log_dir, *args):
    return os.path.join(log_dir, 'proctor', *args)


def bureaucrat_log_directory(log_dir, *args):
    return os.path.join(log_dir, 'bureaucrat', *args)


def workers_log_directory(log_dir, *args):
    return os.path.join(log_dir, 'workers', *args)


def ensure_log_directories(log_dir):
    return all(ensure_directory(make_directory(log_dir))
               for make_directory in (proctor_log_directory,
                                      bureaucrat_log_directory,
                                      workers_log_directory))


def start_stdouterr_rotation_thread(directory, interval=30):
    '''\
    To be used in the bureaucrat or the proctor!
    '''
    # some jitter
    interval += random.random()
    stdout_rotator = StandardOutRotation(directory=directory)
    stderr_rotator = StandardErrorRotation(directory=directory)
    stdout_rotator.reopen()
    stderr_rotator.reopen()

    def monitor():
        while True:
            try:
                stdout_rotator.rotate()
            except:
                traceback.print_exc()
            try:
                stderr_rotator.rotate()
            except:
                traceback.print_exc()

            time.sleep(interval)

    rotation_thread = threading.Thread(target=monitor)
    rotation_thread.daemon = True
    rotation_thread.start()
    return rotation_thread
