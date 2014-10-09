import os


MAX_SIZE = 2 ** 32
ITERATIONS = 8
# todo - honor umask?
MODE = 0100644


def open_log_fd(path, mode=MODE):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_APPEND
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
    def __init__(self, log, max_size=MAX_SIZE, iterations=ITERATIONS,
                 mode=MODE):
        if isinstance(log, file):
            self.path = log.name
            self.target = log
            self._log = log     # keep a reference so the file doesn't
                                # disappear
        elif isinstance(log, basestring):
            self.path = log
            self.target = None
        else:
            raise ValueError("log must be string or file object")

        if not os.path.isfile(self.path):
            raise IOError("log file does not exist")

        self.mode = mode
        if os.stat(self.path).st_mode != self.mode:
            os.chmod(self.path, self.mode)

        self.max_size = max_size
        self.iterations = iterations

    def monitor(self):
        rotated = rotate_path(self.path, self.max_size, self.iterations)
        new_fd = None
        if rotated:
            new_fd = needs_close = open_log_fd(self.path, self.mode)
            if self.target is not None:
                try:
                    os.dup2(new_fd, self.target.fileno())
                    new_fd = self.target.fileno()
                finally:
                    os.close(needs_close)
        return new_fd
