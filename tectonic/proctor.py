import subprocess
import signal
import pysigset


class Process(object):

    def __init__(self, *args, **kwargs):
        self.popen = None
        self._args, self._kwargs = args, kwargs
        self.command = self._args[0]

    @property
    def has_run(self):
        return self.popen is not None

    def restart(self):
        self.popen = subprocess.Popen(*self._args, **self._kwargs)
        return self.popen

    def __getattr__(self, attr):
        return getattr(self.popen, attr)

    def __repr__(self):
        cn = self.__class__.__name__
        args = ', '.join(repr(a) for a in self._args)
        kwargs = ', '.join('%s=%r' % kv for kv in self._kwargs.items())
        return '%s(%s%s)' % (cn, args, '' if not kwargs else ', ' + kwargs)


class Proctor(object):

    def __init__(self, commands=()):
        self.processes = [self.create_process(command) for command in commands]
        self.living = {}
        self.running = True

    @staticmethod
    def create_process(command):
        return Process(command)

    def reap(self, *args, **kwargs):
        return [self.living.pop(process.pid)
                for process in self.living.values()
                if process.poll() is not None]

    def shutdown(self, *args, **kwargs):
        self.running = False
        self.reap()
        for child in self.living.values():
            child.terminate()
        self.reap()
        for child in self.living.values():
            child.kill()
        self.reap()

    def respawn(self, missing=None):
        if missing is None:
            running = set(self.living.values())
            missing = set(self.processes) - running

        for process in missing:
            print process
            try:
                popen = process.restart()
            except OSError as e:
                # TODO handle on a per-process (group?) basis
                raise e
            self.living[popen.pid] = process

    def process_cycle(self, *args, **kwargs):
        self.respawn(self.reap())

    def run(self):
        signals = {signal.SIGCHLD: self.process_cycle,
                   signal.SIGTERM: self.shutdown,
                   signal.SIGINT: self.shutdown}

        with pysigset.suspended_signals(*signals.keys()):
            for signo, handler in signals.items():
                signal.signal(signo, handler)

            self.respawn()

        while self.running:
            pysigset.sigsuspend(pysigset.SIGSET())


if __name__ == '__main__':
    commands = (['sleep', '1'],
                ['sleep', '2'],
                ['sleep', '3'])
    proctor = Proctor(commands)
    proctor.run()
