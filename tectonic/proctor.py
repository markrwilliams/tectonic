import argparse
import os
import subprocess
import signal
import pysigset
import multiprocessing
from passage.way import Passageway
from .log_rotate import (StandardOutRotation,
                         StandardErrorRotation,
                         ensure_log_directories,
                         proctor_log_directory,
                         bureaucrat_log_directory,
                         start_stdouterr_rotation_thread)
from . import messages
from . import client


def notify(polled):
    notify_fd = os.environ.pop('BUREAUCRAT_LAUNCH_PIPE', None)
    if notify_fd:
        notify_fd = int(notify_fd)
        status = '0' if polled is None else '1'
        os.write(notify_fd, status)
        os.close(notify_fd)


def clear_sigprocmask():
    pysigset.sigprocmask(pysigset.SIG_SETMASK, pysigset.SIGSET(), 0)


class Process(object):

    def __init__(self, *args, **kwargs):
        self.popen = None
        self._args, self._kwargs = args, kwargs
        self._kwargs['preexec_fn'] = clear_sigprocmask
        self.command = self._args[0]

    def on_restart(self):
        None

    @property
    def has_run(self):
        return self.popen is not None

    def restart(self):
        self.on_restart()
        self.popen = subprocess.Popen(*self._args, **self._kwargs)
        return self.popen

    def __getattr__(self, attr):
        return getattr(self.popen, attr)

    def __repr__(self):
        cn = self.__class__.__name__
        args = ', '.join(repr(a) for a in self._args)
        kwargs = ', '.join('%s=%r' % kv for kv in self._kwargs.items())
        return '%s(%s%s)' % (cn, args, '' if not kwargs else ', ' + kwargs)


class WorkerProcess(Process):
    passage_way = Passageway([messages.WorkerStandardPairMessage()])

    def on_restart(self):
        with pysigset.suspended_signals(signal.SIGCHLD):
            pw = self.passage_way
            std_pair = client.request_worker_stdpair(passage_way=pw)

        stdout = self._kwargs.pop('stdout', None)
        stderr = self._kwargs.pop('stderr', None)

        if stdout is not None:
            os.close(stdout)
        if stderr is not None:
            os.close(stderr)

        self._kwargs['stdout'] = std_pair.stdout
        self._kwargs['stderr'] = std_pair.stderr


class Proctor(object):
    BUREAUCRAT_INVOCATION = ['python', '-u', '-m', 'tectonic.bureaucrat']

    def __init__(self, commands=(), log_dir=None):
        self.living = {}
        self.running = True
        self.dev_null = open('/dev/null')
        self.passage_way = Passageway([messages.WorkerStandardPairMessage()])
        self.log_dir = log_dir

        self.bureaucrat = self.create_bureaucrat()

        self.processes = [self.bureaucrat]
        self.processes.extend(self.create_worker(command)
                              for command in commands)

    def create_bureaucrat(self):
        # we don't need to try and collect stdout & error from the
        # bureaucrat here, because we only spawn it once -- if it goes
        # down, we die.  at some point we can build redundancy into this.
        bureaucrat_logs = bureaucrat_log_directory(self.log_dir)
        stdout = StandardOutRotation(directory=bureaucrat_logs,
                                     no_fd=True).reopen()
        stderr = StandardErrorRotation(directory=bureaucrat_logs,
                                       no_fd=True).reopen()
        return Process(self.BUREAUCRAT_INVOCATION,
                       stdin=self.dev_null,
                       stdout=stdout,
                       stderr=stderr)

    def create_worker(self, command):
        return WorkerProcess(command, stdin=self.dev_null)

    def reap(self, *args, **kwargs):
        return [self.living.pop(process.pid)
                for process in self.living.values()
                if process.poll() is not None]

    def shutdown(self, *args, **kwargs):
        with pysigset.suspended_signals(signal.SIGCHLD):
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        self.running = False
        self.reap()
        for child in self.living.values():
            child.terminate()
        self.reap()
        for child in self.living.values():
            child.kill()
        self.reap()
        if os.path.exists(client.BUREAUCRAT_PATH):
            os.unlink(client.BUREAUCRAT_PATH)

    def respawn(self, missing=None):
        if missing is None:
            running = set(self.living.values())
            missing = set(self.processes) - running

        for process in missing:
            try:
                popen = process.restart()
            except OSError as e:
                # TODO handle on a per-process (group?) basis
                raise e
            self.living[popen.pid] = process

    def process_cycle(self, *args, **kwargs):
        dead = self.reap()
        if self.bureaucrat in dead:
            self.shutdown()
            return
        self.respawn(dead)

    def run(self):
        signals = {signal.SIGCHLD: self.process_cycle,
                   signal.SIGTERM: self.shutdown,
                   signal.SIGINT: self.shutdown}

        with pysigset.suspended_signals(*signals.keys()):
            logs = proctor_log_directory(self.log_dir)
            self.rotator_thread = start_stdouterr_rotation_thread(logs)

            for signo, handler in signals.items():
                signal.signal(signo, handler)

            self.bureaucrat.restart()
            self.living[self.bureaucrat.pid] = self.bureaucrat

            poll = self.bureaucrat.poll()
            notify(poll)
            if poll is not None:
                raise RuntimeError("Bureaucrat failed to start!")

            self.respawn()

        while self.running:
            pysigset.sigsuspend(pysigset.SIGSET())

        self.shutdown()


argument_parser = argparse.ArgumentParser(
    description='The Proctor process monitors and respawns'
    ' the Bureaucrat and workers.')
argument_parser.add_argument('worker_invocation',
                             nargs='+',
                             help='command to spawn workers')
argument_parser.add_argument('--number', '-n',
                             type=int,
                             default=multiprocessing.cpu_count(),
                             help='number of workers')
argument_parser.add_argument('--log-dir', '-l',
                             default='logs',
                             help='directory in which to store aggregated'
                             ' standard out and error logs for children')


if __name__ == '__main__':
    args = argument_parser.parse_args()
    ensure_log_directories(args.log_dir)
    commands = [args.worker_invocation] * args.number
    proctor = Proctor(commands, log_dir=args.log_dir)

    proctor.run()
