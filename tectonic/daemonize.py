import argparse
import os
import errno
import signal
import select
import sys
import socket
import subprocess
import traceback
import time

from .log_rotate import (StandardOutRotation,
                         StandardErrorRotation,
                         ensure_log_directories,
                         proctor_log_directory)


def eintr_retry(func, *args):
    while True:
        try:
            return func(*args)
        except (OSError, IOError, select.error) as e:
            if e[0] == errno.EINTR:
                continue
            raise


def wait_for_bureacrat(parent_socket, child_pid, sigchld_pipe, log_dir,
                       timeout=1):
    readset = [parent_socket, sigchld_pipe]
    env = {'process_group': None,
           'exit_status': None}

    def get_process_group():
        try:
            env['process_group'] = int(parent_socket.recv(1024))
            readset.remove(sigchld_pipe)
        except:
            traceback.print_exc()
            os.kill(child_pid, signal.SIGKILL)
            return False
        signal.set_wakeup_fd(-1)
        return True

    def get_exit_status():
        try:
            env['exit_status'] = int(parent_socket.recv(1024))
        except:
            traceback.print_exc()
            os.killpg(env['process_group'], signal.SIGKILL)
            return False
        return True

    transitions = {'want_process_group': 'want_exit_status',
                   'want_exit_status': 'done'}
    operations = {'want_process_group': get_process_group,
                  'want_exit_status': get_exit_status}

    state = 'want_process_group'

    while True:
        try:
            ready, _, _ = eintr_retry(
                select.select,
                readset, [], [], timeout)
        except:
            traceback.print_exc()
            process_group = env.get('process_group')
            if process_group:
                os.killpg(process_group, signal.SIGKILL)
            else:
                os.kill(child_pid, signal.SIGKILL)

            msg = 'Could not communicate with proctor and bureaucrat.'
            print >> sys.stderr, msg
            return 1

        if not ready:
            msg = ('Proctor and/or bureaucrat not successfully started before'
                   ' timeout (%d) expired.'
                   ' Check logs in: %r.' % (timeout, log_dir))
            print >> sys.stderr, msg
            return 1

        if sigchld_pipe in ready:
            msg = ('Proctor and/or bureaucrat died unexpected'
                   ' during daemonization.'
                   ' Check logs in: %r.' % (log_dir))
            print >> sys.stderr, msg
            return 1

        operation = operations[state]
        if not operation():
            msg = 'Could not complete operation %s.' % operation
            print >> sys.stderr, msg
            return 1

        state = transitions[state]
        if state == 'done':
            exit_status = env['exit_status']
            if exit_status:
                msg = ('Proctor or bureaucrat did not start successfully.'
                       ' Check logs in: %r' % log_dir)
                print >> sys.stderr, msg
            return exit_status


def _watch_child(pipe, handler=None):
    if handler is None:
        handler = lambda *args, **kwargs: None
    original_handler = signal.signal(signal.SIGCHLD, handler)
    signal.set_wakeup_fd(pipe)
    return original_handler


def kill(pgroupfile):
    if not os.path.exists(pgroupfile):
        print >> sys.stderr, "Process group file %s not found" % pgroupfile
        return 1

    with open(pgroupfile) as f:
        contents = f.read(1024)

    if not contents.isdigit():
        msg = 'Process group file %s does not contain an integer'
        print >> sys.stderr, msg
        return 1

    pgrp = int(contents)

    try:
        os.killpg(pgrp, 0)
    except OSError as e:
        if e.errno != errno.ESRCH:
            raise
        msg = 'Process group %d no longer running' % pgrp
        print >> sys.stderr, msg
        return 1

    try:
        os.killpg(pgrp, signal.SIGTERM)
    except OSError as e:
        if e.errno != errno.ESRCH:
            raise
        msg = 'Process group %d appeared to terminate abnormally'
        print >> sys.stderr, msg
        return 1

    for _ in xrange(5):
        try:
            os.killpg(-pgrp, 0)
        except OSError as e:
            if e.errno == errno.ESRCH:
                return 0
        time.sleep(1)

    try:
        os.killpg(pgrp, signal.SIGKILL)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return 0

    try:
        os.killpg(pgrp, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return 0
    else:
        print >> sys.stderr, ('At least one process in process group %d'
                              ' sleeping uinterruptibly' % pgrp)
        return 1
    return 1


def run(timeout, log_dir, pgroup_file, args):
    parent_socket, child_socket = socket.socketpair(socket.AF_UNIX,
                                                    socket.SOCK_DGRAM)
    signal_sigchld, select_sigchld = os.pipe()

    ensure_log_directories(log_dir)

    original_handler = _watch_child(signal_sigchld)

    child_pid = os.fork()
    if child_pid:
        child_socket.close()
        status = wait_for_bureacrat(parent_socket, child_pid, select_sigchld,
                                    log_dir, timeout)
        sys.exit(status)
    # remove our sigchld handler, etc.
    _watch_child(-1, original_handler)
    parent_socket.close()

    with open('/dev/null') as f:
        stdin = os.dup2(f.fileno(), sys.stdin.fileno())

    # our stdout and err go to the proctor logs now
    proctor_logs = proctor_log_directory(log_dir)
    stdout = StandardOutRotation(directory=proctor_logs).reopen()
    stderr = StandardErrorRotation(directory=proctor_logs).reopen()

    # carry args down
    proctor_cmd = (['python', '-u', '-m', 'tectonic.proctor']
                   + ['--log-dir', log_dir]
                   + args)
    env = os.environ.copy()
    env['BUREAUCRAT_LAUNCH_PIPE'] = str(child_socket.fileno())

    os.setsid()
    new_sid = str(os.getsid(0))

    with open(pgroup_file, 'w') as f:
        f.write(new_sid)

    child_socket.send(new_sid)

    proctor = subprocess.Popen(proctor_cmd,
                               stdin=stdin,
                               stderr=stderr,
                               stdout=stdout,
                               env=env)
    if proctor.poll():
        child_socket.sendall('1')
        sys.exit(1)

    sys.exit(0)


argument_parser = argparse.ArgumentParser()
argument_parser.add_argument('--log-dir', '-l',
                             default='logs',
                             help='how many seconds to wait for'
                             ' successful startup')

argument_parser.add_argument('--timeout', '-t',
                             type=int, default=1,
                             help='how many seconds to wait for'
                             ' successful startup')

argument_parser.add_argument('--pgroupfile',
                             default='tectonic.pgroup',
                             help='process group id file')

argument_parser.add_argument('--kill',
                             action='store_true',
                             default=False)



if __name__ == '__main__':
    known, args = argument_parser.parse_known_args()

    if known.kill:
        status = kill(known.pgroupfile)
        if not status:
            os.unlink(known.pgroupfile)
        sys.exit(status)

    run(known.timeout, known.log_dir, known.pgroupfile, args)
