import pytest
import os
from tectonic import log_rotate


def test_open_log_fd(tmpdir):
    '''\
    The log file should only be created and opened if it's not already present.
    '''
    log = tmpdir.join('log')
    assert not log.check()
    fd = log_rotate.open_log_fd(str(log))
    os.write(fd, 'file')
    os.close(fd)
    assert log.check(file=True)
    assert log.read() == 'file'

    with pytest.raises(OSError):
        log_rotate.open_log_fd(str(log))


def test_rotate_path(tmpdir):
    '''\
    When the newest log file reaches `max_size`, all log files should
    be rotated so that they number `iterations`.
    '''
    log = tmpdir.join('log')
    log.ensure()

    # didn't rotate
    assert not log_rotate.rotate_path(str(log), max_size=0, iterations=2)

    log.open('w').write('log1')

    # did rotate
    assert log_rotate.rotate_path(str(log), max_size=0, iterations=2)

    assert not log.check()
    assert len(tmpdir.listdir()) == 1

    log1 = tmpdir.join('log.1')
    assert log1.check(file=True)
    assert log1.open().read() == 'log1'


def test_LogRotation_creation(tmpdir):
    '''\
    LogRotation should require either a file object or a string
    representing a path.  Both should reference an existing file.
    '''
    path = tmpdir.join('path').ensure()
    fileobj = tmpdir.join('file').ensure()

    log_rotate.LogRotation(log=str(path))
    log_rotate.LogRotation(log=fileobj.open())

    with pytest.raises(ValueError):
        log_rotate.LogRotation(None)

    with pytest.raises(IOError):
        log_rotate.LogRotation(str(tmpdir.join('missing')))

    with pytest.raises(IOError):
        with fileobj.open() as f:
            fileobj.remove()
            log_rotate.LogRotation(f)


def _monitor_does_not_rotate(rotator, local_path):
    assert rotator.monitor() is None
    assert local_path.check()
    assert len(local_path.dirpath().listdir()) == 1


def _monitor_does_rotate(rotator, local_path):
    previous_content = local_path.open().read()

    to_close = rotator.monitor()

    intermediate_content = 'intermediate_content'
    os.write(to_close, intermediate_content)

    to_return = rotator.monitor()

    assert to_return and to_close
    assert len(local_path.dirpath().listdir()) == 3

    for suffix, content in [('', ''),
                            ('.1', intermediate_content),
                            ('.2', previous_content)]:
        log_path = local_path.new(basename=local_path.basename + suffix)
        log_path.check()
        log_path.stat().mode == rotator.mode
        assert log_path.open().read() == content

    return to_return


def test_LogRotation_monitor_path(tmpdir):
    '''\
    LogRotation should be able to rotate a path and open a new fd for it
    when appropriate.
    '''
    log = tmpdir.join('log')
    log.ensure()

    rotator = log_rotate.LogRotation(str(log), max_size=0, iterations=3)
    assert log.stat().mode == rotator.mode

    _monitor_does_not_rotate(rotator, log)

    log.open('w').write('message1')

    new_fd = _monitor_does_rotate(rotator, log)

    os.write(new_fd, 'message2')
    os.close(new_fd)

    assert log.open().read() == 'message2'


def test_LogRotation_monitor_file(tmpdir):
    '''\
    LogRotation should be able to rotate a path and open a new fd for it
    when appropriate.
    '''
    log = tmpdir.join('log')
    original_fileobj = log.open('w')

    rotator = log_rotate.LogRotation(original_fileobj,
                                     max_size=0, iterations=3)
    assert log.stat().mode == rotator.mode

    _monitor_does_not_rotate(rotator, log)

    original_fileobj.write('original')
    original_fileobj.flush()

    _monitor_does_rotate(rotator, log)

    original_fileobj.write('new')
    original_fileobj.flush()

    assert log.open().read() == 'new'
