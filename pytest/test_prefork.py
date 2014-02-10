import os
import fcntl
import shutil
import os.path
import tempfile
from tectonic import prefork


def test_WorkerMetadata():
    """
    This is a simple test, as WorkerMetadata only holds data
    """

    pid = 'pid'
    health_check_read = 100
    last_seen = 'now'
    metadata = prefork.WorkerMetadata(pid=pid,
                                      health_check_read=health_check_read,
                                      last_seen=last_seen)

    assert metadata.pid == pid
    assert metadata.health_check_read == health_check_read
    assert metadata.last_seen == last_seen


def test_WriteAndFlushFile():
    """
    Make sure we can write to and read from a file.

    """

    try:
        # Create a directory. Make sure to remove it at the end.
        dirname = tempfile.mkdtemp()
        filename = 'filename.txt'
        text1 = 'The quick brown fox\n'
        text2 = 'The lazy dog'
        full_path = os.path.join(dirname, filename)

        # Open a file and write using both changed methods
        f = prefork.WriteAndFlushFile(full_path, 'w')
        f.write(text1)
        f.writelines(text2)
        f.close()
        
        # Read everything back
        f = open(full_path, 'r')
        data = f.readlines()
        f.close()
        
        assert data[0] == text1
        assert data[1] == text2

    finally:
        # Always remove it
        shutil.rmtree(dirname)


def test_set_nonblocking():
    """
    See if we can set a file to non-blocking status

    Create a random file for this.
    """

    f = tempfile.TemporaryFile()
    flags = fcntl.fcntl(f, fcntl.F_GETFL, os.O_NONBLOCK)
    assert ((flags | os.O_NONBLOCK) == flags) is False
    altered_f = prefork.set_nonblocking(f)
    flags = fcntl.fcntl(f, fcntl.F_GETFL, os.O_NONBLOCK)
    assert (flags | os.O_NONBLOCK) == flags

    # Destroy the file, even though GC will do that anyway.
    f.close()
        
