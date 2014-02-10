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
