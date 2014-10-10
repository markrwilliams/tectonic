import os
import errno
import json
import socket
import re
from collections import namedtuple
from passage.way import JSONMessage, DescribedObject


class Message(object):

    def serialize(self):
        values = self._asdict()
        values['__pid__'] = os.getpid()
        values['__name__'] = self.__class__.__name__
        return json.dumps(values)


MESSAGE_TYPES = {}


def make_message(name, fields, attrs=None):
    if attrs is None:
        attrs = {}
    msg_type = type(name, (namedtuple(name, fields), Message), attrs)
    MESSAGE_TYPES[name] = msg_type
    return msg_type


WantTCPListener = make_message('WantTCPListener', 'host port listen')
HaveTCPListener = make_message('HaveTCPListener', 'host port')
WantChannel = make_message('WantChannel', 'identity partner',
                           attrs={'normalized':
                                  property(lambda self: tuple(sorted(self)))})
HaveChannel = make_message('HaveChannel', 'identity partner')
WantWorkerStandardPair = make_message('WantWorkerStandardPair', 'ignored')
HaveWorkerStandardPair = make_message('HaveWorkerStandardPair', 'ignored')
Failure = make_message('Failure', 'request_message')


def deserialize(bytes):
    dictionary = json.loads(bytes)
    msg_type = dictionary.pop('__name__')
    pid = dictionary.pop('__pid__')
    return MESSAGE_TYPES[msg_type](**dictionary), pid


# TODO: do something about this very stupid socket stuff.

def _sendall(sock, msg):
    msg_bytes = msg.serialize()
    netstring = '%d:%s,' % (len(msg_bytes), msg_bytes)

    while netstring:
        netstring = msg_bytes[sock.send(netstring):]


NETSTRING_LENGTH = re.compile('(?P<length>\d+):')
MAX_DIGITS = 1024


def _recvall(sock):
    read = 0
    chunks = ''
    m = None
    while m is None:
        chunk = sock.recv(MAX_DIGITS, socket.MSG_PEEK)
        read += len(chunk)
        if read > MAX_DIGITS:
            raise ValueError("too many digits!")
        chunks += chunk
        m = NETSTRING_LENGTH.match(chunks)

    length = int(m.group('length')) + 1
    sock.recv(m.end())

    read = 0
    chunks = []
    while read < length:
        chunk = sock.recv(length - read)
        if not chunk:
            raise IOError("EOF")
        read += len(chunk)
        chunks.append(chunk)

    data = ''.join(chunks)
    assert data[-1] == ','

    return deserialize(data[:-1])


WorkerStandardPair = namedtuple('WorkerStandardPair', 'stdout stderr')


class WorkerStandardPairMessage(JSONMessage):
    type = WorkerStandardPair

    def describe(self, standard_pair):
        return DescribedObject(description=list(standard_pair._fields),
                               filenos=[standard_pair.stdout,
                                        standard_pair.stderr])

    def rescribe(self, description, filenos):
        return WorkerStandardPair(**dict(zip(description, filenos)))
