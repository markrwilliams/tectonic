import os
import time
from tectonic.client import request_channel

channel = request_channel(identity='thing1', partner='thing2')


while True:
    print 'THING1', os.getpid(), 'pinging THING2'
    time.sleep(.5)
    channel.sendall('thing1 ping')
    pong = channel.recv(1024)
    print 'THING1', os.getpid(), 'got ping', pong
    time.sleep(.5)
