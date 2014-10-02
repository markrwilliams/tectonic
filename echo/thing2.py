import os
import time
from tectonic.bureaucrat import request_channel

channel = request_channel(identity='thing2', partner='thing1')


while True:
    print 'THING2', os.getpid(), 'pinging THING1'
    time.sleep(.5)
    channel.sendall('thing2 ping')
    pong = channel.recv(1024)
    print 'THING2', os.getpid(), 'got ping', pong
    time.sleep(.5)
