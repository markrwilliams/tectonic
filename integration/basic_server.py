"""
This is a basic test which allows you to setup a server and listen to it.

For example, running:

python integration/basic_server.py localhost 8040

Sets up a server.

Running curl against it generates the following reponse:

curl 'http://localhost:8040/'
<html><body><h1>ok</h1><br/>from 28330

And in server output will print out the entire string (Lorem ipsum dolor etc.)

"""

import os
import sys
import string
import argparse
import gevent.pywsgi

from tectonic.prefork import Master

if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument('address')
    a.add_argument('port', type=int)
    a.add_argument('--logpath', default='log')
    a.add_argument('--pidfile', default='pidfile')
    a.add_argument('--daemonize', '-d', default=False, action='store_true')

    def wsgi(environ, start_response):
        start_response('200 OK', [('Content-Type', 'text/html')])
        pid = os.getpid()
        spid = str(pid)
        sys.stderr.write('''\
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus
eleifend a metus quis sollicitudin. Aenean nec dolor iaculis, rhoncus
turpis sit amet, interdum quam. Nunc rhoncus magna a leo interdum
luctus. Vestibulum nec sapien diam. Aliquam rutrum venenatis
mattis. Etiam eget adipiscing risus. Vestibulum ante ipsum primis in
faucibus orci luctus et ultrices posuere cubilia Curae; Fusce nibh
nulla, lacinia quis dignissim vel, condimentum at odio. Nunc et diam
mauris. Fusce sit amet odio sagittis, convallis urna a, blandit
urna. Phasellus mattis ligula sed tincidunt pellentesque. Nullam
tempor convallis dapibus.

Duis vitae vulputate sem, nec eleifend orci. Donec vel metus
fringilla, ultricies nunc at, ultrices quam. Donec placerat nisi quis
fringilla facilisis. Fusce eget erat ut magna consectetur
elementum. Aenean non vulputate nulla. Aliquam eu dui nibh. Vivamus
mollis suscipit neque, quis aliquam ipsum auctor non. Nulla cursus
turpis turpis, nec euismod urna placerat at. Nunc id sapien
nibh. Vestibulum condimentum luctus placerat. Donec vitae posuere
arcu.''' + '\n')
        return ['<html><body><h1>ok</h1><br/>from ' + spid]

    args = a.parse_args()

    Master(server_class=gevent.pywsgi.WSGIServer,
           socket_factory=gevent.socket.socket,
           sleep=gevent.sleep,
           wsgi=wsgi,
           address=(args.address, args.port),
           logpath=args.logpath,
           pidfile=args.pidfile).run(args.daemonize)
