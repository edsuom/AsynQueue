# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the file COPYING for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

"""
This module is imported by a subordinate Python process to service
a ProcessWorker.
"""

import sys, traceback

from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.protocols import amp

from util import callTraceback, o2p, p2o, Deferator


class RunTask(amp.Command):
    """
    Runs a task and returns the status and result. The callable, args,
    and kw are all pickled strings. The args and kw can be empty
    strings, indicating no args or kw.

    The response has the following status/result structure:

    'e': An exception was raised; the result is a pretty-printed
         traceback string.

    'r': Ran fine, the result is the pickled return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
         Python one. The result is an ID string to use for your
         calls to C{GetMore}.

    """
    arguments = [
        ('f', amp.String()),
        ('args', amp.String()),
        ('kw', amp.String()),
    ]
    response = [
        ('status', amp.String()),
        ('result', amp.String()),
    ]


class GetMore(amp.Command):
    """
    With a unique ID, get the next iteration of data from an iterator
    or a task result so big that it had to be chunked.

    The response has a 'value' string with the pickled iteration value
    or a chunk of the too-big task result, and a 'moreLeft' Bool
    indicating if you should call me again with this ID for another
    iteration or chunk.
    """
    arguments = [
        ('id', amp.String())
    ]
    response = [
        ('value', amp.String()),
        ('moreLeft', amp.Boolean()),
    ]


class QuitRunning(amp.Command):
    """
    Shutdown the reactor (after I'm done responding) and exit.
    """
    arguments = []
    response = []


class TaskServer(amp.AMP):
    """
    The AMP protocol for running tasks, entirely unsafely.
    """
    shutdownDelay = 1.0

    @classmethod
    def started(cls):
        """
        Now that the reactor is running, sends the "OK" handshaking code
        to the ProcessWorker in my spawning Python interpreter via
        stdout.

        Might do something with hashing an authentication code here
        someday. Or not.
        """
        print "OK"
    
    def _idForTask(self, idBase):
        self._idCounter = getattr(self, '_idCounter', 0) + 1
        return "{}-{:d}".format(idBase, self._idCounter)
    
    def tryTask(self, f, *args, **kw):
        response = {}
        try:
            result = f(*args, **kw)
        except Exception as e:
            response['status'] = 'e'
            response['result'] = callTraceback(e, f, *args, **kw)
        else:
            if Deferator.isIterator(result):
                # An iterator
                response['status'] = 'i'
                
        return response
    
    def runTask(self, f, args, kw):
        f = p2o(f)
        args = p2o(args, [])
        kw = p2o(kw, {})
        return self.tryTask(f, *args, **kw)
    RunTask.responder(runTask)

    def quitRunning(self):
        reactor.callLater(self.shutdownDelay, reactor.stop)
        return {}
    QuitRunning.responder(quitRunning)


def start(address):
    pf = Factory()
    pf.protocol = TaskServer
    # Currently the security of UNIX domain sockets is the only
    # security we have!
    reactor.listenUNIX(address, pf)
    reactor.callWhenRunning(TaskServer.started)
    reactor.run()


if __name__ == '__main__':
    address = sys.argv[-1]
    start(address)
    
