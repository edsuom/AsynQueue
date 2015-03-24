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
import cPickle as pickle

from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.protocols import amp


class RunTask(amp.Command):
    """
    Runs a task and returns the result. The callable, args, and kw are
    all pickled strings. The args and kw can be empty strings,
    indicating no args or kw.
    """
    arguments = [
        ('f_pickled', amp.String()),
        ('args_pickled', amp.String()),
        ('kw_pickled', amp.String()),
    ]
    response = [
        ('result_pickled', amp.String()),
        ('getMoreWith', amp.Integer()),
        ('failureInfo', amp.String())
    ]


class GetMore(amp.Command):
    """
    Get more more data from a task with such a big result thst it had
    to be chunked.
    """
    arguments = [
        ('id', amp.Integer())
    ]
    response = [
        ('result_pickled', amp.String()),
        ('getMoreWith', amp.Integer()),
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

    def _callInfo(self, func, *args, **kw):
        if func.__class__.__name__ == "function":
            text = func.__name__
        elif callable(func):
            text = "{}.{}".format(func.__class__.__name__, func.__name__)
        else:
            try:
                func = str(func)
            except:
                func = repr(func)
            text = "{}[Not Callable!]".format(func)
        text += "("
        if args:
            text += ", ".join(args)
        for name, value in kw.iteritems():
            text += ", {}={}".format(name, value)
        text += ")"
        return text
    
    def tryTask(self, f, *args, **kw):
        response = {'failureInfo': ""}
        try:
            result = f(*args, **kw)
            response['result_pickled'] = pickle.dumps(
                result, pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            lineList = [
                "Exception '{}'".format(str(e)),
                " running task '{}':".format(self._callInfo(f, *args, **kw))]
            lineList.append(
                "-" * (max([len(x) for x in lineList]) + 1))
            lineList.append("".join(traceback.format_tb(e[2])))
            response['failureInfo'] = "\n".join(lineList)
        return response
    
    def runTask(self, f_pickled, args_pickled, kw_pickled):
        f = pickle.loads(f_pickled)
        args = pickle.loads(args_pickled) if args_pickled else []
        kw = pickle.loads(kw_pickled) if kw_pickled else {}
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
    
