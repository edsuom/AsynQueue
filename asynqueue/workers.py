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
Implementors of the worker interface.
"""
import sys, os, os.path, tempfile, shutil

from zope.interface import implements
from twisted.internet import defer

from interfaces import IWorker
import errors, info, util, iteration


# Make all our workers importable from this module
from threads import ThreadQueue, ThreadWorker
from process import ProcessQueue, ProcessWorker
from wire import SocketWorker


class AsyncWorker(object):
    """
    I implement an L{IWorker} that runs tasks in the Twisted main
    loop, one task at a time but in a well-behaved non-blocking
    manner. If the task callable doesn't return a deferred, it better
    get its work done fast.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle.

    Might be useful where you want the benefits of priority queueing
    without leaving the Twisted mindset even for a moment.
    
    """
    implements(IWorker)
    cQualified = ['async', 'local']
    
    def __init__(self, series=[], profiler=None):
        self.iQualified = series
        self.profiler = profiler
        self.info = info.Info()
        self.dLock = util.DeferredLock()

    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    def run(self, task):
        def ready(null):
            # THOU SHALT NOT BLOCK!
            return defer.maybeDeferred(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            if iteration.isIterator(result):
                try:
                    result = iteration.Deferator(result)
                except:
                    status = 'e'
                    result = self.info.setCall(f, args, kw).aboutException()
                else:
                    status = 'i'
                    if consumer:
                        result = iteration.IterationProducer(result, consumer)
            else:
                status = 'r'
            # Hangs if release is done after the task callback
            self.dLock.release()
            task.callback((status, result))

        def oops(failureObj):
            text = self.info.setCall(f, args, kw).aboutFailure(failureObj)
            task.callback(('e', text))

        f, args, kw = task.callTuple
        if self.profiler:
            args = [f] + list(args)
            f = self.profiler.runcall
        consumer = kw.pop('consumer', None)
        vip = (kw.pop('doNext', False) or task.priority <= -20)
        return self.dLock.acquire(vip).addCallback(ready)

    def stop(self):
        return self.dLock.stop()

    def crash(self):
        """
        There's no point to implementing this because the Twisted main
        loop will block along with any task you give this worker.
        """


__all__ = [
    'ThreadQueue', 'ThreadWorker',
    'ProcessQueue', 'ProcessWorker',
    'AsyncWorker', 'SocketWorker',
    'IWorker'
]
