# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Implementors of the L{interfaces.IWorker} interface. These objects
are what handle the tasks in your L{base.TaskQueue}.
"""
import sys, os, os.path, tempfile, shutil

from zope.interface import implements
from twisted.internet import defer

from interfaces import IWorker
import errors, info, util, iteration


# Make all our workers importable from this module
from threads import ThreadWorker
from process import ProcessWorker
from wire import SocketWorker


class AsyncWorker(object):
    """
    I implement an L{IWorker} that runs tasks in the Twisted main
    loop.

    I run each L{task.Task} one at a time but in a well-behaved
    non-blocking manner. If the task callable doesn't return a
    C{Deferred}, it better get its work done fast. You just can't get
    away with blocking in the Twisted main loop.

    You can supply a I{series} keyword containing a list of one or
    more task series that I am qualified to handle.

    This class was mostly written for testing during development, but
    it helped keep the basic functions of a worker in mind. And who
    knows; it might be useful where you want the benefits of priority
    queueing without leaving the Twisted mindset even for a moment.
    """
    implements(IWorker)
    cQualified = ['async', 'local']
    
    def __init__(self, series=[], raw=False):
        """
        Constructs an instance of me with a L{util.DeferredLock}.
        
        @param series: A list of one or more task series that this
          particular instance of me is qualified to handle.

        @param raw: Set C{True} if you want raw iterators to be
          returned instead of L{iteration.Deferator} instances. You
          can override this in with the same keyword set C{False} in a
          call.
        """
        self.iQualified = series
        self.raw = raw
        self.info = info.Info()
        self.dLock = util.DeferredLock()

    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    def run(self, task):
        """
        Implements L{IWorker.run}, running the I{task} in the main
        thread. The task callable B{must} not block.
        """
        def ready(null):
            # THOU SHALT NOT BLOCK!
            return defer.maybeDeferred(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            if not raw and iteration.isIterator(result):
                try:
                    result = iteration.Deferator(result)
                except:
                    result = []
                else:
                    if consumer:
                        result = iteration.IterationProducer(result, consumer)
                status = 'i'
            else:
                status = 'r'
            # Hangs if release is done after the task callback
            self.dLock.release()
            task.callback((status, result))

        def oops(failureObj):
            text = self.info.setCall(f, args, kw).aboutFailure(failureObj)
            task.callback(('e', text))

        f, args, kw = task.callTuple
        raw = kw.pop('raw', None)
        if raw is None:
            raw = self.raw
        consumer = kw.pop('consumer', None)
        vip = (kw.pop('doNext', False) or task.priority <= -20)
        return self.dLock.acquire(vip).addCallback(ready)

    def stop(self):
        """
        Implements L{IWorker.stop}.
        """
        return self.dLock.stop()

    def crash(self):
        """
        There's no point to implementing this because the Twisted main
        loop will block along with any task you give this worker.
        """


__all__ = [
    'ThreadWorker', 'ProcessWorker', 'AsyncWorker', 'SocketWorker',
    'IWorker'
]
