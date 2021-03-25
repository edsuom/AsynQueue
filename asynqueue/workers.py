# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# See edsuom.com for API documentation as well as information about
# Ed's background and other projects, software and otherwise.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.

"""
Implementors of the L{interfaces.IWorker} interface. These objects
are what handle the tasks in your L{base.TaskQueue}.
"""
from __future__ import absolute_import
import sys, os, os.path, tempfile, shutil

from zope.interface import implementer
from twisted.internet import defer

from asynqueue import errors, info, util, iteration
from asynqueue.interfaces import IWorker


# Make all our workers (except WireWorker, because of asynqueue.wire
# interpreter invocation) importable from this module
from asynqueue.threads import ThreadWorker
from asynqueue.process import ProcessWorker
# Not this one
#from asynqueue.wire import WireWorker


@implementer(IWorker)
class AsyncWorker(object):
    """
    I implement an L{IWorker} that runs tasks in the Twisted main
    loop.

    I run each L{tasks.Task} one at a time but in a well-behaved
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
    cQualified = ['async', 'local']
    
    def __init__(self, series=[], raw=False):
        """
        Constructs an instance of me with a L{util.DeferredLock}.
        
        @param series: A list of one or more task series that this
          particular instance of me is qualified to handle.

        @param raw: Set C{True} if you want raw iterators to be
          returned instead of L{iteration.Deferator} instances. You
          can override this with the same keyword set C{False} in a
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
                status = b'i'
            else:
                status = b'r'
            # Hangs if release is done after the task callback
            self.dLock.release()
            task.callback((status, result))

        def oops(failureObj):
            #import pdb; pdb.set_trace()
            text = self.info.setCall(f, args, kw).aboutFailure(failureObj)
            task.callback((b'e', text))

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
    'ThreadWorker', 'ProcessWorker', 'AsyncWorker', 'WireWorker',
    'IWorker'
]
