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
An implementor of the IWorker interface using (gasp! Twisted
heresy!) Python's stdlib multiprocessing.
"""
import multiprocessing as mp

from zope.interface import implements
from twisted.internet import defer
from twisted.python.failure import Failure

from base import TaskQueue
from interfaces import IWorker
import errors, info, util, iteration


class ProcessQueue(TaskQueue):
    """
    I am a task queue for dispatching picklable or keyword-supplied
    callables to be run by workers from a pool of I{N} worker
    processes, the number I{N} being specified as the sole argument of
    my constructor.
    """
    @staticmethod
    def cores():
        return ProcessWorker.cores()

    def __init__(self, N, **kw):
        TaskQueue.__init__(self, **kw)
        for null in xrange(N):
            worker = ProcessWorker()
            self.attachWorker(worker)


class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    process.

    You can also supply a series keyword containing a list of one or
    more task series that I am qualified to handle.
    """
    pollInterval = 0.01
    backOff = 1.04
    
    implements(IWorker)
    cQualified = ['process', 'local']

    @staticmethod
    def cores():
        return mp.cpu_count()
    
    def __init__(self, series=[], profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        # Tools
        self.info = info.Info()
        self.delay = iteration.Delay()
        self.dLock = util.DeferredLock()
        # Multiprocessing with (Gasp! Twisted heresy!) standard lib Python
        self.cMain, cProcess = mp.Pipe()
        pu = ProcessUniverse()
        self.process = mp.Process(target=pu.loop, args=(cProcess,))
        self.process.start()

    def _killProcess(self):
        self.cMain.close()
        self.process.terminate()

    def next(self, ID):
        """
        Do a next call of the iterator held by my process, over the pipe
        and in Twisted fashion.
        """
        def gotLock(null):
            self.cMain.send(ID)
            return self.delay.untilEvent(
                self.cMain.poll).addCallback(responseReady)
        def responseReady(waitStatus):
            if not waitStatus:
                raise errors.TimeoutError(
                    "Timeout waiting for next iteration from process")
            result = self.cMain.recv()
            self.dLock.release()
            if result[1]:
                return result[0]
            return Failure(StopIteration)
        return self.dLock.acquire(vip=True).addCallback(gotLock)
    
    # Implementation methods
    # -------------------------------------------------------------------------
        
    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable and args, kw to the process (must all be
        picklable) and polls the interprocess connection for a result,
        with exponential backoff.
        """
        if task is None:
            # A termination task, do after pending tasks are done
            yield self.dLock.acquire()
            self.cMain.send(None)
            # Wait (a very short amount of time) for the process loop
            # to exit
            self.process.join()
            self.dLock.release()
        else:
            # A regular task
            self.tasks.append(task)
            yield self.dLock.acquire(task.priority <= -20)
            # Our turn!
            #------------------------------------------------------------------
            consumer = task.callTuple[2].pop('consumer', None)
            self.cMain.send(task.callTuple)
            # "Wait" here (in Twisted-friendly fashion) for a response
            # from the process
            yield self.delay.untilEvent(self.cMain.poll)
            status, result = self.cMain.recv()
            self.dLock.release()
            if status == 'i':
                # What we get from the process is an ID to an iterator
                # it is holding onto, but we need to hook up to it
                # with a Prefetcherator and then make a Deferator,
                # which we will either return to the caller or couple
                # to a consumer provided by the caller.
                ID = result
                pf = iteration.Prefetcherator(ID)
                ok = yield pf.setup(self.next, ID)
                if ok:
                    result = iteration.Deferator(pf)
                    if consumer:
                        result = iteration.IterationProducer(result, consumer)
                else:
                    status = 'e'
                    # The process may have an iterator, but it's not a
                    # proper one
                    result = "Failed to iterate for call {}".format(
                        self.info.setCall(*task.callTuple).aboutCall())
            if task in self.tasks:
                self.tasks.remove(task)
            task.callback((status, result))
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        process terminated.
        """
        def terminationTaskDone(null):
            self._killProcess()
            return self.dLock.stop()            
        return self.run(None).addCallback(terminationTaskDone)

    def crash(self):
        self._killProcess()
        return self.tasks


class ProcessUniverse(object):
    """
    Each process for a L{ProcessWorker} lives in one of these.
    """
    def __init__(self):
        self.iterators = {}
        self.runner = util.CallRunner()

    def next(self, ID):
        if ID in self.iterators:
            try:
                value = self.iterators[ID].next()
            except StopIteration:
                del self.iterators[ID]
                return None, False
            return value, True
        return None, False
    
    def loop(self, connection):
        """
        Runs a loop in a dedicated process that waits for new tasks. The
        loop exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here for the next call
            callSpec = connection.recv()
            if callSpec is None:
                # Termination call, no reply expected; just exit the
                # loop
                break
            elif isinstance(callSpec, str):
                # A next-iteration call
                connection.send(self.next(callSpec))
            else:
                # A task call
                status, result = self.runner(callSpec)
                if status == 'i':
                    # Due to the pipe between worker and process, we
                    # hold onto the iterator here and just
                    # return an ID to it
                    ID = str(hash(result))
                    self.iterators[ID] = result
                    result = ID
                connection.send((status, result))
        # Broken out of loop, ready for the process to end
        connection.close()

