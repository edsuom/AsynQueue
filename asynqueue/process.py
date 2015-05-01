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
An implementor of the C{IWorker} interface using
(I{gasp! Twisted heresy!}) Python's standard-library
multiprocessing.

@see: L{ProcessQueue} and L{ProcessWorker}.
"""
import multiprocessing as mp

from zope.interface import implements
from twisted.internet import defer
from twisted.python.failure import Failure

from base import TaskQueue
from interfaces import IWorker
import errors, util, iteration


class ProcessQueue(TaskQueue):
    """
    A L{TaskQueue} that runs tasks on one or more subordinate Python
    processes.
    
    I am a L{TaskQueue} for dispatching picklable or keyword-supplied
    callables to be run by workers from a pool of I{N} worker
    processes, the number I{N} being specified as the sole argument of
    my constructor.
    """
    @staticmethod
    def cores():
        """
        @return: The number of CPU cores available.

        @rtype: int
        """
        return ProcessWorker.cores()

    def __init__(self, N, **kw):
        """
        @param N: The number of workers (subordinate Python processes)
        initially in the queue.

        @type N: int

        @param kw: Keywords for the regular L{TaskQueue} constructor.
        """
        TaskQueue.__init__(self, **kw)
        for null in xrange(N):
            worker = ProcessWorker()
            self.attachWorker(worker)


class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    process.

    You can also supply a I{series} keyword containing a list of one
    or more task series that I am qualified to handle.
    
    @ivar interval: The initial event-checking interval, in seconds.
    @type interval: float
    @ivar backoff: The backoff exponent.
    @type backoff: float

    @cvar cQualified: 'process', 'local'
    
    """
    pollInterval = 0.01
    backOff = 1.04
    
    implements(IWorker)
    cQualified = ['process', 'local']

    @staticmethod
    def cores():
        """
        @return: The number of CPU cores available.

        @rtype: int
        """
        return mp.cpu_count()
    
    def __init__(self, series=[], profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        # Tools
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

        @param ID: A unique identifier for the iterator.
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
        Sends the I{task} callable and args, kw to the process (must all
        be picklable) and polls the interprocess connection for a
        result, with exponential backoff.

        I{This actually works very well, O ye Twisted event-purists.}
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
                    # The process returned an iterator, but it's not 
                    # one I could prefetch from. Probably empty.
                    result = []
            if task in self.tasks:
                self.tasks.remove(task)
            task.callback((status, result))
    
    def stop(self):
        """
        @return: A C{Deferred} that fires when the task loop has ended and
          its process terminated.
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
        """
        My L{loop} calls this when the interprocess pipe sends it a string
        identifier for one of the iterators I have pending.

        @return: A 2-tuple with the specified iterator's next value,
          and a bool indicating if the value was valid or a bogus
          C{None} resulting from a C{StopIteration} error or a
          non-existent iterator.
        @rtype: tuple
        """
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

        @param connection: The sub-process end of an interprocess
        connection.
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

