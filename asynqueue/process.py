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
from time import time
import multiprocessing as mp

from zope.interface import implements
from twisted.internet import defer
from twisted.python.failure import Failure

from base import TaskQueue
from interfaces import IWorker
import errors, util, iteration, info


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

        @param kw: Keywords for the regular L{TaskQueue} constructor,
          except I{callStats}. That enables callStats on each worker.
        """
        callStats = kw.pop('callStats', False)
        TaskQueue.__init__(self, **kw)
        for null in xrange(N):
            worker = ProcessWorker(callStats=callStats)
            self.attachWorker(worker)

    def stats(self):
        """
        Only call this if you've constructed me with
        C{callStats=True}. Returns a C{Deferred} that fires with a
        concatenated list of lists from calls to
        L{ProcessWorker.stats} on each of my workers.
        """
        dList = []
        result = []
        for worker in self.th.roster('process'):
            dList.append(worker.stats().addCallback(result.extend))
        return defer.DeferredList(dList).addCallback(lambda _: result)


class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    process.

    You can also supply a I{series} keyword containing a list of one
    or more task series that I am qualified to handle.

    B{Note:} Each task's callable is pickled along with its arguments
    to be sent over the interprocess pipe. Thus it must be something
    that can be reconstructed at the process, i.e., a method of an
    instance of a class that is importable by the process. Keep this
    in mind if you get errors like this::

      cPickle.PicklingError:
        Can't pickle <type 'function'>: attribute lookup
        __builtin__.function failed

    
    Tuning the I{backoff} coefficient
    =================================

    I did some testing of backoff coefficients with unit tests, where
    the reactor wasn't doing much other than running the asynqueue and
    Twisted's trial test runner.
    
    With tasks whose completion time range from 0 to 1 second, backoff
    of B{1.10} was significantly more efficent than 1.15, even more so
    than 1.20.

    Backoff of 1.05 was somewhat more efficient than 1.10 with
    completion times ranging from 0 to 200 ms: 96.7% process/worker
    efficiency vs. 94.5%, with the mean overhead cut in half to around
    3.3ms. But that's not the whole story: with a constant completion
    time of 100ms, 1.05 was actually B{less} efficient: 95.5% vs. 99%,
    and mean overhead B{increased} from around 0.5 ms to around 4 ms!

    After 100 ms, there will have been 7 checks, with the check
    interval finally doubling to 2.1 ms. Things take off rapidly;
    reaching 200 ms takes just another 4 checks, and the interval is
    then 3.1 ms.

    It's only the calls that take longer that benefit from a smaller
    backoff. Going from 1.05 to 1.10 decreased the efficiency of
    1-second calls from 97.5% to 94.3% because the overhead doubled
    from 25 ms to 60 ms. After so many event checks, the check
    interval had increased considerably, enough to add some
    significant dead time after the calls were done. By the time the
    second is up, there will have been 48 checks and the check
    interval will be 0.1 second.

    A backoff of 1.10 is a bit magic numerically in that the current
    check interval is always about one tenth the amount of time that
    has passed since the first check. For a backoff of 1.05, the
    interval is half that. (It takes 81 checks to reach 1 second
    instead of 48.)
    
    The cost of having too many checks is considerable, though, and
    must be worse with a busy reactor, so a backoff less than 1.10
    with an (initial) interval of 0.001 isn't recommended. But you
    might consider tuning it for your application and system.
    
    @ivar interval: The initial event-checking interval, in seconds.
    @type interval: float
    @ivar backoff: The backoff exponent.
    @type backoff: float

    @cvar cQualified: 'process', 'local'
    """
    backoff = 1.10 # This is the iteration.Delay default, anyhow
    
    implements(IWorker)
    cQualified = ['process', 'local']

    @staticmethod
    def cores():
        """
        @return: The number of CPU cores available.

        @rtype: int
        """
        return mp.cpu_count()
    
    def __init__(self, series=[], raw=False, callStats=False):
        """
        Constructs me with a L{ThreadLooper} and an empty list of tasks.
        
        @param series: A list of one or more task series that this
          particular instance of me is qualified to handle.

        @param raw: Set C{True} if you want raw iterators to be
          returned instead of L{iteration.Deferator} instances. You
          can override this in with the same keyword set C{False} in a
          call.

        @param callStats: Set C{True} if you want stats kept about how
          long the calls took to send and to run on the process. Might
          add significant memory usage and slow things down a bit
          overall if there are lots of calls. Obtain a list of the
          call times here and on the process (2-tuples) with the
          L{stats} method.
        """
        self.tasks = []
        self.iQualified = series
        self.callStats = callStats
        if callStats:
            self.callTimes = []
        # Tools
        self.delay = iteration.Delay(backoff=self.backoff)
        self.dLock = util.DeferredLock()
        # Multiprocessing with (Gasp! Twisted heresy!) standard lib Python
        self.cMain, cProcess = mp.Pipe()
        pu = ProcessUniverse(raw, callStats)
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

    def stats(self):
        """
        Assembles and returns a (deferred) list of call times. Each list
        item is a 2-tuple. The first element is the time it took to
        get the result from the process after sending the call to it,
        and the second element is how long the process took to run on
        the process.
        """
        def gotProcessTimes(pTimes):
            result = []
            for k, pTime in enumerate(pTimes):
                result.append((self.callTimes[k], pTime))
            return result
        return self.next("").addCallback(gotProcessTimes)
    
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
            if self.callStats:
                t0 = time()
            self.cMain.send(task.callTuple)
            # "Wait" here (in Twisted-friendly fashion) for a response
            # from the process
            yield self.delay.untilEvent(self.cMain.poll)
            if self.callStats:
                self.callTimes.append(time()-t0)
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
    def __init__(self, raw=False, callStats=False):
        self.iterators = {}
        self.runner = util.CallRunner(raw, callStats)
    
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
                if callSpec == "":
                    # A blank string is a request for stats. Bummer
                    # that this check runs everytime an iteration
                    # happens, but a simple string comparison
                    # operation shouldn't slow things down measurably
                    connection.send(
                        (getattr(self.runner, 'callTimes'), True))
                else:
                    # A next-iteration call
                    connection.send(self.next(callSpec))
            else:
                # A task call
                status, result = self.runner(callSpec)
                if status == 'i':
                    # Due to the pipe between worker and process, we
                    # hold onto the iterator here and just
                    # return an ID to it
                    ID = str(info.hashIt(result))
                    self.iterators[ID] = result
                    result = ID
                connection.send((status, result))
        # Broken out of loop, ready for the process to end
        connection.close()

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
