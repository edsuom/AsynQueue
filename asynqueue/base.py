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
The task queue and its immediate support staff.
"""

# Imports
import heapq
from zope.interface import implements
from twisted.python.failure import Failure
from twisted.internet import reactor, interfaces, defer
# Use C Deferreds if possible, for efficiency
try:
    from twisted.internet import cdefer
except:
    pass
else:
    defer.Deferred = cdefer.Deferred

from util import Info
import tasks, iteration
from errors import QueueRunError, ImplementationError, WorkerError


class Priority(object):
    """
    I provide simple, asynchronous access to a priority heap.
    """
    def __init__(self):
        self.heap = []
        self.pendingGetCalls = []

    def shutdown(self):
        """
        Shuts down the priority heap, firing errbacks of the deferreds of any
        get requests that will not be fulfilled.
        """
        if self.pendingGetCalls:
            msg = "No more items forthcoming"
            theFailure = Failure(QueueRunError(msg))
            for d in self.pendingGetCalls:
                d.errback(theFailure)
    
    def get(self):
        """
        Gets an item with the highest priority (lowest value) from the heap,
        returning a deferred that fires when the item becomes available.
        """
        if len(self.heap):
            d = defer.succeed(heapq.heappop(self.heap))
        else:
            d = defer.Deferred()
            self.pendingGetCalls.insert(0, d)
        return d
    
    def put(self, item):
        """
        Adds the supplied I{item} to the heap, firing the oldest getter
        deferred if any L{get} calls are pending.
        """
        heapq.heappush(self.heap, item)
        if len(self.pendingGetCalls):
            d = self.pendingGetCalls.pop()
            d.callback(heapq.heappop(self.heap))

    def cancel(self, selector):
        """
        Removes all pending items from the heap that the supplied I{selector}
        function selects. The function must take an item as its sole argument
        and return C{True} if it selects the item for queue removal.
        """
        for item in self.heap:
            if selector(item):
                self.heap.remove(item)
        # Fix up the possibly mangled heap list
        heapq.heapify(self.heap)


class LoadInfoProducer(object):
    """
    I produce information about the current load of a task queue. The
    information consists of the number of tasks currently queued, and
    is written as a single integer to my consumers as a single integer
    whenever a task is queued up and again when it is completed.

    @ivar consumer: A list of the consumers for whom I'm producing
      information.
    
    """
    implements(interfaces.IPushProducer)
    
    def __init__(self):
        self.queued = 0
        self.producing = True
        self.consumers = []

    def registerConsumer(self, consumer):
        """
        Call this with a provider of L{interfaces.IConsumer} and I'll
        produce for it in addition to any others already registered
        with me.
        """
        consumer.registerProducer(self, True)
        self.consumers.append(consumer)
    
    def shutdown(self):
        """
        Stop me from producing and 
        """
        self.producing = False
        for consumer in self.consumers:
            consumer.unregisterProducer()
    
    def oneLess(self):
        self._update(-1)
    
    def oneMore(self):
        self._update(+1)
    
    def _update(self, increment):
        self.queued += increment
        if self.queued < 0:
            self.queued = 0
        if self.producing:
            for consumer in self.consumers:
                consumer.write(self.queued)
    
    #--- IPushProducer implementation -----------------------------------------
    
    def pauseProducing(self):
        self.producing = False
    
    def resumeProducing(self):
        self.producing = True
    
    def stopProducing(self):
        self.shutdown()


class QueueBase(object):
    """
    Base class for asynchronous queues
    """
    def startup(self):
        """
        Starts up a L{defer.deferredGenerator} that runs the queue. This method
        can only be run once, by the constructor upon instantiation.
        """
        @defer.inlineCallbacks
        def runner():
            while True:
                task = yield self.heap.get()
                if task is None:
                    break
                yield self.mgr.assignment(task)
            # Clean up after the loop exits
            result = yield self.mgr.shutdown(self.timeout)
            self.heap.shutdown()
            defer.returnValue(result)
        
        if self.isRunning():
            raise QueueRunError("Startup only occurs upon instantiation")
        self._d = runner()
        self._triggerID = reactor.addSystemEventTrigger(
            'before', 'shutdown', self.shutdown)
    
    def isRunning(self):
        """
        Returns C{True} if the queue is running, C{False} otherwise.
        """
        return hasattr(self, '_triggerID')
    
    def shutdown(self):
        """
        Initiates a shutdown of the queue by putting a lowest-possible
        priority C{None} object onto the priority heap.
        
        @return: A deferred that fires when all the workers have shut
          down, with a list of any tasks left unfinished in the queue.
        """
        def cleanup(unfinishedTasks):
            if hasattr(self, '_triggerID'):
                reactor.removeSystemEventTrigger(self._triggerID)
                del self._triggerID
            return unfinishedTasks
        
        if self.isRunning():
            self.heap.put(None)
            d = self._d
        else:
            d = defer.succeed([])
        d.addCallback(cleanup)
        return d

    def cancelSeries(self, series):
        """
        Cancels any pending tasks in the specified I{series}, unceremoniously
        removing them from the queue.
        """
        self.heap.cancel(lambda item: getattr(item, 'series', None) == series)

    def cancelAll(self):
        """
        Cancels all pending tasks, unceremoniously removing them from the
        queue.
        """
        self.heap.cancel(lambda item: True)
    
    def subscribe(self, consumer):
        """
        Subscribes the supplied provider of L{interfaces.IConsumer}
        to updates on the number of tasks queued whenever it goes up or down.

        The figure is the integer number of calls currently pending, i.e., the
        number of tasks that have been queued up but haven't yet been called
        plus those that have been called but haven't yet returned a result.
        """
        if interfaces.IConsumer.providedBy(consumer):
            self.loadInfoProducer.registerConsumer(consumer)
        else:
            raise ImplementationError(
                "Object doesn't provide the IConsumer interface")


class TaskQueue(QueueBase):
    """
    I am a task queue for dispatching arbitrary callables to be run by one or
    more worker objects.

    You can construct me with one or more workers, or you can attach them later
    with L{attachWorker}, in which you'll receive an ID that you can use to
    detach the worker.

    @keyword timeout: A number of seconds after which to more drastically
      terminate my workers if they haven't gracefully shut down by that point.

    @keyword warn: Set this option C{True} to only warn that a call
      made after queue shutdown is being ignored, rather than raising
      an exception.

    @keyword profile: Set to a filename and each call will be run
        under a profiler. The profiler stats will be dumped to the
        filename when the queue shuts down.

    """
    def __init__(self, *args, **kw):
        self.info = Info()
        self.tasksBeingRetried = []
        self.taskFactory = tasks.TaskFactory()
        self.mgr = tasks.WorkerManager()
        self.heap = Priority()
        self.loadInfoProducer = LoadInfoProducer()
        for worker in args:
            self.attachWorker(worker)
        self.startup()
        self.timeout = kw.get('timeout', None)
        self.warnOnly = kw.get('warn', False)
        self.profile = kw.get('profile', None)
    
    def attachWorker(self, worker):
        """
        Registers a new provider of IWorker for working on tasks from
        the queue, returning an integer ID that uniquely identifies
        the worker.

        See L{WorkerManager.hire}.
        """
        return self.mgr.hire(worker)

    def _getWorkerID(self, workerOrID):
        if workerOrID in self.mgr.workers:
            return workerOrID
        for thisID, worker in self.mgr.workers.iteritems():
            if worker == workerOrID:
                return thisID
    
    def detachWorker(self, workerOrID, reassign=False, crash=False):
        """
        Detaches and terminates the worker supplied or specified by its ID.

        If I{reassign} is set C{True}, any tasks left unfinished by
        the worker are put into new assignments for other or future
        workers. Otherwise, they are returned via the deferred's
        callback.
        
        See L{tasks.WorkerManager.terminate}.
        """
        ID = self._getWorkerID(workerOrID)
        if ID is None:
            return
        if crash:
            d = self.mgr.terminate(ID, crash=True, reassign=reassign)
        else:
            d = self.mgr.terminate(ID, self.timeout, reassign=reassign)
        return d

    def qualifyWorker(self, worker, series):
        """
        Adds the specified I{series} to the qualifications of the supplied
        I{worker}.
        """
        if series not in worker.iQualified:
            worker.iQualified.append(series)
            self.mgr.assignmentFactory.request(worker, series)
    
    def workers(self, ID=None):
        """
        Returns the worker object specified by I{ID}, or C{None} if that worker
        is not employed with me.

        If no ID is specified, a list of the workers currently attached, in no
        particular order, will be returned instead.
        """
        if ID is None:
            return self.mgr.workers.values()
        return self.mgr.workers.get(ID, None)

    def _taskDone(self, statusResult, task, consumer=None):
        """
        Processes the status/result tuple from a worker running a task:

        'e': An exception was raised; the result is a pretty-printed
             traceback string.

        'r': Ran fine, the result is the return value of the call.

        'i': Ran fine, but the result is an iterable other than a
             standard Python one. The result is an instance of
             L{util.Deferator}, which I make into an
             L{iteration.IterationProducer}. If a *consumer* is
             specified, I will register it with the producer and
             return a deferred that fires when the iteration is
             done. Otherwise I will return the producer for you to
             deal with.

        'c': Ran fine (on an AMP server), but the result was too big
             for a single return value. So the result is a deferred
             that will eventually fire with the result after all the
             chunks of the return value have arrived and been
             magically pieced together and unpickled.
        
        't': The task timed out. I'll try to re-run it, once.
        
        """
        status, result = statusResult
        if status == 'e':
            # Error
            return Failure(errors.WorkerError(result))
        if status in "rc":
            # A plain result, or a deferred to a chunked one.
            return result
        if status == 'i':
            # Iterator
            ip = iteration.IterationProducer(result)
            if consumer:
                ip.registerConsumer(consumer)
                return ip.run()
            return ip
        if status == 't':
            # Timedout. Try again, once.
            if task in self.tasksBeingRetried:
                self.tasksBeingRetried.remove(task)
                return Failure(
                    errors.QueueRunError("Timed out after two tries, gave up"))
            self.tasksBeingRetried.append(task)
            task.rush()
            self.heap.put(task)
            return task.reset().addCallback(self._taskDone)
        return Failure(
            errors.QueueRunError("Unknown status '{}'".format(status)))
        
    def call(self, func, *args, **kw):
        """
        Puts a call to I{func} with any supplied arguments and keywords into
        the pipeline, returning a deferred to the eventual result of the call
        when it is eventually pulled from the pipeline and run.

        Scheduling of the call is impacted by the I{niceness} keyword that can
        be included in addition to any keywords for the call. As with UNIX
        niceness, the value should be an integer where 0 is normal scheduling,
        negative numbers are higher priority, and positive numbers are lower
        priority.

        Tasks in a series of tasks all having niceness N+10 are dequeued and
        run at approximately half the rate of tasks in another series with
        niceness N.
        
        @keyword niceness: Scheduling niceness, an integer between -20 and 20,
          with lower numbers having higher scheduling priority as in UNIX
          C{nice} and C{renice}.

        @keyword series: A hashable object uniquely identifying a series for
          this task. Tasks of multiple different series will be run with
          somewhat concurrent scheduling between the series even if they are
          dumped into the queue in big batches, whereas tasks within a single
          series will always run in sequence (except for niceness adjustments).
        
        @keyword doNext: Set C{True} to assign highest possible priority, even
          higher than a deeply queued task with niceness = -20.
        
        @keyword doLast: Set C{True} to assign priority so low that any
          other-priority task gets run before this one, no matter how long this
          task has been queued up.

        @keyword timeout: A timeout interval in seconds from when a worker gets
          a task assignment for the call, after which the call will be retried.

        @keyword consumer: An implementor of L{interfaces.IConsumer}
          that will receive iterations if the result of the call is an
          interator.
        
        """
        if not self.isRunning():
            text = self.info.setCall(func, args, kw).aboutCall()
            text = "Queue shut down, ignoring call\n{}\n".format(text)
            if self.warnOnly:
                print text
            else:
                raise QueueRunError(text)
        self.loadInfoProducer.oneMore()
        # Some parameters just for me, not for the task
        niceness = kw.pop('niceness', 0     )
        series   = kw.pop('series',   None  )
        timeout  = kw.pop('timeout',  None  )
        doLast   = kw.pop('doLast',   False )
        consumer = kw.pop('consumer', None  )
        task = self.taskFactory.new(func, args, kw, niceness, series, timeout)
        # Workers have to honor the doNext keyword, too
        if kw.get('doNext', False):
            task.rush()
        elif doLast:
            task.relax()
        self.heap.put(task)
        task.d.addCallback(self._taskDone, task, consumer)
        return task.d

    def callAll(self, func, *args, **kw):
        """
        Puts a call to I{func} with any supplied arguments and keywords into
        the pipeline, returning a deferred to the eventual result of the call
        when it is eventually pulled from the pipeline and run.

        TODO: Not sure how I'm going to implement this without making
        a huge mess.
        """
        raise NotImplementedError(
            "Broadcast assignments not yet implemented")
