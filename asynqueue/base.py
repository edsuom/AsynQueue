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
import heapq, logging
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

import errors, tasks, iteration
from info import Info


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
            theFailure = Failure(errors.QueueRunError(msg))
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
        try:
            consumer.registerProducer(self, True)
        except RuntimeError:
            # I must have already been registered with this consumer
            return
        self.consumers.append(consumer)
    
    def shutdown(self):
        """
        Stop me from producing and unregister any consumers I have.
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


class Queue(object):
    """
    I am an asynchronous priority queue. Construct me with an item
    handler that can be called with each item from the queue and
    shutdown when I'm done.

    Put anything you like in the queue except C{None} objects. Those
    are reserved for triggering a queue shutdown.
    """
    def __init__(self, handler, timeout=None):
        """
        Starts up a deferred-yielding loop that runs the queue. This
        method can only be run once, by the constructor upon
        instantiation.
        """
        @defer.inlineCallbacks
        def runner():
            while True:
                self._runFlag = True
                item = yield self.heap.get()
                if item is None:
                    break
                self.loadInfoProducer.oneLess()
                yield self.handler(item)
            # Clean up after the loop exits
            result = yield self.handler.shutdown(timeout)
            self.heap.shutdown()
            defer.returnValue(result)
        
        if self.isRunning():
            raise errors.QueueRunError(
                "Startup only occurs upon instantiation")
        self.heap = Priority()
        self.handler = handler
        self.loadInfoProducer = LoadInfoProducer()
        # Start my loop
        self._d = runner()
    
    def isRunning(self):
        """
        Returns C{True} if the queue is running, C{False} otherwise.
        """
        return getattr(self, '_runFlag', False)
    
    def shutdown(self):
        """
        Initiates a shutdown of the queue by putting a lowest-possible
        priority C{None} object onto the priority heap.
        
        @return: A deferred that fires when my handler has shut down,
          with a list of any items left unhandled in the queue.
        """
        if self.isRunning():
            self.heap.put(None)
            return self._d
        return defer.succeed([])

    def put(self, item):
        """
        Put an item into my heap
        """
        self.heap.put(item)
        self.loadInfoProducer.oneMore()
        
    def cancelSeries(self, series):
        """
        Cancels any pending items in the specified I{series},
        unceremoniously removing them from the queue.
        """
        self.heap.cancel(
            lambda item: getattr(item, 'series', None) == series)

    def cancelAll(self):
        """
        Cancels all pending items, unceremoniously removing them from the
        queue.
        """
        self.heap.cancel(lambda item: True)
    
    def subscribe(self, consumer):
        """
        Subscribes the supplied provider of L{interfaces.IConsumer} to
        updates on the number of items queued whenever it goes up or
        down.

        The figure is the integer number of calls currently pending,
        i.e., the number of items that have been queued up but haven't
        yet been handled plus those that have been called but haven't
        yet returned a result.
        """
        if interfaces.IConsumer.providedBy(consumer):
            self.loadInfoProducer.registerConsumer(consumer)
        else:
            raise errors.ImplementationError(
                "Object doesn't provide the IConsumer interface")


class TaskQueue(object):
    """
    I am a task queue for dispatching arbitrary callables to be run by
    one or more worker objects.

    You can construct me with one or more workers, or you can attach
    them later with L{attachWorker}, in which you'll receive an ID
    that you can use to detach the worker.

    @keyword timeout: A number of seconds after which to more
      drastically terminate my workers if they haven't gracefully shut
      down by that point.

    @keyword warn: Merely log errors via an 'asynqueue' logger with
      ERROR events. The default is to stop the reactor and raise an
      exception when an error is encountered.

    @keyword verbose: Provide detailed info about tasks that are logged
      or result in errors.

    @keyword spew: Log all task calls, whether they raise errors or
      not. Can generate huge logs! Implies verbose=True.

    @keyword profile: (TODO) Set to a filename and each call will be
      run under a profiler. The profiler stats will be dumped to the
      filename when the queue shuts down.
    """
    def __init__(self, *args, **kw):
        # Options
        self.timeout = kw.get('timeout', None)
        self.spew = kw.get('spew', False)
        if kw.get('warn', False) or self.spew:
            self.logger = logging.getLogger('asynqueue')
            if self.spew:
                self.logger.setLevel(logging.INFO)
        if kw.get('verbose', False) or self.spew:
            self.info = Info(remember=True)
        profile = kw.get('profile', None)
        # Bookkeeping
        self.tasksBeingRetried = []
        # Tools
        self.th = tasks.TaskHandler(profile)
        self.taskFactory = tasks.TaskFactory()
        # Attach any workers provided now
        for worker in args:
            self.attachWorker(worker)
        # Start things up with my very own live asynchronous queue
        # using a TaskHandler
        self.q = Queue(self.th, self.timeout)
        # Provide for a clean shutdown
        self._triggerID = reactor.addSystemEventTrigger(
            'before', 'shutdown', self.shutdown)

    def isRunning(self):
        """
        Returns C{True} if my task handler and queue are running,
        C{False} otherwise.
        """
        return self.th.isRunning and self.q.isRunning()
        
    def shutdown(self):
        def cleanup(stuff):
            if hasattr(self, '_triggerID'):
                reactor.removeSystemEventTrigger(self._triggerID)
                del self._triggerID
            if hasattr(self, '_dc') and self._dc.active():
                self._dc.cancel()
            for dc in tasks.Task.timeoutCalls:
                if dc.active():
                    dc.cancel()
            return stuff
        
        if not self.isRunning:
            return defer.succeed(None)
        return self.th.shutdown().addCallback(
            lambda _: self.q.shutdown()).addCallback(cleanup)

    def oops(self, text):
        """
        Use as a callback to log errors and get useful information about
        the call if the 'warn' and 'verbose' constructor keywords were
        set, respectively. 
        """
        if hasattr(self, 'logger'):
            # Merely log the error and continue
            self.logger.error(text)
        else:
            # Print the error (to stderr) and stop the reactor
            import sys
            sys.stderr.write("\nERROR: {}".format(text))
            print "\nShutting down in one second!\n"
            self._dc = reactor.callLater(1.0, reactor.stop)
        return text
        
    def attachWorker(self, worker):
        """
        Registers a new provider of IWorker for working on tasks from the
        queue, returning a deferred that fires with an integer ID
        uniquely identifying the worker.

        See L{WorkerManager.hire}.
        """
        def oops(failureObj, ID):
            text = self.info.aboutFailure(failureObj, ID)
            return self.oops(text)
        
        d = self.th.hire(worker)
        if hasattr(self, 'info'):
            ID = self.info.setCall(self.attachWorker, worker).ID
            d.addErrback(oops, ID)
        return d

    def _getWorkerID(self, workerOrID):
        if workerOrID in self.th.workers:
            return workerOrID
        for thisID, worker in self.th.workers.iteritems():
            if worker == workerOrID:
                return thisID
    
    def detachWorker(self, workerOrID, reassign=False, crash=False):
        """
        Detaches and terminates the worker supplied or specified by its
        ID, returning a deferred that fires with a list of tasks left
        unfinished by the worker.

        If I{reassign} is set C{True}, any tasks left unfinished by
        the worker are put into new assignments for other or future
        workers. Otherwise, they are returned via the deferred's
        callback.
        
        See L{tasks.WorkerManager.terminate}.
        """
        ID = self._getWorkerID(workerOrID)
        if ID is None:
            return defer.succeed([])
        if crash:
            return self.th.terminate(ID, crash=True, reassign=reassign)
        return self.th.terminate(ID, self.timeout, reassign=reassign)

    def qualifyWorker(self, worker, series):
        """
        Adds the specified I{series} to the qualifications of the supplied
        I{worker}.
        """
        if series not in worker.iQualified:
            worker.iQualified.append(series)
            self.th.assignmentFactory.request(worker, series)
    
    def workers(self, ID=None):
        """
        Returns the worker object specified by I{ID}, or C{None} if that
        worker is not employed with me.

        If no ID is specified, a list of the workers currently
        attached, in no particular order, will be returned instead.
        """
        if ID is None:
            return self.th.workers.values()
        return self.th.workers.get(ID, None)
        
    def _taskDone(self, statusResult, task, consumer=None, ID=None):
        """
        Processes the status/result tuple from a worker running a task:

        'e': An exception was raised; the result is a pretty-printed
             traceback string.

        'r': Ran fine, the result is the return value of the call.

        'i': Ran fine, but the result was an iterable other than a
             standard Python one. So my result is a Deferator that
             yields deferreds to the worker's iterations, or, if you
             specified a consumer, an IterationProducer registered
             with the consumer that needs to get running to write
             iterations to it.

        'c': Ran fine (on an AMP server), but the result was too big
             for a single return value. So the result is a deferred
             that will eventually fire with the result after all the
             chunks of the return value have arrived and been
             magically pieced together and unpickled.
        
        't': The task timed out. I'll try to re-run it, once.
        """
        status, result = statusResult
        # Deal with any info for this task call
        if ID:
            if status == 'e' or self.spew:
                taskInfo = "Task: {}".format(self.info.aboutCall(ID))
            self.info.forgetID(ID)
        # If we are spewing log entries, deal with that now
        if self.spew:
            try:
                stringResult = str(result)
                taskInfo += " -> {}".format(stringResult[:40])
            except:
                pass
            self.logger.info(taskInfo)
        # Now process the status/result
        if status == 'e':
            return self.oops(result)
        if status in "rc":
            # A plain result, or a deferred to a chunked one
            return result
        if status == 'i':
            # An iteration, possibly an IterationConsumer that we need
            # to run now
            if consumer:
                return result.run()
            return result
        if status == 't':
            # Timedout. Try again, once.
            if task in self.tasksBeingRetried:
                self.tasksBeingRetried.remove(task)
                return Failure(
                    errors.QueueRunError(
                        "Timed out after two tries, gave up"))
            self.tasksBeingRetried.append(task)
            task.rush()
            self.q.put(task)
            return task.reset().addCallback(self._taskDone)
        return Failure(
            errors.QueueRunError("Unknown status '{}'".format(status)))

    def _newTask(self, func, args, kw):
        """
        Make a new Task object from a func-args-kw combo
        """
        if not self.isRunning():
            return self.oops("Queue not running")
        # Some parameters just for me, not for the task
        niceness = kw.pop('niceness', 0     )
        series   = kw.pop('series',   None  )
        timeout  = kw.pop('timeout',  None  )
        doLast   = kw.pop('doLast',   False )
        task = self.taskFactory.new(func, args, kw, niceness, series, timeout)
        # Workers have to honor the consumer and doNext keywords, too
        consumer = kw.get('consumer', None)
        if kw.get('doNext', False):
            task.rush()
        elif doLast:
            task.relax()
        if hasattr(self, 'info'):
            ID = self.info.setCall(func, args, kw).ID
            task.d.addCallback(self._taskDone, task, consumer, ID)
        else:
            task.d.addCallback(self._taskDone, task, consumer)
        return task
        
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
          interator. In such case, the returned result is a deferred
          that fires when the iterations have all been produced.
        
        """
        task = self._newTask(func, args, kw)
        self.q.put(task)
        return task.d

    def update(self, func, *args, **kw):
        """
        Sets an update task from I{func} with any supplied arguments and
        keywords to be run directly on all current and future
        workers. Returns a deferred to the result of the call on all
        current workers, though there is no mechanism for obtaining
        such results for new hires, so it's probably best not to rely
        too much on them.

        The updates are run via a direct remoteCall to each worker,
        not through the queue. Because of the disconnect between
        queued and direct calls, it is likely but not guaranteed that
        any jobs you have queued when this method is called will run
        on a particular worker B{after} this update is run. Wait for
        the deferred from this method to fire before queuing any jobs
        that need the update to be in place before running.

        If you don't want the task saved to the update list, but only
        run on the workers currently attached, set the I{ephemeral}
        keyword C{True}.
        """
        if 'consumer' in kw:
            raise ValueError(
                "Can't supply a consumer for an update because there "+\
                "may be multiple iteration producers")
        ephemeral = kw.pop('ephemeral', False)
        task = self._newTask(func, args, kw)
        return self.th.update(task, ephemeral)
