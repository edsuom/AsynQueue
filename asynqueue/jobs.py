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
Running jobs via Workers given references to objects containing callables.

Process workers (either local or remote) get pickled copies of the
objects while Thread workers use the objects directly in their
threads.
"""

from zope.interface import implements, Interface
from twisted.internet import defer, reactor
from twisted.python.failure import Failure
# Use C Deferreds if possible, for efficiency
try:
    from twisted.internet import cdefer
except:
    pass
else:
    defer.Deferred = cdefer.Deferred

import base, workers

VERBOSE = True
def log(msgProto, *args):
    if VERBOSE:
        print msgProto % args


class JobManager(object):
    """
    I keep jobs running on python interpreters that are attached as
    children, maintaining a pipeline of no fewer than I{N} calls
    pending on each interpreter worker to minimize the effects of
    network latency for the connection to the interpreters and balance
    the load across the workers while still permitting some priority
    queueing of jobs by niceness. For local workers, use N=1 as there
    is (essentially) no latency.

    You can supply an instance of L{base.TaskQueue} to the constructor. I will
    instantiate my own if not.

    I maintain a dict I{updates} of update tasks to perform for each jobID
    before any (further) runs for that job. Each sequence has four elements::

        [funcName, args, kw, workersUpdated]

    When a worker runs a given update task, that worker's ID is appended to the
    I{workersUpdate} list that is the fourth element of I{updates}. That will
    indicate that it needs not run the update task again.

    @ivar queue: The TaskQueue instance I'm using.
    """
    maxRetries = 1
    
    def __init__(self, queue=None):
        self.jobs  = {}
        self.updates = {}
        self.callsPending = {}
        self.registeredClasses = {}
        if queue is None:
            self.queue = base.TaskQueue()
        else:
            self.queue = queue
    
    def shutdown(self):
        """
        Shuts down my task queue, returning a deferred that fires when the
        queue has emptied and all interpreter workers have finished and been
        terminated. The task queue shutdown takes care of shutting down
        everything else, including any attached workers.
        """
        return self.queue.shutdown()

    def jobTried(self, result, jobID, worker):
        """
        Callback from loading a new job.

        If the worker's root reference raised an unexpected failure, returns
        C{False}. If everything went OK, returns C{True}. If there was a
        failure that may not have been the worker's fault, returns C{None}.
        """
        if hasattr(result, 'check'):
            # Oops, failure from a bogus root reference.
            log("Worker %d supplied nonconforming root reference", worker.ID)
            return False
        if isinstance(result, (list, tuple)):
            if result[0]:
                msg = "Callable objects: %s" % ", ".join(result[1])
                log("Job %d loaded OK on worker %s\n%s", jobID, worker.ID, msg)
                self.queue.qualifyWorker(worker, jobID)
                return True
            log("Job %d failed on worker %d:\n%s", jobID, worker.ID, result[1])
            return None
        # Not a failure or status,result tuple, so just pass it along
        return result

    def attachChild(self, childRoot, N=3):
        """
        Attaches a new child interpreter worker using the supplied I{childRoot}
        PB root reference.

        Tries to load all of the currently registered jobs on the worker. If an
        unexpected failure (not a simple job-loading exception) arises, the
        worker is not hired.

        The default number (three) of job runs that the worker is willing to
        queue up on its end can be overridden with the I{N} keyword.
        
        Returns a deferred that fires with the worker's ID, or C{None} if not
        hired.
        """
        def jobTried(status):
            if status:
                d = self._runRegisterClasses(worker)
                if jobID in self.updates:
                    d.addCallback(lambda _: self._runUpdate(jobID, worker))
                return d
            mutable.append(None)
        
        def allDone(null):
            if len(mutable):
                d = self.queue.detachWorker(worker)
                d.addCallback(lambda _: None)
                return d
            return worker.ID

        mutable = []
        worker = ChildWorker(childRoot, N)
        self.queue.attachWorker(worker)
        dList = []
        for jobID, jobInfo in self.jobs.iteritems():
            jobCode = jobInfo[0]
            d = childRoot.callRemote('newJob', jobID, jobCode)
            d.addBoth(self.jobTried, jobID, worker)
            d.addCallback(jobTried)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(allDone)
    
    def detachChild(self, childID):
        """
        Detaches and terminates the child interpreter worker specified by the
        supplied I{childID}.
        """
        return self.queue.detachWorker(childID, reassign=True, crash=True)

    def new(self, jobObject, niceness=0):
        """
        Registers a new object with one or more methods that can be be
        called on qualified child interpreters.
        
        @param jobObject: A picklable Python object that defines the
          namespace and one or more methods callable for the job.

        @keyword niceness: Scheduling niceness for all calls of the job.

        @type niceness: An integer between -20 and 20, with lower numbers
          having higher scheduling priority as in UNIX C{nice} and C{renice}.
        
        @return: A deferred that fires with a unique ID for the job.
        """
        jobID = self._jobCounter = getattr(self, '_jobCounter', 0) + 1
        self.callsPending[jobID] = {}
        self.jobs[jobID] = [jobObject, niceness]
        
        dList = []
        for worker in self.queue.workers():
            d = worker.remoteCaller('newJob', jobID, jobCode)
            d.addBoth(self.jobTried, jobID, worker)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(lambda _: jobID)
        return d

    def _runUpdate(self, jobID, worker):
        dList = []
        for funcName, args, kw, workersUpdated in self.updates[jobID]:
            if worker.ID in workersUpdated:
                continue
            d = worker.remoteCaller('runJob', jobID, funcName, *args, **kw)
            d.addCallback(lambda _: workersUpdated.append(worker.ID))
            dList.append(d)
        return defer.DeferredList(dList)
    
    def update(self, jobID, callName, *args, **kw):
        """
        Appends a new task to the update list for the specified I{jobID}. Runs
        the new update task on all workers currently attached and ensures that
        all new workers run the task for that job before they run any other
        tasks for it.

        The updates are run via a direct remoteCall to each worker, not through
        the queue. Because of the disconnect between queued and direct calls,
        it is likely but not guaranteed that any jobs you have queued when this
        method is called will run on a particular worker B{after} this update
        is run. Wait for the deferred from this method to fire before queuing
        any jobs that need the update to be in place before running.

        If you don't want the task saved to the update list, but only run on
        the workers currently attached, set the I{ephemeral} keyword C{True}.
        """
        ephemeral = kw.pop('ephemeral', False)
        if ephemeral:
            dList = [
                worker.remoteCaller('runJob', jobID, callName, *args, **kw)
                for worker in self.queue.workers()]
        else:
            if jobID not in self.updates:
                self.updates[jobID] = []
            self.updates[jobID].append([callName, args, kw, []])
            dList = [
                self._runUpdate(jobID, worker)
                for worker in self.queue.workers()]
        return defer.DeferredList(dList)

    def _runRegisterClasses(self, worker):
        stringReps = []
        for stringRep, registeredWorkers in self.registeredClasses.iteritems():
            if worker.ID in registeredWorkers:
                continue
            registeredWorkers.append(worker.ID)
            stringReps.append(stringRep)
        return worker.remoteCaller('registerClasses', *stringReps)
    
    def registerClasses(self, *args):
        """
        Instructs my current and future nodes to register the classes specified
        by the argument(s) as self-unjellyable and allowable past PB
        security. The classes will be registered for B{all} jobs, and are
        specified by their string representations::
        
            <package(s).module.class>

        Use judiciously!
        
        """
        for stringRep in args:
            if stringRep not in self.registeredClasses:
                self.registeredClasses[stringRep] = []
        dList = [
            self._runRegisterClasses(worker)
            for worker in self.queue.workers()]
        return defer.DeferredList(dList)
    
    def run(self, jobID, callName, *args, **kw):
        """
        Runs the specified I{jobID} by putting a call to the specified callable
        object in the job's namespace, with any supplied arguments and
        keywords, into the queue.

        Scheduling of the job is impacted by the niceness of the job itself. As
        with UNIX niceness, the value should be an integer where 0 is normal
        scheduling, negative numbers are higher priority, and positive numbers
        are lower priority. Calls for a job having niceness N+10 are dispatched
        at approximately half the rate of calls for a job with niceness N.

        All keywords except for the following are passed to the call:

          - B{timeout}: A timeout interval in seconds from when a worker
            gets a task assignment for the call, after which the call will be
            retried.
          
        @note: The task object generated contains the name of a callable (as a
          string) for the first element of its I{callTuple} attribute, instead
          of a callable itself.
        
        @return: A deferred to the eventual result of the call when it is
          eventually pulled from the queue and run.

        """
        def queueJob(doNext=False):
            if doNext:
                kw['doNext'] = True
            dq = self.queue.call(callName, *args, **kw)
            dq.addErrback(lambda failure: (False, failure.getTraceback()))
            dq.addCallback(jobRan)

        def jobRan(result):
            status, result = result
            if status:
                if d in self.callsPending.get(jobID, []):
                    del self.callsPending[jobID][d]
                    d.callback(result)
            elif result == 'Timeout':
                log("Timeout on job %d, retrying", jobID)
                tryAgain()
            else:
                log("Error running job %d:\n%s", jobID, result)
                tryAgain()

        def tryAgain():
            if jobID in self.callsPending:
                retryCount, callName, args, kw = self.callsPending[jobID][d]
                if retryCount < self.maxRetries:
                    self.callsPending[jobID][d][0] = retryCount + 1
                    queueJob(True)
                    return
            d.callback(None)

        jobID = int(jobID)
        if jobID not in self.jobs:
            raise ValueError("No job '%s' registered" % jobID)
        kw['series'] = jobID
        kw['niceness'] = self.jobs[jobID][1]
        d = defer.Deferred()
        self.callsPending[jobID][d] = [0, callName, args, kw]
        queueJob()
        return d

    def cancel(self, jobID):
        """
        Cancels the specified I{jobID} and any jobs that may be queued for
        it. If the job doesn't exist, no error is raised.
        """
        self.queue.cancelSeries(jobID)
        self.jobs.pop(jobID, None)
        self.updates.pop(jobID, None)
        self.callsPending.pop(jobID, None)
        dList = [
            worker.remoteCaller('forgetJob', jobID)
            for worker in self.queue.workers()]
        return defer.DeferredList(dList)
