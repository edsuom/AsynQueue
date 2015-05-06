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
Unit tests for asynqueue.tasks
"""

import copy
import zope.interface
from twisted.internet import defer, reactor

import tasks, errors, workers
from testbase import MockTask, MockWorker, TestCase


VERBOSE = False


class TestTask(TestCase):
    def taskFactory(self, priority, series=0):
        return tasks.Task(lambda _: None, (None,), {}, priority, series)

    def test_constructorWithValidArgs(self):
        func = lambda : None
        task = tasks.Task(func, (1,), {2:3}, 100, None)
        self.failUnlessEqual(task.callTuple, (func, (1,), {2:3}))
        self.failUnlessEqual(task.priority, 100)
        self.failUnlessEqual(task.series, None)
        self.failUnless(isinstance(task.d, defer.Deferred))

    def test_constructorWithBogusArgs(self):
        self.failUnlessRaises(
            TypeError, tasks.Task, lambda : None, 1, {2:3}, 100, None)
        self.failUnlessRaises(
            TypeError, tasks.Task, lambda : None, (1,), 2, 100, None)

    def test_priorityOtherTask(self):
        taskA = self.taskFactory(0)
        taskB = self.taskFactory(1)
        taskC = self.taskFactory(1.1)
        self.failUnless(taskA < taskB)
        self.failUnless(taskB < taskC)
        self.failUnless(taskA < taskC)
    
    def test_priorityOtherNone(self):
        taskA = self.taskFactory(10000)
        self.failUnless(taskA < None)


class TestTaskFactory(TestCase):
    verbose = False
    
    def setUp(self):
        self.tf = tasks.TaskFactory(MockTask)

    def listInOrder(self, theList):
        self.msg(
            "Serial Numbers:\n{}\n",
            ", ".join([str(x) for x in theList]))
        unsorted = copy.copy(theList)
        theList.sort()
        self.failUnlessEqual(theList, unsorted)

    def test_serialOneSeries(self):
        serialNumbers = []
        for null in xrange(5):
            this = self.tf._serial(None)
            self.failUnless(isinstance(this, float))
            self.failIf(this in serialNumbers)
            serialNumbers.append(this)
        self.listInOrder(serialNumbers)

    def test_serialMultipleSeriesConcurrently(self):
        serialNumbers = []
        for null in xrange(5):
            x = self.tf._serial(1)
            y = self.tf._serial(2)
            serialNumbers.extend([x,y])
        self.failUnlessEqual(
            serialNumbers,
            [1, 2, 2, 3, 3, 4, 4, 5, 5, 6 ])
        #    x0 y0 x1 y1 x2 y2 x3 y3 x4 y4
        
    def test_serialAnotherSeriesComingLate(self):
        serialNumbers = []
        for null in xrange(5):
            x = self.tf._serial(1)
            serialNumbers.append(x)
        for null in xrange(5):
            y = self.tf._serial(2)
            serialNumbers.append(y)
        self.listInOrder(serialNumbers)


class TestAssignmentFactory(TestCase):
    def setUp(self):
        self.af = tasks.AssignmentFactory()
    
    def test_requestBasic(self):
        OriginalAssignment = tasks.Assignment
        
        class ModifiedAssignment(tasks.Assignment):
            mutable = []
            def accept(self, worker):
                self.mutable.append(worker)
                self.d.callback(None)

        def finishUp(null, worker):
            self.failUnlessEqual(ModifiedAssignment.mutable, [worker])
            tasks.Assignment = OriginalAssignment

        tasks.Assignment = ModifiedAssignment
        task = MockTask(lambda x: 10*x, (2,), {}, 100, None)
        worker = MockWorker()
        worker.hired = False
        self.af.request(worker, None)
        for dList in worker.assignments.itervalues():
            self.failUnlessEqual(
                [isinstance(x, defer.Deferred) for x in dList], [True])
        d = self.af.new(task)
        d.addCallback(finishUp, worker)
        return d

    def test_requestAndAccept(self):
        task = MockTask(lambda x: 10*x, (2,), {}, 100, None)
        worker = MockWorker()
        worker.hired = False
        self.af.request(worker, None)
        d = defer.DeferredList([self.af.new(task), task.d])
        d.addCallback(lambda _: self.failUnlessEqual(worker.ran, [task]))
        return d


class TestTaskHandlerHiring(TestCase):
    verbose = False
    
    def setUp(self):
        self.th = tasks.TaskHandler()

    def tearDown(self):
        return self.th.shutdown()
        
    def test_hireRejectBogus(self):
        class AttrBogus(object):
            zope.interface.implements(workers.IWorker)
            cQualified = 'foo'

        self.failUnlessRaises(
            errors.ImplementationError, self.th.hire, None)
        self.failUnlessRaises(
            errors.InvariantError, self.th.hire, AttrBogus())

    def _checkAssignments(self, workerID):
        worker = self.th.workers[workerID]
        assignments = getattr(worker, 'assignments', {})
        for key in assignments.iterkeys():
            self.failUnlessEqual(assignments.keys().count(key), 1)
        self.failUnless(isinstance(assignments, dict))
        for assignment in assignments.itervalues():
            self.failUnlessEqual(
                [True for x in assignment
                 if isinstance(x, defer.Deferred)], [True])

    @defer.inlineCallbacks
    def test_hireSetWorkerID(self):
        worker = MockWorker()
        workerID = yield self.th.hire(worker)
        self.failUnlessEqual(getattr(worker, 'ID', None), workerID)

    @defer.inlineCallbacks
    def test_hireClassQualifications(self):
        class CQWorker(MockWorker):
            cQualified = ['foo']

        worker = CQWorker()
        workerID = yield self.th.hire(worker)
        self._checkAssignments(workerID)

    @defer.inlineCallbacks
    def test_hireInstanceQualifications(self):
        worker = MockWorker()
        worker.iQualified = ['bar']
        workerID = yield self.th.hire(worker)
        self._checkAssignments(workerID)

    @defer.inlineCallbacks
    def test_hireMultipleWorkersThenShutdown(self):
        ID_1 = yield self.th.hire(MockWorker())
        ID_2 = yield self.th.hire(MockWorker())
        self.failIfEqual(ID_1, ID_2)
        self.failUnlessEqual(len(self.th.workers), 2)
        d = self.th.shutdown()
        d.addCallback(lambda _: self.failUnlessEqual(self.th.workers, {}))

    def _callback(self, result, msg, order=None, value=None):
        self._count += 1
        if self._count == 1:
            self.msg("#{:d}: {} -> {}", self._count, msg, str(result))
        if order is not None:
            self.assertEqual(
                self._count, order,
                "Call '{}' was #{:}, expected it to be #{:d}".format(
                    msg, self._count, order))
        if value is not None:
            self.assertEqual(result, value)

    @defer.inlineCallbacks
    def test_terminateGracefully(self):
        self._count = 0
        worker = MockWorker()
        workerID = yield self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = task.d
        d2.addCallback(self._callback, "Task done", 2)
        d3 = self.th.terminate(workerID)
        d3.addCallback(self._callback, "Worker terminated", 3)
        yield defer.DeferredList([d1,d2,d3])

    @defer.inlineCallbacks
    def test_terminateAfterTimeout(self):
        self._count = 0
        worker = MockWorker(runDelay=2.0)
        workerID = yield self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = self.th.terminate(workerID, timeout=1.0)
        d2.addCallback(self._callback, "Worker terminated", 2, value=[task])
        yield defer.DeferredList([d1,d2])
        self.assertFalse(task.d.called)
    
    @defer.inlineCallbacks
    def test_terminateBeforeTimeout(self):
        self._count = 0
        worker = MockWorker(runDelay=1.0)
        workerID = yield self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = self.th.terminate(workerID, timeout=2.0)
        d2.addCallback(self._callback, "Worker terminated", 2, value=[])
        yield defer.DeferredList([d1,d2])
        self.assertTrue(task.d.called)

    @defer.inlineCallbacks
    def test_terminateByCrashing(self):
        self._count = 0
        worker = MockWorker(runDelay=1.0)
        workerID = yield self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        yield self.th(task)
        result = yield self.th.terminate(workerID, crash=True)
        self.assertEqual(result, [task])
        self.assertFalse(task.d.called)
        

class TestTaskHandlerRun(TestCase):
    def setUp(self):
        self.th = tasks.TaskHandler()

    def tearDown(self):
        return self.th.shutdown()

    def test_oneWorker(self):
        worker = MockWorker(0.2)
        N = 10

        def completed(null):
            self.failUnlessEqual(
                [type(x) for x in worker.ran], [MockTask]*N)

        self.th.hire(worker)
        dList = []
        for null in xrange(N):
            task = MockTask(lambda x: x, ('foo',), {}, 100, None)
            # For this test, we don't care about when assignments are accepted
            self.th(task)
            # We only care about when they are done
            dList.append(task.d)
        d = defer.DeferredList(dList)
        d.addCallback(completed)
        return d

    def test_multipleWorkers(self):
        N = 50
        mutable = []
        workerFast = MockWorker(0.1)
        workerSlow = MockWorker(0.2)

        def checkResults(null):
            self.failUnlessEqual(len(mutable), N)
            self.failUnlessApproximates(
                2*len(workerSlow.ran), len(workerFast.ran), 2)
            
        self.th.hire(workerFast)
        self.th.hire(workerSlow)
        dList = []
        for null in xrange(N):
            task = MockTask(lambda : mutable.append(None), (), {}, 100, None)
            # For this test, we don't care about when assignments are accepted
            self.th(task)
            # We only care about when they are done
            dList.append(task.d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d

