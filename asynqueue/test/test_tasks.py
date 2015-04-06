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

    def testConstructorWithValidArgs(self):
        func = lambda : None
        task = tasks.Task(func, (1,), {2:3}, 100, None)
        self.failUnlessEqual(task.callTuple, (func, (1,), {2:3}))
        self.failUnlessEqual(task.priority, 100)
        self.failUnlessEqual(task.series, None)
        self.failUnless(isinstance(task.d, defer.Deferred))

    def testConstructorWithBogusArgs(self):
        self.failUnlessRaises(
            TypeError, tasks.Task, lambda : None, 1, {2:3}, 100, None)
        self.failUnlessRaises(
            TypeError, tasks.Task, lambda : None, (1,), 2, 100, None)

    def testPriorityOtherTask(self):
        taskA = self.taskFactory(0)
        taskB = self.taskFactory(1)
        taskC = self.taskFactory(1.1)
        self.failUnless(taskA < taskB)
        self.failUnless(taskB < taskC)
        self.failUnless(taskA < taskC)
    
    def testPriorityOtherNone(self):
        taskA = self.taskFactory(10000)
        self.failUnless(taskA < None)


class TestTaskFactory(TestCase):
    def setUp(self):
        self.tf = tasks.TaskFactory(MockTask)

    def listInOrder(self, theList):
        if VERBOSE:
            strList = [str(x) for x in theList]
            print "\nSerial Numbers:\n%s\n" % ", ".join(strList)
        unsorted = copy.copy(theList)
        theList.sort()
        self.failUnlessEqual(theList, unsorted)

    def testSerialOneSeries(self):
        serialNumbers = []
        for null in xrange(5):
            this = self.tf._serial(None)
            self.failUnless(isinstance(this, float))
            self.failIf(this in serialNumbers)
            serialNumbers.append(this)
        self.listInOrder(serialNumbers)

    def testSerialMultipleSeriesConcurrently(self):
        serialNumbers = []
        for null in xrange(5):
            x = self.tf._serial(1)
            y = self.tf._serial(2)
            serialNumbers.extend([x,y])
        self.failUnlessEqual(
            serialNumbers,
            [1, 2, 2, 3, 3, 4, 4, 5, 5, 6 ])
        #    x0 y0 x1 y1 x2 y2 x3 y3 x4 y4
        
    def testSerialAnotherSeriesComingLate(self):
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
    
    def testRequestBasic(self):
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

    def testRequestAndAccept(self):
        task = MockTask(lambda x: 10*x, (2,), {}, 100, None)
        worker = MockWorker()
        worker.hired = False
        self.af.request(worker, None)
        d = defer.DeferredList([self.af.new(task), task.d])
        d.addCallback(lambda _: self.failUnlessEqual(worker.ran, [task]))
        return d


class TestTaskHandlerHiring(TestCase):
    def setUp(self):
        self.th = tasks.TaskHandler()

    def testHireRejectBogus(self):
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

    def testHireSetWorkerID(self):
        worker = MockWorker()
        workerID = self.th.hire(worker)
        self.failUnlessEqual(getattr(worker, 'ID', None), workerID)

    def testHireClassQualifications(self):
        class CQWorker(MockWorker):
            cQualified = ['foo']

        worker = CQWorker()
        workerID = self.th.hire(worker)
        self._checkAssignments(workerID)
        
    def testHireInstanceQualifications(self):
        worker = MockWorker()
        worker.iQualified = ['bar']
        workerID = self.th.hire(worker)
        self._checkAssignments(workerID)
    
    def testHireMultipleWorkersThenShutdown(self):
        ID_1 = self.th.hire(MockWorker())
        ID_2 = self.th.hire(MockWorker())
        self.failIfEqual(ID_1, ID_2)
        self.failUnlessEqual(len(self.th.workers), 2)
        d = self.th.shutdown()
        d.addCallback(lambda _: self.failUnlessEqual(self.th.workers, {}))
        return d

    def _callback(self, result, msg, order=None, value=None):
        self._count += 1
        if VERBOSE:
            if self._count == 1:
                print "\n"
            print "#%d: %s -> %s" % (self._count, msg, str(result))
        if order is not None:
            self.failUnlessEqual(self._count, order)
        if value is not None:
            self.failUnlessEqual(result, value)

    def testTerminateGracefully(self):
        self._count = 0
        worker = MockWorker()
        workerID = self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = task.d
        d2.addCallback(self._callback, "Task done", 2)
        d3 = self.th.terminate(workerID)
        d3.addCallback(self._callback, "Worker terminated", 3)
        return defer.gatherResults([d1,d2,d3])

    def testTerminateAfterTimeout(self):
        def checkTask(null):
            self.failIf(task.d.called)
        
        self._count = 0
        worker = MockWorker(runDelay=2.0)
        workerID = self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = self.th.terminate(workerID, timeout=1.0)
        d2.addCallback(self._callback, "Worker terminated", 2, value=[task])
        return defer.gatherResults([d1,d2]).addCallback(checkTask)

    def testTerminateBeforeTimeout(self):
        def checkTask(null):
            self.failUnless(task.d.called)
        
        self._count = 0
        worker = MockWorker(runDelay=1.0)
        workerID = self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d1 = self.th(task)
        d1.addCallback(self._callback, "Assignment accepted", 1)
        d2 = self.th.terminate(workerID, timeout=2.0)
        d2.addCallback(self._callback, "Worker terminated", 2, value=[])
        return defer.gatherResults([d1,d2]).addCallback(checkTask)

    def testTerminateByCrashing(self):
        def checkTask(null):
            self.failIf(task.d.called)
        
        self._count = 0
        worker = MockWorker(runDelay=1.0)
        workerID = self.th.hire(worker)
        task = MockTask(lambda x: x, ('foo',), {}, 100, None)
        d = self.th(task)
        d.addCallback(self._callback, "Assignment accepted", 1)
        d.addCallback(lambda _: self.th.terminate(workerID, crash=True))
        d.addCallback(self._callback, "Worker terminated", 2, value=[task])
        return d.addCallback(checkTask)


class TestTaskHandlerRun(TestCase):
    def setUp(self):
        self.th = tasks.TaskHandler()

    def tearDown(self):
        return self.th.shutdown()

    def testOneWorker(self):
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

    def testMultipleWorkers(self):
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

