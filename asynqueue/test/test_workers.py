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
Unit tests for asynqueue.workers
"""

import time, random, threading
import multiprocessing as mp
import zope.interface
from twisted.internet import defer

from testbase import deferToDelay, TestCase, base, workers, errors


class TestThreadWorker(TestCase):
    verbose = True
    
    def setUp(self):
        self.worker = workers.ThreadWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _blockingTask(self, x):
        delay = random.uniform(0.1, 1.0)
        self.msg(
            "Running {:f} sec. task in thread {}",
            delay, threading.currentThread().getName())
        time.sleep(delay)
        return 2*x

    def testShutdown(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.call(self._blockingTask, 0)
        d.addCallback(lambda _: self.queue.shutdown())
        d.addCallback(checkStopped)
        return d

    def testShutdownWithoutRunning(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.shutdown()
        d.addCallback(checkStopped)
        return d

    def testStop(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.call(self._blockingTask, 0)
        d.addCallback(lambda _: self.worker.stop())
        d.addCallback(checkStopped)
        return d

    def testOneTask(self):
        d = self.queue.call(self._blockingTask, 15)
        d.addCallback(self.assertEqual, 30)
        return d

    def testMultipleWorkers(self):
        N = 20
        mutable = []

        def gotResult(result):
            self.msg("Task result: {}", result)
            mutable.append(result)

        def checkResults(null):
            self.assertEqual(len(mutable), N)
            self.assertEqual(
                sum(mutable),
                sum([2*x for x in xrange(N)]))

        # Create and attach two more workers, for a total of three
        for null in xrange(2):
            worker = workers.ThreadWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(self._blockingTask, x)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d


class TestAsyncWorker(TestCase):
    verbose = False
    
    def setUp(self):
        self.worker = workers.AsyncWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _twistyTask(self, x):
        delay = random.uniform(1.5, 2.0)
        self.msg("Running {:f} sec. async task", delay)
        return deferToDelay(delay).addCallback(lambda _: 2*x)
        
    def test_call(self):
        d = self.queue.call(self._twistyTask, 2)
        d.addCallback(self.assertEqual, 4)
        return d


class TestProcessWorker(TestCase):
    def setUp(self):
        self.worker = workers.ProcessWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _twistyTask(self, x):
        delay = random.uniform(1.5, 2.0)
        self.msg(
            "Running {:f} sec. async task in process {}",
            delay, self.worker.pName)
        return deferToDelay(delay).addCallback(lambda _: 2*x)
        
    def _blockingTask(self, x):
        delay = random.uniform(1.5, 2.0)
        self.msg(
            "Running {:f} sec. blocking task in process {}",
            delay, self.worker.pName)
        time.sleep(delay)
        return 2*x

    def checkStopped(self, null):
        self.assertFalse(self.worker.process.is_alive())
            
    def testShutdown(self):
        d = self.queue.call(blockingTask, 0)
        d.addCallback(lambda _: self.queue.shutdown())
        d.addCallback(self.checkStopped)
        return d

    def testShutdownWithoutRunning(self):
        d = self.queue.shutdown()
        d.addCallback(self.checkStopped)
        return d

    def testStop(self):
        d = self.queue.call(blockingTask, 0)
        d.addCallback(lambda _: self.worker.stop())
        d.addCallback(self.checkStopped)
        return d

    def testOneTask(self):
        d = self.queue.call(blockingTask, 15)
        d.addCallback(self.assertEqual, 30)
        return d

    def testMultipleWorkers(self):
        N = 20
        mutable = []

        def gotResult(result):
            if VERBOSE:
                print "Task result: %s" % result
            mutable.append(result)

        def checkResults(null):
            self.assertEqual(len(mutable), N)
            self.assertEqual(
                sum(mutable),
                sum([2*x for x in xrange(N)]))

        # Create and attach two more workers, for a total of three
        for null in xrange(2):
            worker = workers.ProcessWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(blockingTask, x)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d
