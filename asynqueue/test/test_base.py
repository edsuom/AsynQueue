# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
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
Unit tests for asynqueue.base
"""

import logging

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from .. import base
from .testbase import DeferredIterable, MockWorker, TestCase


VERBOSE = False


class Test_Priority(TestCase):
    def setUp(self):
        self.heap = base.Priority()

    def test_getInOrder(self):
        dList = []
        for item in (2,1,4,0,3):
            self.heap.put(item)
        for item in range(5):
            d = self.heap.get()
            d.addCallback(self.failUnlessEqual, item)
            dList.append(d)
        return defer.DeferredList(dList)

    def test_getBeforePut(self):
        dList, items = [], []
        for item in range(5):
            self.heap.put(item)
        for item in range(5):
            d = self.heap.get()
            d.addCallback(items.append)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            lambda _: self.failUnlessEqual(sum(items), 10))
    
    def test_cancel(self):
        dList, items = [], []
        for item in range(5):
            self.heap.put(item)
        self.heap.cancel(lambda x: x is 2)
        for item in range(4):
            d = self.heap.get()
            d.addCallback(items.append)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            lambda _: self.failUnlessEqual(items, [0,1,3,4]))


class Test_TaskQueue(TestCase):
    verbose = False
    spew = False
    
    def setUp(self):
        self.queue = base.TaskQueue(
            spew=self.spew, verbose=self.isVerbose())

    def tearDown(self):
        return self.queue.shutdown()

    def test_oneTask(self):
        worker = MockWorker(0.5)
        self.queue.attachWorker(worker)
        d = self.queue.call(lambda x: 2*x, 15)
        d.addCallback(self.failUnlessEqual, 30)
        return d

    def test_oneWorker(self):
        N = 30
        mutable = []

        def checkResults(null):
            self.failUnlessEqual(len(mutable), N)
            self.failUnlessEqual(
                sum(mutable),
                sum([2*x for x in range(N)]))

        worker = MockWorker(0.01)
        self.queue.attachWorker(worker)
        dList = []
        for x in range(N):
            d = self.queue.call(lambda y: 2*y, x)
            d.addCallback(lambda result: mutable.append(result))
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d

    def test_multipleWorkers(self):
        N = 100
        mutable = []

        def checkResults(null):
            self.failUnlessEqual(len(mutable), N)
            self.failUnlessEqual(
                sum(mutable),
                sum([2*x for x in range(N)]))

        IDs = []
        for runDelay in (0.05, 0.1, 0.4):
            worker = MockWorker(runDelay)
            ID = self.queue.attachWorker(worker)
            self.assertNotIn(ID, IDs)
            IDs.append(ID)
        dList = []
        for x in range(N):
            d = self.queue.call(lambda y: 2*y, x)
            d.addCallback(lambda result: mutable.append(result))
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d

    def test_update(self):
        def done(results):
            self.assertEqual(len(results), 3)
            self.assertEqual(results, [3.0]*3)
        for runDelay in (0.05, 0.1, 0.4):
            worker = MockWorker(runDelay, verbose=self.isVerbose())
            self.queue.attachWorker(worker)
        return self.queue.update(lambda x: 2*x, 1.5).addCallback(done)
        
    @defer.inlineCallbacks
    def test_iteration(self):
        # MockWorker doesn't work for this
        import threads
        worker = threads.ThreadWorker()
        self.queue.attachWorker(worker)
        for N in (0, 1, 10):
            dr = yield self.queue.call(range, N)
            count = 0
            for k, d in enumerate(dr):
                x = yield d
                self.assertEqual(x, k, "For k={:d}, N={:d}".format(k, N))
                count += 1
            self.assertEqual(
                count, N,
                "Expected {:d} iterations, got {:d}".format(N, count))


class Test_TaskQueueErrors(TestCase):
    verbose = False
    
    class TestHandler(logging.Handler):
        def __init__(self, verbose):
            logging.Handler.__init__(self)
            self.verbose = verbose
            self.records = []
        def emit(self, record):
            self.records.append(record)
            if self.verbose:
                print("LOGGED: {}".format(record.getMessage()))
    
    def setUp(self):
        self.handler = self.TestHandler(self.verbose)
        logging.getLogger('asynqueue').addHandler(self.handler)
    
    def tearDown(self):
        return self.queue.shutdown()

    def timesTwo(self, x):
        return 2*x
        
    def bogusCall(self):
        raise Exception("Test error")
        return

    def newQueue(self, **kw):
        #from threads import ThreadWorker
        #worker = ThreadWorker()
        worker = MockWorker(0.01)
        self.queue = base.TaskQueue(**kw)
        self.queue.attachWorker(worker)

    def checkMessage(self, message):
        self.assertIn("Exception 'Test error'", message)
        self.assertIn("bogusCall", message)
        self.msg("Log message:\n{}", message)

    def test_failure(self):
        def done(result):
            self.assertIsInstance(result, Failure)
            self.checkMessage(result.getErrorMessage())
            delayedCalls = reactor.getDelayedCalls()
        self.newQueue()
        return self.queue.call(
            self.bogusCall, returnFailure=True).addBoth(done)
        
    def test_stop(self):
        def done(text):
            self.assertEqual(len(self.handler.records), 0)
            self.checkMessage(text)
            delayedCalls = reactor.getDelayedCalls()
            # One for the reactor shutdown, and one for the timeout
            self.assertEqual(len(delayedCalls), 2)
            delayedCalls[0].cancel()
        # TODO: Redirect stdout, stderr for duration of test
        self.newQueue()
        return self.queue.call(self.bogusCall).addCallback(done)
        
    def test_warn(self):
        def done(text):
            self.assertEqual(len(self.handler.records), 1)
            self.checkMessage(self.handler.records[0].getMessage())
        self.newQueue(warn=True)
        return self.queue.call(self.bogusCall).addCallback(done)

    def test_warn_verbose(self):
        def done(text):
            self.assertEqual(len(self.handler.records), 1)
            self.checkMessage(self.handler.records[0].getMessage())
        self.newQueue(warn=True, verbose=True)
        return self.queue.call(self.bogusCall).addCallback(done)

    @defer.inlineCallbacks
    def test_spew(self):
        N = 10
        self.newQueue(spew=True)
        for x in range(N):
            y = yield self.queue.call(self.timesTwo, x)
            self.assertEqual(y, 2*x)
        self.assertEqual(len(self.handler.records), N)
        for x, thisRecord in enumerate(self.handler.records):
            self.assertIn(
                "timesTwo({:d}) -> {:d}".format(x, 2*x),
                thisRecord.getMessage())
        
            
        
        
        
