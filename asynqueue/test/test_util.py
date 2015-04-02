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
Unit tests for asynqueue.util
"""

import time, random, threading
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

from testbase import TestCase, errors, util, iteration, deferToDelay


class Picklable(object):
    classValue = 1.2

    def __init__(self):
        self.x = 0

    def foo(self, y):
        self.x += y

    def __eq__(self, other):
        return (
            self.classValue == other.classValue
            and
            self.x == other.x
        )


class TestFunctions(TestCase):
    verbose = False

    def test_pickling(self):
        pObj = Picklable()
        pObj.foo(3.2)
        pObj.foo(1.2)
        objectList = [None, "Some text!", 37311, -1.37, Exception, pObj]
        for obj in objectList:
            pickleString = util.o2p(obj)
            self.assertIsInstance(pickleString, str)
            roundTrip = util.p2o(pickleString)
            self.assertEqual(obj, roundTrip)
        self.assertEqual(roundTrip.x, 4.4)


class TestInfo(TestCase):
    verbose = False

    def setUp(self):
        self.p = Picklable()
        self.info = util.Info(remember=True)

    def _foo(self, x):
        return 2*x

    def _bar(self, x, y=0):
        return x+y
        
    def test_getID(self):
        IDs = []
        fakList = [
            (self._foo, (1,), {}),
            (self._foo, (2,), {}),
            (self._bar, (3,), {}),
            (self._bar, (3,), {'y': 1}),
        ]
        for fak in fakList:
            ID = self.info.setCall(*fak).getID()
            self.assertNotIn(ID, IDs)
            IDs.append(ID)
        for k, ID in enumerate(IDs):
            self.assertEqual(
                self.info.getInfo(ID, 'callTuple'),
                fakList[k])
        
    def _divide(self, x, y):
        return x/y

    def test_nn(self):
        def bogus():
            pass
        
        # Bogus
        ns, fn = self.info.setCall(bogus).nn()
        self.assertEqual(ns, None)
        self.assertEqual(fn, None)
        # Module-level function
        from test_workers import blockingTask
        ns, fn = self.info.setCall(blockingTask).nn()
        self.assertEqual(ns, None)
        self.assertEqual(util.p2o(fn), blockingTask)
        # Method, pickled
        stuff = util.TestStuff()
        ns, fn = self.info.setCall(stuff.accumulate).nn()
        self.assertIsInstance(util.p2o(ns), util.TestStuff)
        self.assertEqual(fn, 'accumulate')
        # Method by fqn string
        ns, fn = self.info.setCall("util.testFunction").nn()
        self.assertEqual(ns, None)
        self.assertEqual(fn, "util.testFunction")
        
    def test_aboutCall(self):
        IDs = []
        pastInfo = []
        for pattern, f, args, kw in (
                ('[cC]allable!', None, (), {}),
                ('\.foo\(1\)', self.p.foo, (1,), {}),
                ('\.foo\(2\)', self.p.foo, (2,), {}),
                ('\._bar\(1, y=2\)', self._bar, (1,), {'y':2}),
        ):
            ID = self.info.setCall(f, args, kw).getID()
            self.assertNotIn(ID, IDs)
            IDs.append(ID)
            text = self.info.aboutCall()
            pastInfo.append(text)
            self.assertPattern(pattern, text)
        # Check that the old info is still there
        for k, ID in enumerate(IDs):
            self.assertEqual(self.info.aboutCall(ID), pastInfo[k])

    def test_aboutException(self):
        try:
            self._divide(1, 0)
        except Exception as e:
            text = self.info.aboutException()
        self.msg(text)
        self.assertPattern('Exception ', text)
        self.assertPattern('[dD]ivi.+by zero', text)


class TestDeferredTracker(TestCase):
    verbose = False

    def setUp(self):
        self.dt = util.DeferredTracker()

    def _slowStuff(self, N, delay=None, maxDelay=0.2):
        dList = []
        for k in xrange(N):
            if delay is None:
                delay = maxDelay*random.random()
            d = deferToDelay(delay)
            d.addCallback(lambda _: k)
            dList.append(d)
        return dList
    
    @defer.inlineCallbacks
    def test_basic(self):
        # Nothing in there yet, immediate
        yield self.dt.deferToAll()
        yield self.dt.deferToLast()
        # Put some in and wait for them
        for d in self._slowStuff(3):
            self.dt.put(d)
        yield self.dt.deferToAll()
        # Put some in with the same delay and defer to the last one
        for d in self._slowStuff(3, delay=0.5):
            self.dt.put(d)
        x = yield self.dt.deferToLast()
        self.assertEqual(x, 2)
        yield self.dt.deferToAll()
    

class ToyConsumer(object):
    implements(IConsumer)

    def __init__(self, verbose):
        self.verbose = verbose
    
    def registerProducer(self, *args):
        self.x = []
        self.registered = args
        if self.verbose:
            print "Registered:", args

    def unregisterProducer(self):
        self.registered = None

    def write(self, data):
        if self.verbose:
            print "Wrote:", data
        self.x.append(data)


class Stuff(object):
    def divide(self, x, y, delay=0.2):
        time.sleep(delay)
        return x/y

    def iterate(self, N, maxDelay=0.2):
        for k in xrange(N):
            if maxDelay > 0:
                time.sleep(maxDelay*random.random())
            yield k

    def fiveSeconderator(self):
        def oneSecond():
            t0 = time.time()
            while time.time() - t0 < 1.0:
                time.sleep(0.05)
            return time.time() - t0
        for k in xrange(5):
            time.sleep(0.1)
            yield oneSecond()


class TestThreadLooper(TestCase):
    verbose = False

    def setUp(self):
        self.stuff = Stuff()
        self.resultList = []
        self.t = util.ThreadLooper()

    def tearDown(self):
        return self.t.stop()
        
    def test_loop(self):
        self.assertTrue(self.t.threadRunning)
        self.t.callTuple = None
        self.t.event.set()
        return deferToDelay(0.2).addCallback(
            lambda _: self.assertFalse(self.t.threadRunning))
            
    @defer.inlineCallbacks
    def test_call_OK_once(self):
        status, result = yield self.t.call(self.stuff.divide, 10, 2, delay=0.3)
        self.assertEqual(status, 'r')
        self.assertEqual(result, 5)

    def _gotOne(self, sr):
        self.assertEqual(sr[0], 'r')
        self.resultList.append(sr[1])

    def _checkResult(self, null, expected):
        self.assertEqual(self.resultList, expected)
        
    def test_call_multi_OK(self):
        dList = []
        for x in (2, 4, 8, 10):
            d = self.t.call(self.stuff.divide, x, 2, delay=0.2*random.random())
            d.addCallback(self._gotOne)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            self._checkResult, [1, 2, 4, 5])

    def test_call_doNext(self):
        dList = []
        for num, delay, doNext in (
                (3, 0.4, False), (6, 0.1, False), (12, 0.1, True)):
            d = self.t.call(
                self.stuff.divide, num, 3, delay=delay, doNext=doNext)
            d.addCallback(self._gotOne)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            self._checkResult, [1, 4, 2])

    @defer.inlineCallbacks
    def test_call_error_once(self):
        status, result = yield self.t.call(self.stuff.divide, 1, 0)
        self.assertEqual(status, 'e')
        self.msg("Expected error message:", '-')
        self.msg(result)
        self.assertPattern(r'\.divide', result)
        self.assertPattern(r'[dD]ivi.+zero', result)

    @defer.inlineCallbacks
    def test_iterator_basic(self):
        for k in xrange(100):
            N = random.randrange(5, 20)
            self.msg("Repeat #{:d}, iterating {:d} times...", k+1, N)
            status, result = yield self.t.call(self.stuff.iterate, N, 0)
            self.assertEqual(status, 'i')
            resultList = []
            for d in iteration.Deferator(result):
                item = yield d
                resultList.append(item)
            self.assertEqual(resultList, range(N))
    
    @defer.inlineCallbacks
    def test_iterator_fast(self):
        status, result = yield self.t.call(self.stuff.iterate, 10)
        self.assertEqual(status, 'i')
        dRegular = self.t.call(self.stuff.divide, 3.0, 2.0)
        resultList = []
        for d in iteration.Deferator(result):
            item = yield d
            resultList.append(item)
        self.assertEqual(resultList, range(10))
        status, result = yield dRegular
        self.assertEqual(status, 'r')
        self.assertEqual(result, 1.5)

    @defer.inlineCallbacks
    def test_iterator_slow(self):
        status, result = yield self.t.call(self.stuff.fiveSeconderator)
        self.assertEqual(status, 'i')
        dRegular = self.t.call(self.stuff.divide, 3.0, 2.0)
        resultList = []
        for d in iteration.Deferator(result):
            item = yield d
            resultList.append(item)
        self.assertEqual(len(resultList), 5)
        self.assertApproximates(sum(resultList), 5.0, 0.2)
        status, result = yield dRegular
        self.assertEqual(status, 'r')
        self.assertEqual(result, 1.5)
        
    def test_deferToThread_OK(self):
        def done(result):
            self.assertEqual(result, 5)
        def oops(failureObj):
            self.fail("Shouldn't have gotten here!")
        return self.t.deferToThread(
            self.stuff.divide, 10, 2).addCallbacks(done, oops)
        
    def test_deferToThread_error(self):
        def done(result):
            self.fail("Shouldn't have gotten here!")
        def oops(failureObj):
            self.assertPattern(r'[dD]ivi', failureObj.getErrorMessage())
        return self.t.deferToThread(
            self.stuff.divide, 1, 0).addCallbacks(done, oops)
    
    def test_deferToThread_iterator(self):
        @defer.inlineCallbacks
        def done(p):
            self.msg("Call to iterator returned: {}", repr(p))
            c = ToyConsumer(self.verbose)
            p.registerConsumer(c)
            self.assertEqual(c.registered, (p, True))
            yield p.run()
            self.assertEqual(len(c.x), 5)
            self.assertApproximates(sum(c.x), 5.0, 0.2)
            self.assertEqual(c.registered, None)

        return self.t.deferToThread(
            self.stuff.fiveSeconderator).addCallback(done)
        
