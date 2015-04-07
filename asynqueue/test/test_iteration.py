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

import random
from copy import copy

from twisted.internet import defer

import errors, iteration
from testbase import TestCase, IterationConsumer


generator = (2*x for x in range(10))

def generatorFunction(x, N=7):
    for y in xrange(N):
        yield x*y


class DeferredIterable(object):
    def __init__(self, x):
        self.x = x

    def next(self):
        d = iteration.deferToDelay(0.3*random.random())
        d.addCallback(lambda _: self.x.pop(0))
        return d
        

class IteratorGetter(object):
    def __init__(self, x):
        self.x = x
        self.dHistory = []

    def getNext(self, slowness=0.5):
        if self.x:
            x = self.x.pop()
            d = iteration.deferToDelay(slowness*random.random())
            d.addCallback(lambda _ : (x, True, len(self.x) > 0))
        else:
            d = defer.succeed((None, False, False))
        self.dHistory.append(d)
        return d


class TestDeferator(TestCase):
    verbose = False

    def test_isIterator(self):
        isIterator = iteration.Deferator.isIterator
        for unSuitable in (None, "xyz", (1,2), [3,4], {1:'a', 2:'b'}):
            self.assertFalse(isIterator(unSuitable))
        for suitable in (generator, generatorFunction):
            self.assertTrue(isIterator(suitable), repr(suitable))

    def test_repr(self):
        x = range(10)
        repFunction = repr(generatorFunction)
        df = iteration.Deferator(repFunction, lambda _: x.pop())
        rep = repr(df)
        self.msg(rep)
        self.assertPattern("Deferator wrapping", rep)

    @defer.inlineCallbacks
    def test_iterates(self):
        x = [5, 4, 3, 2, 1, 0]
        ig = IteratorGetter(x)
        df = iteration.Deferator(repr(ig), ig.getNext, slowness=0.4)
        for k, d in enumerate(df):
            value = yield d
            self.msg("Item #{:d}: {}", k+1, value)
            self.assertEqual(value, k)
    

class TestPrefetcherator(TestCase):
    verbose = False

    @defer.inlineCallbacks
    def test_setIterator(self):
        pf = iteration.Prefetcherator()
        for unSuitable in (None, "xyz", (1,2), [3,4], {1:'a', 2:'b'}):
            status = yield pf.setup(unSuitable)
            self.assertFalse(status)
        status = yield pf.setup(generatorFunction(4))
        self.assertTrue(status)
        self.assertEqual(pf.lastFetch, (0, True))

    @defer.inlineCallbacks
    def test_setNextCallable(self):
        di = DeferredIterable([7, 13, 21])
        pf = iteration.Prefetcherator()
        status = yield pf.setup(di.next)
        self.assertTrue(status)
        self.assertEqual(pf.lastFetch, (7, True))

    @defer.inlineCallbacks
    def test_getNext_withIterator(self):
        iterator = generatorFunction(6, N=5)
        pf = iteration.Prefetcherator("gf")
        yield pf.setup(iterator)
        k = 0
        self.msg(" k val\tisValid\tmoreLeft", "-")
        while True:
            value, isValid, moreLeft = yield pf.getNext()
            self.msg(
                "{:2d}:  {}\t{}\t{}", k, value,
                "+" if isValid else "0",
                "+" if moreLeft else "0")
            self.assertEqual(value, k*6)
            self.assertTrue(isValid)
            if k < 4:
                self.assertTrue(moreLeft)
            else:
                self.assertFalse(moreLeft)
                break
            k += 1
        value, isValid, moreLeft = yield pf.getNext()
        self.assertFalse(isValid)
        self.assertFalse(moreLeft)

    @defer.inlineCallbacks
    def test_getNext_withNextCallable_immediate(self):
        listOfStuff = ["57", None, "1.3", "whatever"]
        pf = iteration.Prefetcherator()
        status = yield pf.setup(iter(listOfStuff).next)
        self.assertTrue(status)
        k = 0
        self.msg(" k{}\tisValid\tmoreLeft", " "*10, "-")
        while True:
            value, isValid, moreLeft = yield pf.getNext()
            self.msg(
                "{:2d}:{:>10s}\t{}\t{}", k, value,
                "+" if isValid else "0",
                "+" if moreLeft else "0")
            self.assertEqual(value, listOfStuff[k])
            self.assertTrue(isValid)
            if k < len(listOfStuff)-1:
                self.assertTrue(moreLeft)
            else:
                self.assertFalse(moreLeft)
                break
            k += 1
        value, isValid, moreLeft = yield pf.getNext()
        self.assertFalse(isValid)
        self.assertFalse(moreLeft)

    @defer.inlineCallbacks
    def test_getNext_withNextCallable_deferred(self):
        listOfStuff = ["57", None, "1.3", "whatever"]
        di = DeferredIterable(copy(listOfStuff))
        pf = iteration.Prefetcherator()
        status = yield pf.setup(di.next)
        self.assertTrue(status)
        k = 0
        self.msg(" k{}\tisValid\tmoreLeft", " "*10, "-")
        while True:
            value, isValid, moreLeft = yield pf.getNext()
            self.msg(
                "{:2d}:{:>10s}\t{}\t{}", k, value,
                "+" if isValid else "0",
                "+" if moreLeft else "0")
            self.assertEqual(value, listOfStuff[k])
            self.assertTrue(isValid)
            if k < len(listOfStuff)-1:
                self.assertTrue(moreLeft)
            else:
                self.assertFalse(moreLeft)
                break
            k += 1
        value, isValid, moreLeft = yield pf.getNext()
        self.assertFalse(isValid)
        self.assertFalse(moreLeft)
        
    @defer.inlineCallbacks
    def test_withDeferator(self):
        N = 5
        expected = range(0, 3*N, 3)
        iterator = generatorFunction(3, N=N)
        pf = iteration.Prefetcherator()
        status = yield pf.setup(iterator)
        self.assertTrue(status)
        dr = iteration.Deferator(None, pf.getNext)
        self.msg(
            "expected: {}",
            ", ".join((str(x) for x in expected)))
        self.msg(" k  value", "-")
        for k, d in enumerate(dr):
            value = yield d
            self.msg("{:2d}  {:2d}", k, value)
            self.assertEqual(value, expected[k])
        
        
