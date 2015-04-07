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

import random, threading
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

import util
from testbase import deferToDelay, blockingTask, Picklable, TestCase


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


