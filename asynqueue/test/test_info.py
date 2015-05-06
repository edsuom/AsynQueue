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
Unit tests for asynqueue.info
"""

from copy import copy
import random, threading
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

import util, info
from testbase import deferToDelay, blockingTask, Picklable, TestCase


class TestFunctions(TestCase):
    verbose = False

    def setUp(self):
        self.hashes = []

    def checkHash(self, obj):
        x = info.hashIt(obj)
        self.assertNotIn(x, self.hashes)
        self.hashes.append(x)
    
    def test_hashIt(self):
        self.checkHash(None)
        self.checkHash("abc")
        self.checkHash("def")
        self.checkHash(1)
        self.checkHash(2.0)
        self.checkHash(self.checkHash)
        self.checkHash([1, 2])
        self.checkHash((3, 4))
        self.checkHash({1:2, 2:'a'})
        

class TestInfo(TestCase):
    verbose = False

    def setUp(self):
        self.p = Picklable()
        self.info = info.Info(remember=True)

    def _foo(self, x):
        return 2*x

    def _bar(self, x, y=0):
        return x+y
        
    def test_getID(self):
        IDs = []
        fakList = [
            ['_foo', (1,), {}],
            ['_foo', (2,), {}],
            ['_bar', (3,), {}],
            ['_bar', (3,), {'y': 1}],
        ]
        for fak in fakList:
            nfak = copy(fak)
            nfak[0] = getattr(self, fak[0])
            ID = self.info.setCall(*nfak).ID
            self.assertNotIn(ID, IDs)
            IDs.append(ID)
        for k, ID in enumerate(IDs):
            callDict = self.info.getInfo(ID, 'callDict')
            fs = callDict['fs']
            self.assertIn(fakList[k][0], fs)
            self.assertTrue(callable(callDict['f']))
            for kk, name in enumerate(('args', 'kw')):
                self.assertEqual(callDict[name], fakList[k][kk+1])
        
    def _divide(self, x, y):
        return x/y

    def test_nn(self):
        # Module-level function
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
                ('foo\(1\)', self.p.foo, (1,), {}),
                ('foo\(2\)', self.p.foo, (2,), {}),
                ('_bar\(1, y=2\)', self._bar, (1,), {'y':2}),
        ):
            ID = self.info.setCall(f, args, kw).ID
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

    def test_context(self):
        @defer.inlineCallbacks
        def tryInfo(null, k):
            with self.info.context(self.p.foo, k) as x:
                yield deferToDelay(0.1)
                # Now get the info later, after someone else has
                # likely done their own setCall
                callInfo = x.aboutCall()
                self.assertPattern("foo\({:d}\)".format(k), callInfo)
        dList = []
        for k in xrange(5):
            dList.append(deferToDelay(
                0.5*random.random()).addCallback(tryInfo, k))
        return defer.DeferredList(dList).addCallback(
            lambda _: self.assertEqual(len(self.info.pastInfo), 0))

        
        
            
                
