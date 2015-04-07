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

import random, threading
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

import util, info
from testbase import deferToDelay, blockingTask, Picklable, TestCase


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
