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
Unit tests for asynqueue.interfaces
"""

import multiprocessing as mp
from zope.interface import implementer
from twisted.internet import defer

from asynqueue import errors, interfaces
from asynqueue.test.testbase import TestCase


VERBOSE = True


@implementer(interfaces.IWorker)
class NoCAttr(object):
    def __init__(self):
        self.iQualified = []

@implementer(interfaces.IWorker)
class NoIAttr(object):
    cQualified = []

@implementer(interfaces.IWorker)
class AttrBogus(object):
    cQualified = 'foo'
    def __init__(self):
        iQualified = 'bar'

@implementer(interfaces.IWorker)
class AttrOK(object):
    cQualified = ['foo']
    def __init__(self):
        self.iQualified = ['bar']


class TestIWorker(TestCase):
    def testInvariantCheckClassAttribute(self):
        worker = AttrOK()
        try:
            interfaces.IWorker.validateInvariants(worker)
        except:
            self.fail(
                "Acceptable class attribute shouldn't raise an exception")
        for worker in [x() for x in (NoCAttr, NoIAttr, AttrBogus)]:
            self.failUnlessRaises(
                errors.InvariantError,
                interfaces.IWorker.validateInvariants, worker)
    
    def testInvariantCheckInstanceAttribute(self):
        worker = AttrOK()
        for attr in (None, []):
            if attr is not None:
                worker.iQualified = attr
            try:
                interfaces.IWorker.validateInvariants(worker)
            except:
                self.fail(
                    "Acceptable instance attribute shouldn't raise exception")
        worker.iQualified = 'foo'
        self.failUnlessRaises(
            errors.InvariantError,
            interfaces.IWorker.validateInvariants, worker)
