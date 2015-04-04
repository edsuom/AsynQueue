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
Unit tests for asynqueue.interfaces
"""

import multiprocessing as mp
import zope.interface
from twisted.internet import defer

import errors, interfaces
from testbase import TestCase


VERBOSE = True


class NoCAttr(object):
    zope.interface.implements(interfaces.IWorker)
    def __init__(self):
        self.iQualified = []

class NoIAttr(object):
    zope.interface.implements(interfaces.IWorker)
    cQualified = []

class AttrBogus(object):
    zope.interface.implements(interfaces.IWorker)
    cQualified = 'foo'
    def __init__(self):
        iQualified = 'bar'

class AttrOK(object):
    zope.interface.implements(interfaces.IWorker)
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
