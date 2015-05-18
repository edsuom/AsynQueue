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
Unit tests for asynqueue.workers
"""

from twisted.internet import defer

import wire
from testbase import deferToDelay, TestCase


class TestMandelbrotWorkerUniverse(TestCase):
    verbose = True

    def setUp(self):
        self.mwu = wire.MandelbrotWorkerUniverse()
        return self.mwu.setup(1000, 1)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mwu.runner.shutdown()
    
    @defer.inlineCallbacks
    def test_basic(self):
        N = 100
        fhAsIterator = self.mwu.run(N, -0.630, 0, 1.4, 1.4)
        rows = []
        for k, row in enumerate(fhAsIterator):
            self.msg("{:03d}: {:d} bytes", k, len(row))
            rows.append(row)
        fhAsIterator.close()
        self.assertEqual(len(rows), N)
        
