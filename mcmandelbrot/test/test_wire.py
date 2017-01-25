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

import png, time

from twisted.internet import defer

from asynqueue.threads import ThreadWorker

from mcmandelbrot import wire
from mcmandelbrot.test.testbase import \
    deferToDelay, FakeFile, MockWireWorker, TestCase


class TestMandelbrotWorkerUniverse(TestCase):
    verbose = False

    def setUp(self):
        self.mwu = wire.MandelbrotWorkerUniverse()
        return self.mwu.setup(1000, 1)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.mwu.shutdown()
        
    @defer.inlineCallbacks
    def test_basic(self):
        N = 100
        ID = self.mwu.run(N, -0.630, 0, 1.4, 1.4)
        chunks = []
        while True:
            chunk = yield self.mwu.getNext(ID)
            if chunk is None:
                break
            chunks.append(chunk)
        pngReader = png.Reader(bytes="".join(chunks))
        width, height, pixels, metadata = pngReader.read()
        self.assertEqual(width, N)
        self.assertEqual(height, N)
        timeSpent, N_pixels = yield self.mwu.done(ID)
        self.assertEqual(N_pixels, N*N)
        self.assertEqual(len(self.mwu.pendingRuns), 0)

    @defer.inlineCallbacks
    def test_cancel(self):
        N = 1000
        ID = self.mwu.run(N, -0.630, 0, 1.4, 1.4)
        yield deferToDelay(0.0)
        self.mwu.cancel(ID)
        runInfo = yield self.mwu.done(ID)
        self.msg("Partial run in {:f} seconds, {:d} pixels", *runInfo)
        self.assertLess(runInfo[1], N*N)


class TestRemoteRunner(TestCase):
    verbose = True
    
    def setUp(self):
        self.rr = wire.RemoteRunner()
        return self.rr.setup()#worker=MockWireWorker())

    def tearDown(self):
        return self.rr.shutdown()

    @defer.inlineCallbacks
    def test_run_basic(self):
        N = 100
        fh = FakeFile(verbose=self.isVerbose())
        runInfo = yield self.rr.run(
            fh, N, -0.630, 0, 1.4, 1.4)
        fh.close()
        self.assertEqual(runInfo[1], N*N)

        
