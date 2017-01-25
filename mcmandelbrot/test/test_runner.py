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
Unit tests for mcmandelbrot.runner
"""

import png

from twisted.internet import defer

from mcmandelbrot import runner
from mcmandelbrot.test.testbase import deferToDelay, FakeFile, TestCase


class TestRunner(TestCase):
    verbose = True

    def setUp(self):
        self.r = runner.Runner(1000, 3)

    def tearDown(self):
        return self.r.shutdown()

    @defer.inlineCallbacks
    def test_run_basic(self):
        N = 100
        fh = FakeFile(verbose=self.isVerbose())
        runInfo = yield self.r.run(fh, N, -0.630, 0, 1.4, 1.4)
        self.assertEqual(runInfo[1], N*N)
        pngReader = png.Reader(bytes="".join(fh.data))
        width, height, pixels, metadata = pngReader.read()
        self.assertEqual(width, N)
        self.assertEqual(height, N)
