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

import time, random
from twisted.internet import defer

from ..util import TestStuff
from .. import base, tasks, workers
from .testbase import deferToDelay, TestCase, IterationConsumer


class TestAsyncWorker(TestCase):
    verbose = True
    
    def setUp(self):
        self.worker = workers.AsyncWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _twistyTask(self, x):
        delay = random.uniform(0.1, 0.5)
        self.msg("Running {:f} sec. async task", delay)
        return deferToDelay(delay).addCallback(lambda _: 2*x)
        
    def test_call(self):
        d = self.queue.call(self._twistyTask, 2)
        d.addCallback(self.assertEqual, 4)
        return d

    def test_multipleTasks(self):
        N = 5
        expected = [2*x for x in range(N)]
        for k in self.multiplerator(N, expected):
            self.d = self.queue.call(self._twistyTask, k)
        return self.dm

    def test_multipleCalls(self):
        N = 5
        expected = [(b'r', 2*x) for x in range(N)]
        worker = workers.AsyncWorker()
        for k in self.multiplerator(N, expected):
            task = tasks.Task(self._twistyTask, (k,), {}, 0, None)
            self.d = task.d
            worker.run(task)
        return self.dm.addCallback(lambda _: worker.stop())

    @defer.inlineCallbacks
    def test_iteration(self):
        N1, N2 = 50, 100
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        dr = yield self.queue.call(stuff.stufferator)
        chunks = []
        for d in dr:
            chunk = yield d
            self.assertEqual(len(chunk), N1)
            chunks.append(chunk)
        self.assertEqual(len(chunks), N2)

    @defer.inlineCallbacks
    def test_iterationProducer(self):
        N1, N2 = 50, 100
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        consumer = IterationConsumer(self.verbose)
        yield self.queue.call(stuff.stufferator, consumer=consumer)
        for chunk in consumer.data:
            self.assertEqual(len(chunk), N1)
        self.assertEqual(len(consumer.data), N2)
