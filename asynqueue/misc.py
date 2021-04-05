# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
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
Miscellaneous stuff that is needed for C{test_wire} and nothing else.
"""

from asynqueue.wire import WireWorkerUniverse


class TestMethods:
    chars = "abcdefghijklmnopqrst"
    
    def add(self, x, y):
        return x+y
    def divide(self, x, y):
        return x/y
    def setStuff(self, N1, N2):
        self.stuff = []
        for j in range(N1):
            stuff = "".join(
                [self.chars[k % len(self.chars)]
                 for k in range(N2)])
            self.stuff.append(stuff.encode())
    def getStuff(self):
        return self.stuff
    def stuffSize(self):
        return len(self.stuff)
    def stufferator(self):
        for chunk in self.stuff:
            yield chunk
    def blockingTask(self, x, delay):
        import time
        time.sleep(delay)
        return 2*x


class TestUniverse(TestMethods, WireWorkerUniverse):
    pass
