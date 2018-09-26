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
A dead-simple L{TaskQueue} that queues up and runs non-blocking
tasks in the Twisted main loop.
"""

from asynqueue.base import TaskQueue
from asynqueue.workers import AsyncWorker


class NullQueue(TaskQueue):
    """
    A L{TaskQueue} that runs tasks in the Twisted main loop.
    
    """
    def __init__(self, **kw):
        """
        @param kw: Keywords for the regular L{TaskQueue} constructor.
        """
        TaskQueue.__init__(self, **kw)
        worker = AsyncWorker()
        self.attachWorker(worker)
