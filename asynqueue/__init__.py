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
Priority queueing of tasks to one or more workers.

Workers can run asynchronously in the main thread
(L{workers.AsyncWorker}, for non-blocking tasks), in one or more
threads (L{workers.ThreadWorker}), or on one or more subordinate
Python processes (L{workers.ProcessWorker}).
"""

from __future__ import absolute_import

try:
    from .workers import *
    from .base import TaskQueue
    from .threads import ThreadQueue
    from .process import ProcessQueue
    from .info import showResult, Info
    from .util import DeferredTracker, DeferredLock
except:
    print("WARNING: Error doing convenience import of modules!")
    

# For Python Version agnosticism
# ----------------------------------------------------------------------------
class VA(object):
    """
    I provide version-agnostic attributes and a I{py3} attribute that
    indicates if we're running under Python 3.
    """
    def __init__(self):
        import sys
        if sys.version_info < (3, 0):
            # Python 2.x
            self.py3 = False
            self.buffer = buffer
            self.long = long
            self.unicode = unicode
            import cPickle as p
        else:
            # Python 3.x
            self.py3 = True
            self.buffer = memoryview
            self.long = int
            self.unicode = str
            import pickle as p
        self.pickle = p
# ----------------------------------------------------------------------------
va = VA()
