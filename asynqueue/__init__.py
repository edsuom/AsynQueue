# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Priority queueing of tasks to one or more workers.

Workers can run asynchronously in the main thread
(L{workers.AsyncWorker}, for non-blocking tasks), in one or more
threads (L{workers.ThreadWorker}), or on one or more subordinate
Python processes (L{workers.ProcessWorker}).
"""

from workers import *
from base import TaskQueue
from threads import ThreadQueue
from process import ProcessQueue
from info import showResult, Info
from util import DeferredTracker, DeferredLock
