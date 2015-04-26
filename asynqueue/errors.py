"""
Custom Exceptions

B{AsynQueue} provides asynchronous task queueing based on the Twisted
framework, with task prioritization and a powerful worker
interface. Worker implementations are included for running tasks
asynchronously in the main thread, in separate threads, and in
separate Python interpreters (multiprocessing).

Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
U{http://edsuom.com/}. This program is free software: you can
redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version. This
program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details. You should have received a copy of the GNU General
Public License along with this program.  If not, see
U{http://www.gnu.org/licenses/}.

@author: Edwin A. Suominen
"""

from zope.interface import Invalid


class QueueRunError(Exception):
    """
    An attempt was made to dispatch tasks when the dispatcher isn't running.
    """

class ImplementationError(Exception):
    """
    There was a problem implementing the required interface.
    """

class NotReadyError(ImplementationError):
    """
    You shouldn't have called yet!
    """
        
class InvariantError(Invalid):
    """
    An invariant of the IWorker provider did not meet requirements.
    """
    def __repr__(self):
        return "InvariantError(%r)" % self.args

class TimeoutError(Exception):
    """
    A local worker took too long to provide a result.
    """

class WorkerError(Exception):
    """
    A worker ran into an exception trying to run a task.
    """

class ThreadError(Exception):
    """
    A function call in a thread raised an exception.
    """
