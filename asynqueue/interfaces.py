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
The worker interface.
"""

from zope.interface import invariant, Interface, Attribute
from twisted.internet.interfaces import IConsumer

from asynqueue import errors


class IWorker(Interface):
    """
    Provided by worker objects that can have tasks assigned to them for
    processing.

    All worker objects are considered qualified to run tasks of the
    default C{None} series. To indicate that subclasses or subclass
    instances are qualified to run tasks of user-defined series in
    addition to the default, the hashable object that identifies the
    additional series must be listed in the C{cQualified} or C{iQualified}
    class or instance attributes, respectively.
    """
    cQualified = Attribute(
        """
        A class-attribute list containing all series for which all instances
        of the subclass are qualified to run tasks.
        """)

    iQualified = Attribute(
        """
        An instance-attribute list containing all series for which the
        subclass instance is qualified to run tasks.
        """)

    def _check_qualifications(ob):
        """
        Qualification attributes must be present as lists.
        """
        for attrName in ('cQualified', 'iQualified'):
            x = getattr(ob, attrName, None)
            if not isinstance(x, list):
                raise errors.InvariantError(ob)
    invariant(_check_qualifications)

    def setResignator(callableObject):
        """
        Registers the supplied I{callableObject} to be called if the
        worker deems it necessary to resign, e.g., a remote connection
        has been lost.
        """

    def run(task):
        """
        Adds the task represented by the specified I{task} object to the list
        of tasks pending for this worker, to be run however and whenever
        the worker sees fit. However, workers are expected to run
        highest-priority tasks before anything else they have lined up in
        their mini-queues.

        Unless the worker is constructed with a C{raw=True} keyword or
        the task includes C{raw=True}, an iterator resulting from the
        task is converted into an instance of
        L{iteration.Deferator}. The underlying iteration (possibly
        across a pipe or wire) must be handled transparently to the
        user. If the task has a I{consumer} keyword set to an
        implementor of C{IConsumer}, an L{iteration.IterationProducer}
        coupled to that consumer will be the end result instead.

        Make sure that any callbacks you add to the task's internal
        deferred object C{task.d} return the callback argument. Otherwise,
        the result of your task will be lost in the callback chain.
        
        @return: A C{Deferred} that fires when the worker is ready to
          be assigned another task.
        """
        
    def stop():
        """
        Attempts to gracefully shut down the worker, returning a
        C{Deferred} that fires when the worker is done with all
        assigned tasks and will not cause any errors if the reactor is
        stopped or its object is deleted.

        The C{Deferred} returned by your implementation of this method
        must not fire until B{after} the results of all pending tasks
        have been obtained. Thus the deferred must be chained to each
        C{task.d} somehow.

        Make sure that any callbacks you add to the task's internal
        deferred object C{task.d} return the callback argument. Otherwise,
        the result of your task will be lost in the callback chain.
        """

    def crash():
        """
        Takes drastic action to shut down the worker, rudely and
        synchronously.

        @return: A list of I{task} objects, one for each task left
          uncompleted. You shouldn't have to call this method if no
          tasks are left pending; the L{stop} method should be enough
          in that case.

        """
