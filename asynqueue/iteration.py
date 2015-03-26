# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the file COPYING for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

"""
Iteration, Twisted style
"""

from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer, IConsumer

import errors


def deferToDelay(delay):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, None)
    return d


class Deferator(object):
    """
    Use an instance of me in place of a task result that is an
    iterable other than one of Python's built-in containers (list,
    dict, etc.). I yield deferreds to the next iteration of the
    result.

    When the deferred from my first L{next} call fires, with the first
    iteration of the underlying (possibly remote) iterable, you can
    call L{next} again to get a deferred to the next one, and so on,
    until I raise L{StopIteration} just like a regular iterable.

    You MUST wrap my iteration in a L{defer.inlineCallbacks} loop or
    otherwise wait for each yielded deferred to fire before asking for
    the next one. Something like this:

    @defer.inlineCallbacks
    def printItems(self, ID):
        for d in Deferator("remoteIterator", getMore, ID):
            listItem = yield d
            print listItem

    Instantiate me with the string representation of the underlying
    iterable and a function (along with any args and kw) that returns
    a deferred to a 3-tuple containing (1) the next value yielded from
    the task result, (2) a Bool indicating if this value is valid or a
    bogus first one from an empty iterator, and (3) a Bool indicating
    whether there are more iterations left.

    This requires your get-more function to be one step ahead somehow,
    returning C{False} as its status indicator when the *next* call
    would raise L{StopIteration}. Use L{Prefetcherator}.

    """
    builtIns = (
        str, unicode,
        list, tuple, bytearray, buffer, dict, set, frozenset)
    
    @classmethod
    def isIterator(cls, obj):
        """
        Returns C{True} if the object is an iterator suitable for use with
        me, C{False} otherwise.
        """
        if isinstance(obj, cls.builtIns):
            return False
        try:
            iter(obj)
        except:
            result = False
        else:
            result = True
        return result

    def __init__(self, representation, f, *args, **kw):
        self.representation = representation.strip('<>')
        self.callTuple = f, args, kw 
        self.moreLeft = True

    def __repr__(self):
        return "<Deferator wrapping of <{}>, at {}>".format(
            self.representation, id(self))

    def __iter__(self):
        return self
        
    def next(self):
        def gotNext(result):
            value, self.moreLeft = result
            return value
        
        if self.moreLeft:
            if hasattr(self, 'd') and not self.d.called:
                raise errors.NotReadyError(
                    "You didn't wait for the last deferred to fire!")
            f, args, kw = self.callTuple
            self.d = f(*args, **kw).addCallback(gotNext)
            return self.d
        raise StopIteration


class Prefetcherator(object):
    """
    I prefetch iterations from an iterator, providing a L{getNext}
    method suitable for L{Deferator}.
    """
    __slots__ = ['ID', 'iterator', 'nextCallTuple', 'lastFetch']

    def __init__(self, ID):
        self.ID = ID

    def isBusy(self):
        return hasattr(self, 'iterator') or hasattr(self, 'nextCallTuple')

    def _nextCall(self):
        f, args, kw = self.nextCallTuple
        return f(*args, **kw)
    
    def _tryNext(self):
        """
        Returns the next value from the iterator along with a Bool
        indicating if it's a valid one. Deletes the iterator when it
        runs empty.
        """
        if not hasattr(self, 'iterator'):
            return None, False
        try:
            value = self.iterator.next()
        except:
            del self.iterator
            value = None
            isValid = False
        else:
            isValid = True
        return value, isValid

    def _tryNextCall(self):
        """
        Returns a deferred that fires with the value from my
        I{nextCallTuple} along with a Bool indicating if it's a valid
        value. Deletes the nextValue reference after it returns with a
        failure.
        """
        def done(value):
            return value, True
        def oops(failureObj):
            del self.nextCallTuple
            return None, False
        if not hasattr(self, 'nextCallTuple'):
            return defer.succeed((None, False))
        return self._nextCall().addCallbacks(done, oops)

    def setIterator(self, iterator):
        """
        Give me a new iterator and a fresh start with an optimistic first
        prefetch. If all goes well, returns C{True}, or C{False}
        otherwise.
        """
        if self.isBusy():
            return False
        self.iterator = iterator
        self.lastFetch = self._tryNext()
        return self.lastFetch[1]

    def setNextCallable(self, f, *args, **kw):
        """
        Instead of an iterator, set a callable (along with any args/kw)
        that will return a deferred to the next iteration or a failure
        for L{StopIteration}. Start things off with a first
        prefetch. Returns a deferred that fires with C{True} if all
        goes well, or C{False} otherwise.
        """
        def done(value):
            self.lastFetch = value, True
            return True
        def oops(failureObj):
            del self.nextCallTuple
            self.lastFetch = None, False
            return False
        if self.isBusy():
            return defer.succeed(False)
        self.nextCallTuple = (f, args, kw)
        return self._nextCall().addCallbacks(done, oops)

    def _getNext_withIterator(self):
        """
        The iterator/immediate version of getNext
        """
        value, isValid = self.lastFetch
        if not isValid:
            # The last prefetch returned a bogus value, and obviously
            # no more are left now. You probably shouldn't have made
            # this call, though it can't be helped for iterators that
            # are empty from the start.
            return None, False, False
        # The prefetch of this call's value was valid, so try a
        # prefetch for a possible next call after this one.
        nextValue, isValid = self.lastFetch = self._tryNext()
        # If the prefetch wasn't valid, another call shouldn't be made.
        return value, True, isValid

    def _getNext_withCallable(self):
        """
        The callable/deferred version of getNext
        """
        def done(result):
            self.lastFetch = result
            nextValue, isValid = result
            # If the prefetch wasn't valid, another call shouldn't be made.
            return (value, True, isValid)

        value, isValid = self.lastFetch
        if not isValid:
            # The last prefetch returned a bogus value, and obviously
            # no more are left now. You probably shouldn't have made
            # this call, though it can't be helped for iterators that
            # are empty from the start.
            return defer.succeed((None, False, False))
        # The prefetch of this call's value was valid, so try a
        # prefetch for a possible next call after this one.
        return self._tryNextCall().addCallback(done)
        
    def getNext(self):
        """
        Gets the next value from my current iterator, or a deferred value
        from my current nextCallTuple, returning it along with a Bool
        indicating if this is a valid value and another one indicating
        if more values are left.
        """
        if hasattr(self, 'iterator'):
            if hasattr(self, 'nextCallTuple'):
                raise Exception(
                    "You can't define both an iterator and a nextCallTuple")
            return self._getNext_withIterator()
        if hasattr(self, 'nextCallTuple'):
            return self._getNext_withCallable()
        raise Exception("Neither an iterator nor a nextCallTuple is defined")



class IterationProducer(object):
    """
    I am a producer of iterations from a L{Deferator}. Get me running
    with a call to L{run}, which returns a deferred that fires when
    I'm done iterating or when the consumer has stopped me, whichever
    comes first.
    """
    implements(IPushProducer)

    checkInterval = 0.1

    def __init__(self, dr, consumer):
        if not isinstance(dr, Deferator):
            raise TypeError("Object {} is not a Deferator".format(repr(dr)))
        self.dr = dr
        if not IConsumer.providedBy(consumer):
            raise errors.ImplementationError(
                "Object {} isn't a consumer".format(repr(consumer)))
        self.consumer = consumer
        consumer.registerProducer(self, True)
    
    @defer.inlineCallbacks
    def run(self):
        self.paused = False
        self.running = True
        for d in self.dr:
            # Pause/stop opportunity before the deferred fires
            if not self.running:
                break
            while self.paused:
                yield deferToDelay(self.checkInterval)
            item = yield d
            # Pause/stop opportunity before the item write
            if not self.running:
                break
            while self.paused:
                yield deferToDelay(self.checkInterval)
            # Write the item and do the next iteration
            self.consumer.write(item)
            
    def pauseProducing(self):
        self.paused = True

    def resumeProducing(self):
        self.paused = False

    def stopProducing(self):
        self.running = False
