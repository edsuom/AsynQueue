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

import time, inspect

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer, IConsumer

import errors


def deferToDelay(delay):
    return Delay(delay)()


class Delay(object):
    """
    I let you delay things and wait for things that may take a while,
    in Twisted fashion.

    Perhaps a bit more suited to the util module, but that would
    require this module to import it, and it imports this module.
    """
    interval = 0.01
    backoff = 1.04

    def __init__(self, interval=None, backoff=None, timeout=None):
        if interval:
            self.interval = interval
        if backoff:
            self.backoff = backoof
        if timeout:
            self.timeout = timeout
        if self.backoff < 1.0 or self.backoff > 1.3:
            raise ValueError(
                "Unworkable backoff {:f}, keep it in 1.0-1.3 range".format(
                    self.backoff))

    def __call__(self, delay=None):
        if delay is None:
            delay = self.interval
        d = defer.Deferred()
        reactor.callLater(delay, d.callback, None)
        return d

    @defer.inlineCallbacks
    def untilEvent(self, eventChecker):
        """
        Returns a deferred that fires when a call to the supplied
        event-checking callable returns an affirmative (not C{None},
        C{False}, etc.) result, or until the optional timeout limit is
        reached.

        The result of the deferred is C{True} if the event actually
        happened, or C{False} if a timeout occurred.

        The event checker should *not* return a deferred. Calls the
        event checker less and less frequently as the wait goes on,
        depending on the backoff exponent (default is 1.04).
        """
        if not callable(eventChecker):
            raise TypeError("You must supply a callable event checker")

        t0 = time.time()
        interval = self.interval
        while True:
            if eventChecker():
                defer.returnValue(True)
                break
            if hasattr(self, 'timeout') and time.time()-t0 > self.timeout:
                defer.returnValue(False)
                break
            # No response yet, check again after the poll interval,
            # which increases exponentially so that each incremental
            # delay is somewhat proportional to the amount of time
            # spent waiting thus far.
            yield self(interval)
            interval *= self.backoff


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

    Instantiate me with a string representation of the underlying
    iterable (or the object itself, if it's handy) and a function
    (along with any args and kw) that returns a deferred to a 3-tuple
    containing (1) the next value yielded from the task result, (2) a
    Bool indicating if this value is valid or a bogus first one from
    an empty iterator, and (3) a Bool indicating whether there are
    more iterations left.

    This requires your get-more function to be one step ahead somehow,
    returning C{False} as its status indicator when the *next* call
    would raise L{StopIteration}. Use L{Prefetcherator.getNext} after
    setting the prefetcherator up with a suitable iterator or
    next-item callable.

    The object (or string representation) isn't strictly needed; it's
    for informative purposes in case an error gets propagated back
    somewhere. You can cheat and just use C{None} for the first
    constructor argument. Or you can supply a Prefetcherator as the
    first and sole argument.
    
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
        if inspect.isgenerator(obj) or inspect.isgeneratorfunction(obj):
            return True
        try:
            iter(obj)
        except:
            result = False
        else:
            result = True
        return result

    def __init__(self, objOrRep, *args, **kw):
        self.moreLeft = True
        if isinstance(objOrRep, (str, unicode)):
            self.representation = objOrRep.strip('<>')
        else:
            self.representation = repr(objOrRep)
            if isinstance(objOrRep, Prefetcherator):
                self.callTuple = (objOrRep.getNext, [], {})
                return
        self.callTuple = args[0], args[1:], kw 

    def __repr__(self):
        return "<Deferator wrapping of\n  <{}>,\nat 0x{}>".format(
            self.representation, format(id(self), '012x'))

    def __iter__(self):
        return self
        
    def next(self):
        def gotNext(result):
            value, isValid, self.moreLeft = result
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

    You can supply an ID for me and a callWhenDone function that will
    be called when iterations are done, i.e., when the third item in
    the L{getNext} return value is False.
    """
    __slots__ = [
        'ID', 'nextCallTuple', 'lastFetch', 'callWhenDone']

    def __init__(self, ID=None, callWhenDone=None):
        self.ID = ID
        if callWhenDone is not None:
            self.callWhenDone = callWhenDone

    def __repr__(self):
        text = "<Prefetcherator instance '{}'".format(self.ID)
        if self.isBusy:
            text += " with nextCallTuple '{}'>".format(
                repr(self.nextCallTuple))
        else:
            text += ">"
        return text
            
    def isBusy(self):
        return hasattr(self, 'nextCallTuple')

    def setup(self, *args, **kw):
        """
        Set me up with a new iterator, or the callable for an
        iterator-like-object, along with any args or keywords. Does a
        first prefetch, returning a deferred that fires with C{True}
        if all goes well or C{False} otherwise.
        """
        def parseArgs():
            if not args:
                return False
            if Deferator.isIterator(args[0]):
                self.nextCallTuple = (args[0].next, [], {})
                return True
            if callable(args[0]):
                self.nextCallTuple = (args[0], args[1:], kw)
                return True
            return False

        def done(result):
            self.lastFetch = result
            return result[1]
        
        if self.isBusy() or not parseArgs():
            return defer.succeed(False)
        return self._tryNext().addCallback(done)
        
    def _tryNext(self):
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
        f, args, kw = self.nextCallTuple
        return defer.maybeDeferred(f, *args, **kw).addCallbacks(done, oops)
    
    def _callCallWhenDone(self):
        if hasattr(self, 'callWhenDone'):
            self.callWhenDone()
            del self.callWhenDone

    def getNext(self):
        """
        Gets the next value from my current iterator, or a deferred value
        from my current nextCallTuple, returning it along with a Bool
        indicating if this is a valid value and another one indicating
        if more values are left.

        Once a prefetch returns a bogus value, the result of this call
        will remain (None, False, False), until a new iterator or
        nextCallable is set.

        Use this method as the callable (second constructor argument)
        of L{Deferator}.
        """
        def done(thisFetch):
            nextIsValid = thisFetch[1]
            if not nextIsValid:
                if hasattr(self, 'lastFetch'):
                    del self.lastFetch
                self._callCallWhenDone()
                # This call's value is valid, but there's no more
                return value, True, False
            # This call's value is valid and there is more to come
            result = value, True, True
            self.lastFetch = thisFetch
            return result

        value, isValid = getattr(self, 'lastFetch', (None, False))
        if not isValid:
            # The last prefetch returned a bogus value, and obviously
            # no more are left now.
            return defer.succeed((None, False, False))
        # The prefetch of this call's value was valid, so try a
        # prefetch for a possible next call after this one.
        return self._tryNext().addCallback(done)


class IterationProducer(object):
    """
    I am a producer of iterations from a L{Deferator}. Get me running
    with a call to L{run}, which returns a deferred that fires when
    I'm done iterating or when the consumer has stopped me, whichever
    comes first.
    """
    implements(IPushProducer)

    checkInterval = 0.1

    def __init__(self, dr, consumer=None):
        if not isinstance(dr, Deferator):
            raise TypeError("Object {} is not a Deferator".format(repr(dr)))
        self.dr = dr
        self.delay = Delay()
        if consumer is not None:
            self.setConsumer(consumer)

    def registerConsumer(self, consumer):
        """
        How could we push to a consumer without knowing what it is?
        """
        if not IConsumer.providedBy(consumer):
            raise errors.ImplementationError(
                "Object {} isn't a consumer".format(repr(consumer)))
        try:
            consumer.registerProducer(self, True)
        except RuntimeError:
            # Ignore any exception raised from a consumer already
            # having registered me.
            pass
        self.consumer = consumer
    
    @defer.inlineCallbacks
    def run(self):
        """
        Produces the iterations, returning a deferred that fires when the
        iterations are done.
        """
        if not hasattr(self, 'consumer'):
            raise AttributeError("Can't run without a consumer registered")
        self.paused = False
        self.running = True
        for d in self.dr:
            # Pause/stop opportunity before the deferred fires
            if not self.running:
                break
            if self.paused:
                yield self.delay.untilEvent(lambda: not self.paused)
            item = yield d
            # Pause/stop opportunity before the item write
            if not self.running:
                break
            if self.paused:
                yield self.delay.untilEvent(lambda: not self.paused)
            # Write the item and do the next iteration
            self.consumer.write(item)
        # Done with the iteration, and with producer/consumer
        # interaction
        self.consumer.unregisterProducer()
            
    def pauseProducing(self):
        self.paused = True

    def resumeProducing(self):
        self.paused = False

    def stopProducing(self):
        self.running = False


def iteratorToProducer(iterator, consumer=None):
    """
    Converts a possibly slow-running iterator into a Twisted-friendly
    producer.

    If a consumer is not supplied, whatever consumer gets this must
    register with the returned producer by calling its non-interface
    method L{IterationProducer.registerConsumer}.

    In any case, the iterations/production starts with a call to its
    L{IterationProducer.run} method.
    
    """
    pf = Prefetcherator()
    rIterator = repr(iterator)
    if not pf.setup(iterator):
        raise TypeError(
            "Object {} is not a suitable iterator".format(rIterator))
    dr = Deferator(rIterator, pf.getNext)
    return IterationProducer(dr, consumer)


def consumeIterations(iterator, consumer):
    """
    Has the supplied iterator write its items into the supplied
    consumer. Returns a deferred that fires when the iterations are
    done.
    """
    producer = iteratorToProducer(iterator, consumer)
    return producer.run()
    
