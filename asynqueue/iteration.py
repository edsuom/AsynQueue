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
from twisted.python.failure import Failure
from twisted.internet.interfaces import IPushProducer, IConsumer

import errors
# Almost everybody in the package imports this module, so it can
# import very little from the package.


def deferToDelay(delay):
    return Delay(delay)()

def isIterator(x):
    return Deferator.isIterator(x)

# Debug
from info import showResult

    
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
            self.backoff = backoff
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
    dict, etc.). I yield deferreds to the next iteration of the result
    and maintain an internal deferred that fires when the iterations
    are done or terminated cleanly with a call to my L{stop}
    method. The deferred fires with C{True} if the iterations were
    completed, or C{False} if not, i.e., a stop was done.

    Access the done-iterating deferred via my I{d} attribute. I also
    try to provide access to its methods attributes and attributes as
    if they were my own.

    When the deferred from my first L{next} call fires, with the first
    iteration of the underlying (possibly remote) iterable, you can
    call L{next} again to get a deferred to the next one, and so on,
    until I raise L{StopIteration} just like a regular iterable.

    B{NOTE}: There are two very important rules. First, you MUST wrap
    my iteration in a L{defer.inlineCallbacks} loop or otherwise wait
    for each yielded deferred to fire before asking for the next
    one. Second, you must call the 'stop' method of the Deferator (or
    the deferreds it yields) before doing a 'stop' or 'return' to
    prematurely terminate the loop. Good behavior looks something like
    this:

    @defer.inlineCallbacks
    def printItems(self, ID):
        for d in Deferator("remoteIterator", getMore, ID):
            listItem = yield d
            print listItem
            if listItem == "Danger Will Robinson":
                d.stop()
                # You still have to break out of the loop after calling
                # the deferator's stop method
                return

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
    first and sole argument, or an iterator for which a Prefetcherator
    will be constructed internally.
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
        self.d = defer.Deferred()
        self.moreLeft = True
        if isinstance(objOrRep, (str, unicode)):
            # Use supplied string representation
            self.representation = objOrRep.strip('<>')
        else:
            # Use repr of the object itself
            self.representation = repr(objOrRep)
        if args:
            # A callTuple was supplied
            self.callTuple = args[0], args[1:], kw
            return
        if isinstance(objOrRep, Prefetcherator):
            # A Prefetcherator was supplied
            self.callTuple = (objOrRep.getNext, [], {})
            return
        if self.isIterator(objOrRep):
            # An iterator was supplied for which I will make my own
            # Prefetcherator
            pf = Prefetcherator()
            if pf.setup(objOrRep):
                self.callTuple = (pf.getNext, [], {})
                return
        # Nothing worked; make me equivalent to an empty iterator
        self.moreLeft = False
        self.representation = repr([])
        # The non-existent iteration was "complete" since nothing was
        # terminated prematurely.
        self._callback(True)
    
    def __repr__(self):
        return "<Deferator wrapping of\n  <{}>,\nat 0x{}>".format(
            self.representation, format(id(self), '012x'))

    def __getattr__(self, name):
        """
        Provides access to my done-iterating deferred's attributes as if
        they were my own.
        """
        if name == 'd':
            raise AttributeError("No internal deferred is defined!")
        return getattr(self.d, name)

    def _callback(self, wasCompleteIteration):
        if not self.d.called:
            self.d.callback(wasCompleteIteration)
        
    # Iterator implementation
    #--------------------------------------------------------------------------
    
    def __iter__(self):
        return self

    def next(self):
        def gotNext(result):
            value, isValid, self.moreLeft = result
            return value
        
        if self.moreLeft:
            if hasattr(self, 'dIterate') and not self.dIterate.called:
                raise errors.NotReadyError(
                    "You didn't wait for the last deferred to fire!")
            f, args, kw = self.callTuple
            self.dIterate = f(*args, **kw).addCallback(gotNext)
            self.dIterate.stop = self.stop
            return self.dIterate
        if hasattr(self, 'dIterate'):
            del self.dIterate
        self._callback(True)
        raise StopIteration

    def stop(self):
        """
        You must call this to cleanly break out of a loop of my iterations.

        Not part of the official iterator implementation, but
        necessary for a deferred way of iterating. You need a way of
        letting whatever is producing the iterations know that there
        won't be any more of them.

        For convenience, each deferred that I yield during iteration
        has a reference to this method via its own 'stop' attribute.
        """
        self.moreLeft = False
        self._callback(False)


class Prefetcherator(object):
    """
    I prefetch iterations from an iterator, providing a L{getNext}
    method suitable for L{Deferator}.

    You can supply an ID for me, purely to provide a more informative
    representation and something you can retrieve via my I{ID}
    attribute.
    """
    __slots__ = [
        'ID', 'nextCallTuple', 'lastFetch']

    def __init__(self, ID=None):
        self.ID = ID

    def __repr__(self):
        text = "<Prefetcherator instance '{}'".format(self.ID)
        if self.isBusy():
            text += "\n with nextCallTuple '{}'>".format(
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
                iterator = args[0]
                if not hasattr(iterator, 'next'):
                    iterator = iter(iterator)
                if not hasattr(iterator, 'next'):
                    raise AttributeError(
                        "Can't get a nextCallTuple from so-called "+\
                        "iterator '{}'".format(repr(args[0])))
                self.nextCallTuple = (iterator.next, [], {})
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

    def __init__(self, dr, consumer=None):
        if not isinstance(dr, Deferator):
            raise TypeError("Object {} is not a Deferator".format(repr(dr)))
        self.dr = dr
        self.delay = Delay()
        if consumer is not None:
            self.registerConsumer(consumer)

    def deferUntilDone(self):
        """
        Returns a deferred that fires when I am done producing iterations.
        """
        d= defer.Deferred()
        self.dr.chainDeferred(d)
        return d
            
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
        Produces my iterations, returning a deferred that fires with
        C{None} when they are done.
        """
        if not hasattr(self, 'consumer'):
            raise AttributeError("Can't run without a consumer registered")
        self.paused = False
        self.running = True
        for d in self.dr:
            # Pause/stop opportunity after the last item write (if
            # any) and before the deferred fires
            if not self.running:
                break
            if self.paused:
                yield self.delay.untilEvent(lambda: not self.paused)
            item = yield d
            # Another pause/stop opportunity before the item write
            if not self.running:
                break
            if self.paused:
                yield self.delay.untilEvent(lambda: not self.paused)
            # Write the item and do the next iteration
            self.consumer.write(item)
        # Done with the iteration, and with producer/consumer
        # interaction
        self.consumer.unregisterProducer()
        defer.returnValue(None)
            
    def pauseProducing(self):
        self.paused = True

    def resumeProducing(self):
        self.paused = False

    def stopProducing(self):
        self.running = False
        self.dr.stop()


@defer.inlineCallbacks
def iteratorToProducer(iterator, consumer=None, wrapper=None):
    """
    Converts a possibly slow-running iterator into a Twisted-friendly
    producer, returning a deferred that fires with the producer when
    it's ready. If the the supplied object is not a suitable iterator
    (perhaps empty), the result will be C{None}.

    If a consumer is not supplied, whatever consumer gets this must
    register with the producer by calling its non-interface method
    L{IterationProducer.registerConsumer} and then its
    L{IterationProducer.run} method to start the iteration/production.

    If you supply a consumer, those two steps will be done
    automatically, and this method will fire with a deferred that
    fires when the iteration/production is done.
    """
    result = None
    if Deferator.isIterator(iterator):
        pf = Prefetcherator()
        ok = yield pf.setup(iterator)
        if ok:
            if wrapper:
                if callable(wrapper):
                    args = (wrapper, pf.getNext)
                else:
                    result = Failure(TypeError(
                        "Wrapper '{}' is not a callable".format(
                            repr(wrapper))))
            else:
                args = (pf.getNext,)
            dr = Deferator(repr(iterator), *args)
            result = IterationProducer(dr, consumer)
            if consumer:
                yield result.run()
    defer.returnValue(result)
