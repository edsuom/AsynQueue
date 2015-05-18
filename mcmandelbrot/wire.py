#!/usr/bin/env python
#
# mcmandelbrot
#
# An example package for AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2015 by Edwin A. Suominen,
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
Uses L{asynqueue.wire} to run and communicate with a server that
generates Mandelbrot Set images.

For the server end, use L{server} to get a Twisted C{service} object
you can start or add to an C{application}.

To communicate with the server, use L{} to get an AsynQueue
C{Worker} that you can add to your C{TaskQueue}.

Both ends of the connection need to be able to import this module and
reference its L{MandelbrotWorkerUniverse} class.
"""

import sys, urlparse

from twisted.internet import defer

from asynqueue.base import TaskQueue
from asynqueue.threads import Filerator
from asynqueue.wire import \
    WireWorkerUniverse, WireWorker, WireServer, ServerManager

import runner

# Server Connection defaults
PORT = 1978
INTERFACE = None
DESCRIPTION = None

FQN = "mcmandelbrot.wire.MandelbrotWorkerUniverse"


class MandelbrotWorkerUniverse(WireWorkerUniverse):
    """
    I run on the remote Python interpreter to run a runner from there,
    accepting commands from your L{RemoteRunner} via L{run},
    L{getRunInfo} and L{cancel} and sending the results and iterations
    to it.
    """
    @defer.inlineCallbacks
    def setup(self, N_values, steepness):
        if hasattr(self, 'runner'):
            dList = []
            for dRun, dCancel in self.pendingRuns.itervalues():
                dList.append(dRun)
                if not dCancel.called:
                    dCancel.callback(None)
            yield defer.DeferredList(dList)
            yield self.runner.q.shutdown()
        # These methods are called via a one-at-a-time queue, so it's
        # not a problem to overwrite the old runner with a new one,
        # after waiting for the old one's runs to get canceled and
        # then for it to shut down.
        self.pendingRuns = {}
        self.runner = runner.Runner(N_values, steepness)

    def run(self, Nx, cr, ci, crPM, ciPM):
        def done(stuff):
            fh.close()
            if ID in self.pendingRuns:
                del self.pendingRuns[ID]
            return stuff

        fh = Filerator()
        # Same as calculated by WireRunner to save iterator. This is important.
        ID = str(hash(fh))
        dCancel = defer.Deferred()
        dRun = self.runner.run(
            fh, Nx, cr, ci, crPM, ciPM, dCancel).addCallback(done)
        self.pendingRuns[ID] = dRun, dCancel
        return fh

    def getRunInfo(self, ID):
        if ID in self.pendingRuns:
            return self.pendingRuns[ID][0]
        
    def cancel(self, ID):
        if ID in self.pendingRuns:
            dCancel = self.pendingRuns[ID][1]
            if not dCancel.called:
                dCancel.callback(None)


class RemoteRunner(object):
    """
    Call L{setup} and wait for the C{Deferred} it returns, then you
    can call L{image} as much as you like to get images streamed to
    you as iterations of C{Deferred} chunks.

    Call L{shutdown} when done, unless you are using both a remote
    server and an external instance of C{TaskQueue}.
    """
    setupDefaults = {'N_values': 2000, 'steepness': 3}
    
    def __init__(self, description=None, q=None):
        self.description = description
        self.sv = self.setupDefaults.copy()
        if q is None:
            self.q = TaskQueue()
            self.stopper = self.q.shutdown
        else:
            self.q = q
    
    @defer.inlineCallbacks
    def setup(self, **kw):
        """
        Call at least once to set things up. Repeated calls with the same
        keywords, or with no keywords, do nothing. Keywords with a
        value of C{None} are ignored.

        @keyword N_values: The number of possible values for each iteration

        @keyword steepness: The steepness of the exponential applied to
          the value curve.
        
        @return: A C{Deferred} that fires when things are setup, or
          immediately if they already are as specified.
        """
        def checkSetup():
            result = False
            if 'FLAG' not in self.sv:
                self.sv['FLAG'] = True
                result = True
            for name, value in kw.iteritems():
                if value is None:
                    continue
                if self.sv.get(name, None) != value:
                    self.sv.update(kw)
                    return True
            return result
        
        if checkSetup():
            if self.description is None:
                # Local server running on a UNIX socket. Mostly useful
                # for testing.
                self.mgr = ServerManager(FQN)
                description = self.mgr.newSocket()
                yield self.mgr.spawn(description)
            wwu = MandelbrotWorkerUniverse()
            worker = WireWorker(wwu, description, series=['mcm'])
            yield self.q.attachWorker(worker)
            yield self.q.call(
                'setup',
                self.sv['N_values'],
                self.sv['steepness'], series='mcm')

    @defer.inlineCallbacks
    def shutdown(self):
        if hasattr(self, 'mgr'):
            yield self.mgr.done()
        if hasattr(self, 'stopper'):
            yield self.stopper()

    @defer.inlineCallbacks
    def run(self, fh, Nx, cr, ci, crPM, ciPM, dCancel=None):
        """
        Runs a C{compute} method on a remote Python interpreter to
        generate a PNG image of the Mandelbrot Set and write it in
        chunks, indirectly, to the write-capable object I{fh}, which
        in this case must implement C{IConsumer}. When this method is
        called by L{image.renderImage}, I{fh} will be a request and
        those do implement C{IConsumer}.

        The image is centered at location I{cr, ci} in the complex
        plane, plus or minus I{crPM} on the real axis and I{ciPM on
        the imaginary axis.

        @see: L{runner.run}.

        This method doesn't call L{setup}; that is taken care of by
        L{image.Imager} for HTTP requests and by L{writeImage} for
        local image file generation.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation and the number of pixels computed.
        """
        def canceler(null, ID):
            return self.q.call('cancel', ID, niceness=-15)
        
        # The heart of the matter
        producer = yield self.q.call(
            'run', Nx, cr, ci, crPM, ciPM, consumer=fh)
        # We have an instance of IterationProducer, but we need to
        # extract the ID it's using to get iterations. Why? Because
        # the canceler and run results are keyed to it.
        drCallTuple = producer.dr[0]
        prefetcherator = drCallTuple[0].im_self
        nextCallTuple = prefetcherator.nextCallTuple
        ID = nextCallTuple[1][0]
        if dCancel:
            dCancel.addCallback(canceler, ID)
        runInfo = yield self.q.call('getRunInfo', ID)
        defer.returnValue(runInfo)

    @defer.inlineCallbacks
    def writeImage(self, fileName, *args, **kw):
        """
        Call with the same arguments as L{run} after I{fh}, preceded by a
        writable I{fileName}. It will be opened for writing and its
        file handle supplied to L{run} as I{fh}.

        Writes the PNG image as it is generated remotely, returning a
        C{Deferred} that fires with the result of L{run} when the
        image is all written.

        @see: L{setup} and L{run}
        """
        yield self.setup(
            N_values=kw.pop('N_values', None),
            steepness=kw.pop('steepness', None))
        fh = open(fileName, 'w')
        runInfo = yield self.run(fh, *args)
        fh.close()
        defer.returnValue(runInfo)


def server(description=None, port=1978, interface=None):
    """
    Creates a Twisted C{endpoint} service for Mandelbrot Set images.

    The returned C{service} responds to connections as specified by
    I{description} and accepts 'image' commands via AMP to produce PNG
    images of the Mandelbrot set in a particular region of the complex
    plane.

    If you omit the I{description}, it will be a TCP server running on
    a particular I{port}. The default is C{1978}, which is the year in
    which the first computer image of the Mandelbrot Set was
    generated. You can specify an I{interface} dotted-quad address for
    the TCP server if you want to limit connections that way.

    @see: L{MandelbrotWorkerUniverse.image}
    """
    if description is None:
        description = b"tcp:{:d}".format(port)
        if interface:
            description += ":interface={}".format(interface)
    wwu = MandelbrotWorkerUniverse()
    ws = WireServer(wwu)
    return ws.run(description)


if '/twistd' in sys.argv[0]:
    application = service.Application("Mandelbrot Set PNG Image Server")
    return server(DESCRIPTION, PORT, INTERFACE).setServiceParent(application)
