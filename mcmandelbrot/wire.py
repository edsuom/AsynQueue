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

import urlparse

from twisted.internet import defer

from asynqueue.base import TaskQueue
from asynqueue.threads import Filerator
from asynqueue.wire import \
    WireWorkerUniverse, WireWorker, WireServer, ServerManager

import runner


FQN = "mcmandelbrot.wire.MandelbrotWorkerUniverse"


class MandelbrotWorkerUniverse(WireWorkerUniverse):
    """
    """
    @defer.inlineCallbacks
    def setup(self, N_values, steepness):
        if hasattr(self, 'runner'):
            yield self.runner.q.shutdown()
        # These methods are call via a one-at-a-time queue, so it's not
        # a problem to overwrite the old runner with a new one, after
        # waiting for the old one to shut down.
        self.runner = runner.Runner(N_values, steepness)

    @defer.inlineCallbacks
    def image(self, xySpans):
        fh = Filerator()
        yield self.runner.compute(fh, *xySpans)
        fh.close()
        yield fh.deferUntilDone()


class Client(object):
    """
    Call L{setup} and wait for the C{Deferred} it returns, then you
    can call L{image} as much as you like to get images streamed to
    you as iterations of C{Deferred} chunks.

    Call L{shutdown} when done, unless you are using both a remote
    server and an external instance of C{TaskQueue}.
    """
    Nx = 640
    Nx_max = 10000 # 100 megapixels ought to be enough

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
                # Local server running on a UNIX socket
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
        
    def setImageWidth(self, N):
        self.Nx = N

    def image(self, cr, ci, crPlusMinus, ciPlusMinus=None, consumer=None):
        """
        Gets a new PNG image of the Mandelbrot Set at location I{cr, ci}
        in the complex plane, +/- I{crPlusMinus, ciPlusMinus}. If
        I{ciPlusMinus} is not specified, it is that same as
        I{crPlusMinus}, resulting in a square image.

        The call creates a C{Deferator} that is converted to an
        C{IterationProducer} if you supply a I{consumer} it can
        produce to. Either way, the remote server iterates chunks of a
        PNG image as they are computed on the remote end.
        
        @return: A C{Deferred} that fires with a C{Deferator} if not
          consumer is supplied, or when the C{IterationProducer} is
          done producing to it if one was.
        """
        def diff(k):
            return xySpans[k][1] - xySpans[k][0]
        
        if ciPlusMinus is None:
            ciPlusMinus = crPlusMinus
        xySpans = []
        for center, plusMinus in (
                (cr, crPlusMinus, self.Nx),
                (ci, ciPlusMinus, Ny)):
            xySpans.append([center - plusMinus, center + plusMinus])
        xySpans[0].append(self.Nx)
        xySpans[1].append(int(self.Nx * diff(1) / diff(0)))
        # The heart of the matter
        return self.q.call('image', xySpans, consumer=consumer)

    @defer.inlineCallbacks
    def writeImage(self, fileName, *args, **kw):
        """
        Call with the same arguments as L{image} except with a I{fileName}
        first. Writes the PNG image as it is generated remotely,
        returning a C{Deferred} that fires when the image is all
        written.

        @see: L{setup} and L{image}
        """
        yield self.setup(
            N_values=kw.pop('N_values', None),
            steepness=kw.pop('steepness', None))
        fh = open(fileName, 'w')
        dr = yield self.image(*args, **kw)
        for d in dr:
            chunk = yield d
            fh.write(chunk)
        fh.close()

    @defer.inlineCallbacks
    def renderImage(self, request):
        """
        Call with a Twisted.web I{request} that includes a URL query map
        in C{request.args} specifying I{cr}, I{ci}, I{crpm}, and,
        optionally, I{crpi}. Writes the PNG image data to the request
        as it is generated remotely. When the image is all written,
        calls C{request.finish} and fires the C{Deferred} it returns.

        An example query string, for the basic Mandelbrot Set overview
        with 1200 points:
        
        C{?N=1200&Nv=1000&s=3&cr=-0.8&ci=0.0&crpm=1.45&crpi=1.2}

        @see: L{setup} and L{image}
        """
        x = {}
        kw = {}
        for name, value in request.args.iteritems():
            if name == 'N':
                N = int(value[0])
                if N > self.Nx_max:
                    N = self.Nx_max
                self.setImageWidth(value[0])
            elif name == 'Nv':
                kw['N_values'] = value
            elif name == 's':
                kw['steepness'] =value
            else:
                x[name] = float(value[0])
        if kw or 'FLAG' not in self.sv:
            yield self.setup(**kw)
        yield self.image(
            x['cr'], x['ci'], x['crpm'],
            ciPlusMinus=x.get('crpi', None), consumer=request)
        request.finish()
    

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
