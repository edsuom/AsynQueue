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
Render Mandelbrot Set images in PNG format in response to Twisted
web requests. Used by L{html}.
"""

import urlparse

from twisted.internet import defer

from mcmandelbrot import wire


class RunnerToken(object):
    """
    B{TODO:} Implmement this with C{asynqueue.base.Priority} to
    dispatch requests to runners based on how fast they've run
    previous ones.
    """
    scores = {}
    def __init__(self, runnerInstance):
        self.r = runnerInstance


class Imager(object):
    """
    Call L{renderImage} with Twisted web I{request} objects as much as
    you like to write PNG images in response to them.

    Call L{shutdown} when done.
    """
    Nx = 640
    Nx_min = 240
    Nx_max = 10000 # 100 megapixels ought to be enough

    N_values = 3000
    steepness = 3

    def __init__(self, descriptions=[], verbose=False):
        self.dStart = self.setup(descriptions, verbose)

    def setup(self, descriptions, verbose=False):
        """
        Sets me up with one or more instances of L{wire.RemoteRunner}.

        B{TODO:} Implement the use of multiple
        descriptions. Currently, the list must have one and only one.

        @param descriptions: A list of one or more Twisted C{endpoint}
          descriptions for connecting a L{wire.RemoteRunner}. Include
          a C{None} object in the list to use (or also use) one that
          runs via a UNIX socket on the local machine.
        """
        dList = []
        if len(descriptions) != 1:
            raise NotImplementedError("You can use only one runner, for now")
        for description in descriptions:
            self.runner = wire.RemoteRunner(description)
            d = self.runner.setup(
                N_values=self.N_values, steepness=self.steepness)
            dList.append(d)
        return defer.DeferredList(dList)

    def shutdown(self):
        return self.runner.shutdown()
        
    def setImageWidth(self, N):
        if N < self.Nx_min:
            N = self.Nx_min
        elif N > self.Nx_max:
            N = self.Nx_max
        self.Nx = N

    @defer.inlineCallbacks
    def renderImage(self, request):
        """
        Call with a Twisted.web I{request} that includes a URL query map
        in C{request.args} specifying I{cr}, I{ci}, I{crpm}, and,
        optionally, I{crpi}. Writes the PNG image data to the request
        as it is generated remotely. When the image is all written,
        calls C{request.finish} and fires the C{Deferred} it returns.

        An example query string, for the basic Mandelbrot set overview
        with 1200 points::
        
          ?N=1200&cr=-0.8&ci=0.0&crpm=1.45&crpi=1.2
        
        """
        x = {}
        d = request.notifyFinish().addErrback(lambda _: None)
        neededNames = ['cr', 'ci', 'crpm']
        for name, value in request.args.iteritems():
            if name == 'N':
                self.setImageWidth(int(value[0]))
            else:
                x[name] = float(value[0])
            if name in neededNames:
                neededNames.remove(name)
        if not neededNames:
            ciPM = x.get('cipm', x['crpm'])
            if hasattr(self, 'dStart'):
                yield self.dStart
                del self.dStart
            timeSpent, N = yield self.runner.run(
                request, self.Nx,
                x['cr'], x['ci'], x['crpm'], ciPM,
                dCancel=d, requester=request.getClient())
            if not d.called:
                request.finish()
