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
Render Mandelbrot Set images in PNG format to Twisted web requests
"""

import urlparse

from twisted.internet import defer

import runner


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

    msgProto = "{} :: ({:+f} +/- {:f}, {:f} +/- {:f}) in {:4.2f} sec."
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.runner = runner.Runner(self.N_values, self.steepness)

    def shutdown(self):
        return self.runner.shutdown()

    def log(self, ip, cr, ci, crpm, ciPM, timeSpent):
        if self.verbose:
            print self.msgProto.format(ip, cr, ci, crpm, ciPM, timeSpent)
        
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

        An example query string, for the basic Mandelbrot Set overview
        with 1200 points:
        
        C{?N=1200&cr=-0.8&ci=0.0&crpm=1.45&crpi=1.2}
        """
        x = {}
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
            timeSpent = yield self.runner.run(
                request, self.Nx,
                x['cr'], x['ci'], x['crpm'], ciPM)
            self.log(
                request.getClient(),
                x['cr'], x['ci'], x['crpm'], ciPM, timeSpent)
        request.finish()
