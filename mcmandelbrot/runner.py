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
Runner for Mandelbrot set computation processes.
"""

import sys, time, array

import png
import numpy as np

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer

import asynqueue
from asynqueue.threads import OrderedItemProducer
from asynqueue.process import ProcessQueue

from mcmandelbrot.valuer import MandelbrotValuer


class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    N_minProcesses = 2
    N_maxProcesses = 6
    
    msgProto = "{} ({:+16.13f} +/-{:10E}, {:+16.13f} +/-{:10E}) "+\
               "{:d} pixels in {:4.2f} sec"
    
    def __init__(self, N_values, stats=False, verbose=False):
        self.q = ProcessQueue(self.N_processes, callStats=stats)
        self.mv = MandelbrotValuer(N_values)
        self.verbose = verbose

    def shutdown(self):
        return self.q.shutdown()
        
    @property
    def N_processes(self):
        maxValue = min([
            self.N_maxProcesses, ProcessQueue.cores()])
        return max([self.N_minProcesses, maxValue])

    def log(self, *args):
        if self.verbose:
            print self.msgProto.format(*args)
    
    def run(self, fh, Nx, cr, ci, crPM, ciPM, dCancel=None, requester=None):
        """
        Runs my L{compute} method to generate a PNG image of the
        Mandelbrot Set and write it in chunks to the file handle or
        write-capable object I{fh}.

        The image is centered at location I{cr, ci} in the complex
        plane, plus or minus I{crPM} on the real axis and I{ciPM} on
        the imaginary axis.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation and the number of pixels computed.
        """
        def done(N):
            timeSpent = time.time() - t0
            if requester:
                self.log(
                    requester, cr, crPM, ci, ciPM, N, timeSpent)
            return timeSpent, N

        t0 = time.time()
        xySpans = []
        for center, plusMinus in ((cr, crPM), (ci, ciPM)):
            xySpans.append([center - plusMinus, center + plusMinus])
        xySpans[0].append(Nx)
        diffs = []
        for k in (0, 1):
            diff = xySpans[k][1] - xySpans[k][0]
            if diff <= 5E-16:
                return defer.succeed((0, 0))
            diffs.append(diff)
        xySpans[1].append(int(Nx * diffs[1] / diffs[0]))
        return self.compute(
            fh, xySpans[0], xySpans[1], dCancel).addCallback(done)
        
    @defer.inlineCallbacks
    def compute(self, fh, xSpan, ySpan, dCancel=None):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image, written as a PNG image to the supplied file
        handle I{fh} one row at a time.

        @return: A C{Deferred} that fires when the image is completely
          written and you can close the file handle, with the number
          of pixels computed (may be a lower number than expected if
          the connection terminated early).
        """
        def f(rows):
            try:
                writer = png.Writer(Nx, Ny, bitdepth=8, compression=9)
                writer.write(fh, rows)
            except Exception as e:
                # Trap ValueError caused by mid-stream cancellation
                if not isinstance(e, StopIteration):
                    if "rows" not in e.message and "height" not in e.message:
                        print "Error generating PNG: {}".format(e.message)
        
        crMin, crMax, Nx = xSpan
        ciMin, ciMax, Ny = ySpan
        # We have at most 5 calls in the process queue for each worker
        # servicing them, to allow midstream canceling and interleave
        # parallel computation requests.
        ds = defer.DeferredSemaphore(5*self.N_processes)
        p = OrderedItemProducer()
        yield p.start(f)
        # "The pickle module keeps track of the objects it has already
        # serialized, so that later references to the same object won't be
        # serialized again." --Python docs
        for k, ci in enumerate(np.linspace(ciMax, ciMin, Ny)):
            # "Wait" for the number of pending calls to fall back to
            # the limit
            yield ds.acquire()
            # Make sure the render hasn't been canceled
            if getattr(dCancel, 'called', False):
                break
            # Call one of my processes to get each row of values,
            # starting from the top
            d = p.produceItem(
                self.q.call, self.mv, crMin, crMax, Nx, ci,
                series='process')
            d.addCallback(lambda _: ds.release())
        yield p.stop()
        defer.returnValue(Nx*(k+1))

    def showStats(self, callInfo):
        """
        Displays stats about the run on stdout
        """
        def gotStats(stats):
            x = np.asarray(stats)
            workerTime, processTime = [np.sum(x[:,k]) for k in (0,1)]
            print "Run stats, with {:d} parallel ".format(self.N_processes) +\
                "processes running {:d} calls\n{}".format(len(stats), "-"*70)
            print "Process:\t{:7.2f} seconds, {:0.1f}% of main".format(
                processTime, 100*processTime/totalTime)
            print "Worker:\t\t{:7.2f} seconds, {:0.1f}% of main".format(
                workerTime, 100*workerTime/totalTime)
            print "Total on main:\t{:7.2f} seconds".format(totalTime)
            diffs = 1000*(x[:,0] - x[:,1])
            mean = np.mean(diffs)
            print "Mean worker-to-process overhead (ms/call): {:0.7f}".format(
                mean)
        totalTime = callInfo[0]
        print "Computed {:d} pixels in {:1.1f} seconds.".format(
            callInfo[1], totalTime)
        return self.q.stats().addCallback(gotStats)
