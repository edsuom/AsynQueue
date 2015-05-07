#!/usr/bin/env python
#
# mcmandelbrot
#
# An example module/program for AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""
C{mcmandelbrot.py [-s] Nx xMin xMax yMin yMax [filePath [N_values]]}

An example of C{AsynQueue} in action. Can be fun to play with if you
have a multicore CPU. You will need the following packages, which you
can get via C{pip install}:

  - C{weave} (part of SciPy)
  - C{numpy} (part of SciPy)
  - C{matplotlib}
  - C{asynqueue} (Duh...)

Here are some command-line args to try. Add a different filename to
each if you want to save a gallery of images to view and zoom in
on. Use PNG format to preserve the most detail.


4000 -2.15 +0.70 -1.20 +1.20
============================
Overview.


2000 -0.757 -0.743 +0.004 +0.025
================================

Tip of the upper "spear" separating the main part of the set from the
big secondary bulb at the 9:00 position.


3000 -1.43 -0.63 -0.365 +0.365
==============================

The big secondary bulb.


3000 -1.30 -1.08 +0.192 +0.368
==============================

The tertiary bulb at the 11:00 position on the big secondary bulb,
with filaments and baby Mandelbrot sets sprouted along their
lengths.


3000 -1.172 -1.152 +0.282 +0.302
================================

The first major intersection along the major filament.


3000 -1.165 -1.160 +0.291 +0.294
================================

Detail of the intersection and the filament branched off to the left
leading to a baby Mandelbrot set that points down and left.


3000 -1.16228 -1.16224 +0.292248 +0.292268
==========================================

An intersection midway along the left filament halfway to the
down-and-left Mandelbrot set


3000 -1.162261 -1.1622590 +0.2922574 +0.2922587
===============================================

Detail of the intersection showing an even smaller Mandelbrot set
there that points up and left.


3000 -1.1622600 -1.1622598 +0.2922580 +0.2922582
================================================

Further detail of the intersection showing the up-and-left Mandelbrot
set with interesting visual features caused by a limited escape cutoff
of 1000 in the value computation loop, L{MandelbrotValuer.__call__}


3000 -1.1622600 -1.1622598 +0.2922580 +0.2922582 <imgFile> 20000
================================================================

Sets I{N_values} to an insanely high cutoff, resulting in something
that looks a lot more boring for the exact same neighborhood in the
complex plane. The image is just another tilted version of the
overview. This goes on forever and ever, until limited by the
numerical precision of the computer.

Running this on my 8-core, 3.5 GHz AMD FX-8320 with C{N_processes=7}
took 18.2 seconds. Total time elapsed running L{MandelbrotValuer} on
the cores was 111.8 seconds, with just under 5 ms of overhead for each
of the 3000 calls. Most of that overhead was idle time spent polling
the interprocess pipe for results and then unpickling the 3000-element
row vectors that arrived over it from the processes.

Running it with C{useThread=True} to keep everything on a single CPU
core took 86.7 seconds, 4.8 times as long. There is some inefficiency
involved with L{process.ProcessQueue}, but it can make a huge
difference for parallel computing tasks with plenty of CPU cores
available.
"""

import sys, time

import weave
from weave.base_info import custom_info
import numpy as np
import matplotlib.cm as cm
import matplotlib.pyplot as plt

from twisted.internet import defer, reactor

import asynqueue


class my_info(custom_info):
    _extra_compile_args = ['-Wcpp']


class MandelbrotValuer(object):
    """
    Returns the values (number of iterations to escape, if at all,
    inverted) of the Mandelbrot set at point cr + i*ci in the complex
    plane, for a range of real values with a constant imaginary component.

    C code adapted from Ilan Schnell's C{iterations} function at::
    
      https://svn.enthought.com/svn/enthought/Mayavi/
        branches/3.0.4/examples/mayavi/mandelbrot.py}

    with periodicity testing and test-interval updating adapted from
    Simpsons's code contribution at::

      http://en.wikipedia.org/wiki/User:Simpsons_contributor/
        periodicity_checking

    and period-2 bulb testing from Wikibooks::

      http://en.wikibooks.org/wiki/Fractals/
        Iterations_in_the_complex_plane/Mandelbrot_set

    The values are inverted, i.e., subtracted from the maximum value,
    so that no-escape points (technically, the only points actually in
    the Mandelbrot Set) have zero value and points that escape
    immediately have the maximum value. This allows simple mapping to
    the classic image with a black area in the middle.
    """
    support_code = """
    bool region_test(double zr, double zr2, double zi2)
    {
        double q;
        // (x+1)^2 + y2 < 1/16
        if (zr2 + 2*zr + 1 + zi2 < 0.0625) return(true);
        // q = (x-1/4)^2 + y^2
        q = zr2 - 0.5*zr + 0.0625 + zi2;
        // q*(q+(x-1/4)) < 1/4*y^2
        q *= (q + zr - 0.25);
        if (q < 0.25*zi2) return(true);
        return(false);
    }

    int eval_point(int j, int N, double cr, double ci)
    {
        int k = 1;
        double zr = cr;
        double zi = ci;
        double zr2 = zr * zr, zi2 = zi * zi;
        // If we are in one of the two biggest "lakes," we need go no further
        if (region_test(zr, zr2, zi2)) return N;
        // Periodicity-testing variables
        double zrp = 0, zip = 0;
        int k_check = 0, N_check = 3, k_update = 0;
        while ( k < N ) {
            // Compute Z[n+1] = Z[n]^2 + C, with escape test
            if ( zr2+zi2 > 16.0 ) return k;
            zi = 2.0 * zr * zi + ci;
            zr = zr2 - zi2 + cr;
            k++;
            // Periodicity test: If same point is reached as previously,
            // there is no escape
            if ( zr == zrp )
                if ( zi == zip ) return N;
            // Check if previous-value update needed
            if ( k_check == N_check )
            {
                // Yes, do it
                zrp = zr;
                zip = zi;
                // Check again after another N_check iterations, an
                // interval that occasionally doubles
                k_check = 0;
                if ( k_update == 5 )
                {
                    k_update = 0;
                    N_check *= 2;
                }
                k_update++;
            }
            k_check++;
            // Compute squares for next iteration
            zr2 = zr * zr;
            zi2 = zi * zi;
        }
    }
    """
    code = """
    #define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
    int j, k;
    for (j=0; j<Nx[0]; j++) {
        // Evaluate the point
        Y1(j) = eval_point(j, kmax, X1(j), ci);
    }
    """
    vars = ['x', 'y', 'ci', 'kmax']

    def __init__(self, N_values):
        self.N_values = N_values
        self.infoObj = my_info()
        self.scale = 0.2 / N_values
    
    def __call__(self, crMin, crMax, N, ci):
        """
        Computes values for I{N} points along the real (horizontal) axis
        from I{crMin} to I{crMax}, with the constant imaginary
        component I{ci}.

        @return: A 1-D C{NumPy} array of length I{N} containing the
          escape values as 16-bit integers.
        """
        yy = np.zeros(N)
        quarterDiff = 0.25 * (crMax - crMin) / N
        for dx, dy in (
                ( 0.0,          0.0        ),
                (-quarterDiff, -quarterDiff),
                (+quarterDiff, -quarterDiff),
                (-quarterDiff, +quarterDiff),
                (+quarterDiff, +quarterDiff)):
            x = np.linspace(crMin+dx, crMax+dx, N, dtype=np.float64)
            y = self.computeValues(N, x, ci+dy)
            yy += y.astype(np.float)
        # Invert the iteration values so that trapped points have zero
        # value, then scale to 0.0 - 1.0 range
        z = self.scale * (5*self.N_values - yy)
        return z.astype(np.float16)

    def computeValues(self, N, x, ci):
        """
        Computes and returns a row vector of escape iterations, integer
        values.
        """
        kmax = self.N_values - 1
        y = np.zeros(N, dtype=np.int16)
        weave.inline(
            self.code, self.vars,
            customize=self.infoObj, support_code=self.support_code)
        return y
        

class ResultsManager(object):
    """
    I manage the array results from the computations, running
    everything in a thread via my own worker (and series) in the
    TaskQueue.
    """
    colorMap = cm.jet
    
    def __init__(self, q, power):
        self.q = q
        self.power = power
        self.q.attachWorker(asynqueue.ThreadWorker())

    def __len__(self):
        return self.z.size
        
    def init(self, Nx, Ny, N_values):
        def f_i():
            self.z = np.zeros((Ny, Nx))
        return self.q.call(f_i, series='thread')

    def handleRow(self, row, k):
        """
        Handles a I{row} (1-D array) from one of the processes at row
        index I{k}.
        """
        def f_hr():
            self.z[k,:] = np.power(row, self.power)
        return self.q.call(f_hr, series='thread')

    def plot(self, extent, imgFilePath=None):
        """
        Color-maps values in the 2D array I{z} and renders a pseudocolor plot.
        """
        def f_p():
            if imgFilePath:
                plt.imsave(
                    imgFilePath, self.z, origin='lower', cmap=self.colorMap)
                return
            return self.colorMap
        return self.q.call(f_p, series='thread')
        

class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    power = 5.0
    N_values = 1000
    N_processes = 7

    def __init__(self, Nx, xMin, xMax, yMin, yMax, stats=False):
        self.xSpan = (xMin, xMax, Nx)
        Ny = int(Nx * (yMax - yMin) / (xMax - xMin))
        self.ySpan = (yMin, yMax, Ny)
        self.stats = stats
        self.dt = asynqueue.DeferredTracker()
        self.q = asynqueue.ProcessQueue(self.N_processes, callStats=stats)
        self.rm = ResultsManager(self.q, self.power)

    @defer.inlineCallbacks
    def run(self, *args):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image.

        @param imgFilePath: The filename for saving the image instead
          of displaying it. PNG format suggested.
        @type imgFilePath: First argument, if any, a string.

        @param N_values: The number of possible discrete values for
          the result, i.e., the number of times to try iterations of
          M{z = z^2 + c} to see if escape is reached and determine
          that C{c} lies outside the Mandelbrot set.
        @type N_values: Second argument, if any, an integer. Default
          is 1000.
        
        """
        imgFilePath = args[0] if args else None
        N_values = int(args[1]) if len(args) > 1 else self.N_values
        self.dt.put(self.rm.init(self.xSpan[2], self.ySpan[2], N_values))
        t0 = time.time()
        N = yield self.compute(N_values)
        totalTime = time.time() - t0
        print "Computed {:d} values in {:1.1f} seconds.".format(
            N, totalTime)
        if self.stats:
            stats = yield self.q.stats()
            self.showStats(totalTime, stats)
        extent = [
            self.xSpan[0], self.xSpan[1],
            self.ySpan[0], self.ySpan[1]]
        colorMap = yield self.rm.plot(extent, imgFilePath)
        if colorMap:
            fig = plt.figure()
            plt.imshow(
                self.rm.z,
                origin='lower', extent=extent, aspect='equal', cmap=colorMap)
            plt.colorbar()
            plt.show()
        
    @defer.inlineCallbacks
    def compute(self, N_values):
        """
        Computes the Mandelbrot Set for my complex region of interest and
        returns a C{Deferred} that fires with a 2D C{NumPy} array of
        normalized values.
        """
        yield self.dt.deferToAll()
        crMin, crMax, Nx = self.xSpan
        mv = MandelbrotValuer(N_values)
        for k, ci in self.frange(*self.ySpan):
            # Call one of my processes to get a row of values
            d = self.q.call(mv, crMin, crMax, Nx, ci, series='process')
            d.addCallback(self.rm.handleRow, k)
            self.dt.put(d)
        yield self.dt.deferToAll()
        defer.returnValue(len(self.rm))

    def frange(self, minVal, maxVal, N):
        """
        Iterates over a range of I{N} evenly spaced floats from I{minVal}
        to I{maxVal}, yielding the iteration index and the value.
        """
        val = float(minVal)
        step = (float(maxVal) - val) / N
        for k in xrange(N):
            yield k, val
            val += step

    def showStats(self, totalTime, stats):
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


def run(*args, **kw):
    """
    Call with [-s,] Nx, xMin, xMax, yMin, yMax[, filePath[, N_values]]

    @keyword callStats: Set C{True} to print stats about calls.

    @keyword stopWhenDone: Set C{True} to stop the reactor when done.
    """
    def reallyRun():
        runner = Runner(*newArgs, **kw)
        d = runner.run(*args[5:])
        if stopWhenDone:
            d.addCallback(lambda _: reactor.stop())
        return d
    
    if not args:
        args = sys.argv[1:]
    if '-s' in args:
        kw['stats'] = True
        args.remove('-s')
    if len(args) < 5:
        print "Arguments: N rMin rMax iMin iMax [imageFile [N_values]]"
        sys.exit(1)
    newArgs = [int(args[0])]
    newArgs.extend([float(x) for x in args[1:5]])
    stopWhenDone = kw.pop('stopWhenDone', False)
    reactor.callWhenRunning(reallyRun)
    reactor.run()


if __name__ == '__main__':
    run(stopWhenDone=True)
