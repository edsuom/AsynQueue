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

import sys

import weave
import numpy as np
import matplotlib.pyplot as plt

from twisted.internet import defer, reactor

import asynqueue


class MandelbrotValuer(object):
    """
    Returns the values (number of iterations to escape, if at all,
    inverted) of the Mandelbrot set at point cr + i*ci in the complex
    plane, for a range of real values with a constant imaginary component.

    C code adapted from Ilan Schnell's C{iterations} function at::
    
      https://svn.enthought.com/svn/enthought/Mayavi/
        branches/3.0.4/examples/mayavi/mandelbrot.py}

    The values are inverted, i.e., subtracted from the maximum value, so
    that no-escape points (technically, the only points actually in
    the Mandelbrot Set) have zero value and points that escape
    immediately have the maximum value. This allows simple mapping to
    the classic image with a black area in the middle.
    """
    N_values = 512
    
    code = """
    int j, k;
    double zr, zi, zr2, zi2;
    for (j=0; j<Nx[0]; j++) {
        k = 1;
        zr = X1(j);
        zi = ci;
        while ( k < kmax ) {
            zr2 = zr * zr;
            zi2 = zi * zi;
            if ( zr2+zi2 > 16.0 ) break;
            zi = 2.0 * zr * zi + ci;
            zr = zr2 - zi2 + X1(j);
            k++;
        }
        // Invert so that the fastest escape has the max value
        k = kmax - k;
        // Scale to range 0.0-1.0 and, since this value of c isn't needed
        // anymore, put the result in the array item
        X1(j) = (double)(k) / kmax;
    }
    """
    vars = ['x', 'ci', 'kmax']

    def __call__(self, crMin, crMax, N, ci):
        """
        Computes values for I{N} points along the real (horizontal) axis
        from I{crMin} to I{crMax}, with the constant imaginary
        component I{ci}.

        @return: A 1-D C{NumPy} array of length I{N} containing the
          values as normalized 16-bit floats in the range
          0.0-1.0. It's a small datatype but entirely adequate, even
          if antialiasing is done.
        
        """
        x = np.linspace(crMin, crMax, N, dtype=np.float64)
        kmax = self.N_values - 1
        weave.inline(self.code, self.vars)
        return x.astype(np.float16)


class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    N_processes = 4

    def __init__(self, Nx, xMin, xMax, yMin, yMax, useThread=False):
        self.xSpan = (xMin, xMax, Nx)
        Ny = int(Nx * (yMax - yMin) / (xMax - xMin))
        self.ySpan = (yMin, yMax, Ny)
        if useThread:
            self.q = asynqueue.TaskQueue()
            self.q.attachWorker(asynqueue.ThreadWorker())
        else:
            self.q = asynqueue.ProcessQueue(self.N_processes)

    def run(self, imgFilePath):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image.
        """
        reactor.callWhenRunning(self._reallyRun, imgFilePath)
        reactor.run()

    def _reallyRun(self, imgFilePath):
        def done(z):
            if z is not None:
                self.plot(z)
            reactor.stop()
        
        return self.compute().addCallback(done)
        
    @defer.inlineCallbacks
    def compute(self):
        """
        Computes the Mandelbrot Set for my complex region of interest and
        returns a C{Deferred} that fires with a 2D C{NumPy} array of
        normalized values.
        """
        def gotResult(row, k):
            if not self.isRunning:
                # Stopping, ignore the result
                return
            if isinstance(row, str):
                # Error, print it and stop when we can
                print str
                self.isRunning = False
                return self.q.shutdown()
            z[k,:] = row

        self.isRunning = True
        mv = MandelbrotValuer()
        crMin, crMax, Nx = self.xSpan
        dt = asynqueue.DeferredTracker()
        z = np.zeros((self.ySpan[2], Nx), dtype=np.float16)
        for k, ci in self.frange(*self.ySpan):
            if not self.isRunning:
                break
            # Call one of my processes to get a row of values
            d = self.q.call(mv, crMin, crMax, Nx, ci)
            d.addCallback(gotResult, k)
            dt.put(d)
        yield dt.deferToAll()
        if self.isRunning:
            defer.returnValue(z)

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

    def plot(self, z):
        """
        Color-maps values in the 2D array I{z} and renders a pseudocolor plot.
        """
        fig = plt.figure()
        extent = [
            self.xSpan[0], self.xSpan[1],
            self.ySpan[0], self.ySpan[1]]
        plt.imshow(z, origin='lower', extent=extent, aspect='equal')
        plt.colorbar()

        plt.show()


def run(*args):
    """
    Call with xMin, xMax, yMin, yMax, filePath
    """
    if not args:
        args = [int(sys.argv[1])]
        args = [float(x) for x in sys.argv[2:6]]
        args.append(sys.argv[6])
    Runner(*args[:5]).run(args[5])


if __name__ == '__main__':
    run(4000, -2.25, 0.75, -1.5, +1.5, 'mcm.png')
    #run()




    
