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
Point valuer for L{mcmandelbrot}. Each CPU core has its own copy
of L{MandelbrotValuer} that is called via the
C{AsynQueue.ProcessQueue}.
"""

import sys, time, array

import png
import weave
from weave.base_info import custom_info
import numpy as np

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer

import asynqueue
from asynqueue.threads import Consumerator


from colormap import ColorMapper


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
    the classic image with a black area in the middle. Then they are
    scaled to the 0.0-1.0 range, and an exponent is applied to
    emphasize changes at shorter escape times. Finally, they are
    mapped to RGB triples and returned.

    @ivar cm: A callable object that converts C{NumPy} array inputs in
      the 0.0-1.0 range to an unsigned-int8 Python array of RGB
      triples.
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

    def __init__(self, N_values, steepness):
        """
        Constructor:

        @param N_values: The number of iterations to try, hence the
          range of integer values, for a single call to
          L{computeValues}. Because a 5-point star around each point
          is evaluated with the values summed, the actual range of
          values for each point is 5 times greater.

        @param steepness: The amount to rescale the values after
          scaling to the -0.5 to 0.5 range and before applying the
          logistic function and color mapping.
        """
        self.N_values = N_values
        
        self.steepness = steepness
        self.cm = ColorMapper()
        # The maximum possible escape value is mapped to 1.0, before
        # exponent and then color mapping are applied
        self.scale = 0.2 / N_values
        self.infoObj = my_info()
    
    def __call__(self, crMin, crMax, N, ci):
        """
        Computes values for I{N} points along the real (horizontal) axis
        from I{crMin} to I{crMax}, with the constant imaginary
        component I{ci}.

        @return: A Python B-array I{3*N} containing RGB triples for an
          image representing the escape values.
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
        # value, then scale to the range [-1.0, +1.0]
        z = 2*self.scale * (5*self.N_values - yy) - 1.0
        # Transform to emphasize details in the middle
        z = self.transform(z, self.steepness)
        # [-1.0, +1.0] --> [0.0, 1.0]
        z = 0.5*(z + 1.0)
        # Map to my RGB colormap
        return self.cm(z)

    def transform(self, x, k):
        """
        """
        return np.power(x, k)
    
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
