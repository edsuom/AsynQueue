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
Colormapping with Kenneth Moreland's "Diverging Color Maps for
Scientific Visualization",
U{http://www.sandia.gov/~kmorel/documents/ColorMaps/}.
"""

import os.path
from pkg_resources import resource_stream
from array import array

import numpy as np
 

class ColorMapper(object):
    """
    I map floating-point values in the range 0.0 to 1.0 to RGB byte
    triplets.

    @cvar fileName: A file with a colormap of RGB triplets, one for
      each of many linearly increasing values to be mapped, in CSV
      format.
    """
    N_blackRed = 2000
    useBlackRed = True
    fileName = "moreland.csv"

    def __init__(self, useBlackRed=False):
        if not useBlackRed:
            useBlackRed = self.useBlackRed
        if useBlackRed:
            self.rgb = self.blackRedMap(self.N_blackRed)
        else:
            self.rgb = self.csvFileMap()
        self.jMax = len(self.rgb) - 1

    def blackRedMap(self, N):
        """
        Returns an RGB colormap of dimensions C{Nx3} that transitions from
        black to red, then red to orange, then orange to white.
        """
        ranges = [
            [0.000, 1.0/3],  # Red component ranges
            [0.8/3, 2.0/3],  # Green component ranges
            [1.8/3, 1.000],  # Blue component ranges
        ]
        return self._rangeMap(N, ranges)

    def _rangeMap(self, N, ranges):
        rgb = np.zeros((N, 3), dtype=np.uint8)
        kt = np.rint(N*np.array(ranges)).astype(int)
        # Range #1: Increase red
        rgb[0:kt[0,1],0] = np.linspace(0, 255, kt[0,1])
        # Range #2: Max red, increase green
        rgb[kt[0,1]:,0] = 255
        rgb[kt[1,0]:kt[1,1],1] = np.linspace(0, 255, kt[1,1]-kt[1,0])
        # Range #3: Max red and green, increase blue
        rgb[kt[1,1]:,1] = 255
        rgb[kt[2,0]:,2] = np.linspace(0, 255, kt[2,1]-kt[2,0])
        return rgb
        
    def csvFile(self):
        """
        Returns an RGB colormap loaded from I{fileName} in my package
        directory.
        """
        filePath = os.path.join(
            os.path.dirname(__file__), self.fileName)
        if os.path.exists(filePath):
            fh = open(filePath)
        else:
            fh = resource_stream(__name__, self.fileName)
        rgb = np.loadtxt(fh, delimiter=',', dtype=np.uint8)
        fh.close()
        return rgb
    
    def __call__(self, x):
        result = array('B')
        np.rint(self.jMax * x, x)
        for j in x.astype(np.uint16):
            result.extend(self.rgb[j,:])
        return result

    
