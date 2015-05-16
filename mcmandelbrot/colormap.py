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
            [0.000, 1.9/3, 255],  # Red component ranges
            [1.7/3, 2.7/3, 255],  # Green component ranges
            [2.7/3, 1.000, 128],  # Blue component ranges
        ]
        return self._rangeMap(N, ranges)

    def _rangeMap(self, N, ranges):
        rgb = np.zeros((N, 3), dtype=np.uint8)
        kt = np.rint(N*np.array(ranges[:,:2])).astype(int)
        # Range #1: Increase red
        rgb[0:kt[0,1],0] = np.linspace(0, ranges[0][2], kt[0,1])
        # Range #2: Max red, increase green
        rgb[kt[0,1]:,0] = ranges[0][2]
        rgb[kt[1,0]:kt[1,1],1] = np.linspace(0, ranges[1][2], kt[1,1]-kt[1,0])
        # Range #3: Max red and green, increase blue
        rgb[kt[1,1]:,1] = ranges[1][2]
        rgb[kt[2,0]:,2] = np.linspace(0, ranges[1][2], kt[2,1]-kt[2,0])
        return rgb
        
    def csvFileMap(self):
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

    
