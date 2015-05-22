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
An example of C{AsynQueue} in action. Can be fun to play with if
you have a multicore CPU. You will need the following packages, which
you can get via C{pip install}: C{weave} (part of SciPy); C{numpy}
(part of SciPy); C{matplotlib}; and of course C{asynqueue}.

Command line usage::

  mcmandelbrot
    [-d <description>,] [-N, <iterations>,]
    [-s, <steepness>,] [-o, <imageFile>,]
    Nx, cr, ci, crPM[, ciPM]

Writes PNG image to stdout unless B{-o} is set, then saves it to
I{imageFile}. In that case, prints some stats about the
multiprocessing computation to I{stdout}.

To see an overview displayed with ImageMagick's I{display} command::

  mcmandelbrot 2000 -0.630 0 1.4 1.2 |display

Write a detailed view to the image I{detail.png}::

  mcmandelbrot -o detail.png 3000 -0.73249 +0.21653 0.0112

Run a local AMP server in the background and then connect to it to
generate images (don't leave TCP servers running on Internet-exposed
machines unless you know what you're doing)::

  twistd -y AsynQueue-0.9.1/mcmandelbrot/wire.py
  export DESC="tcp:localhost:1978"
  mcmandelbrot -d $DESC -o detail-01.png 3000 -0.73249 +0.21653 0.01
  mcmandelbrot -d $DESC -o detail-02.png 3000 -0.73249 +0.21653 0.001

Run a local HTTP server, to which you can connect via port 8080 and
generate images interactively with your web browser::

  twistd -noy AsynQueue-0.9.1/mcmandelbrot/html.py

As of this package release date, I have one of these running on a
quad-core VPS at U{http://mcm.edsuom.com}.

@see: L{main}, L{html}, L{wire} to start. Interesting supporting
  modules are L{image} and L{runner}. The ground-level computation is
  done in L{valuer}.

"""


