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
An example of C{AsynQueue} in action. Can be fun to play with if you
have a multicore CPU. You will need the following packages, which you
can get via C{pip install}:

  - C{weave} (part of SciPy)
  - C{numpy} (part of SciPy)
  - C{matplotlib}
  - C{asynqueue} (Duh...)


Command line usage::

  mcmandelbrot
    [-s steepness] [-N values] [-o imageFile]
    N cr ci crPM [ciPM]

Produces chunks of a PNG image, to stdout if you don't specify an
imageFile with the C{-o} option.

Example: C{mcm 2000 -0.630 0 1.4 1.2 >overview.png}

"""

import sys

from twisted.internet import defer, reactor

import runner, wire


def run(*args, **kw):
    """
    Call with::

      [-d <description>,] [-N, <values>,]
      [-s, <steepness>,] [-o, <imageFile>,]
      Nx, cr, ci, crPM[, ciPM]

    Writes PNG image to stdout unless -o is set, then saves it to
    C{imageFile}. In that case, prints some stats about the
    multiprocessing computation to stdout.

    Options-as-arguments:
    
      - B{d}: An endpoint I{description} of a server running a
        L{wire.server}.

    @keyword N_values: Integer number of possible values for
      Mandelbrot points during iteration. Can set with the C{-N
      values} arg instead.
    
    @keyword ignoreReactor: Set C{True} to let somebody else start and
      stop the reactor.
    """
    @defer.inlineCallbacks
    def reallyRun():
        if description:
            myRunner = wire.RemoteRunner(description)
            yield myRunner.setup(
                N_values=N_values, steepness=steepness)
        else:
            myRunner = runner.Runner(N_values, steepness, stats)
        runInfo = yield myRunner.run(fh, Nx, cr, ci, crPM, ciPM)
        if stats:
            yield myRunner.showStats(runInfo)
        yield myRunner.shutdown()
        if not ignoreReactor:
            reactor.stop()
        defer.returnValue(runInfo)
        
    def getOpt(opt, default):
        optCode = "-{}".format(opt)
        if optCode in args:
            k = args.index(optCode)
            args.pop(k)
            optType = type(default)
            return optType(args.pop(k))
        return default
        
    ignoreReactor = kw.pop('ignoreReactor', False)
    if not args:
        args = sys.argv[1:]
    args = list(args)
    steepness = getOpt('s', 3.0)
    N_values = getOpt('N', 2000)
    fileName = getOpt('o', "")
    description = getOpt('d', "")
    if fileName:
        stats = True
        fh = open(fileName, 'w')
    else:
        stats = False
        fh = sys.stdout
    if len(args) < 4:
        print(
            "Usage: [-s steepness] [-N values] "+\
            "[-o imageFile] [-d description] " +\
            "N cr ci crPM [ciPM]")
        sys.exit(1)
    Nx = int(args[0])
    cr, ci, crPM = [float(x) for x in args[1:4]]
    ciPM = float(args[4]) if len(args) > 4 else crPM
    if ignoreReactor:
        return reallyRun()
    else:
        reactor.callWhenRunning(reallyRun)
        reactor.run()


if __name__ == '__main__':
    run()