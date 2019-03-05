#!/usr/bin/env python
#
# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015, 2018-19 by Edwin A. Suominen,
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

NAME = "AsynQueue"


### Imports and support
from setuptools import setup

### Define requirements
required = ['Twisted']


### Define setup options
kw = {'version': "0.9.8",
      'license': "Apache License (2.0)",
      'platforms': "OS Independent",

      'url': "http://edsuom.com/{}.html".format(NAME),
      'project_urls': {
          'GitHub': "https://github.com/edsuom/{}".format(NAME),
          'API': "http://edsuom.com/{}/{}.html".format(
              NAME, NAME.lower()),
          },
      'author': "Edwin A. Suominen",
      'author_email': "foss@edsuom.com",
      'maintainer': "Edwin A. Suominen",
      'maintainer_email': "foss@edsuom.com",
      
      'install_requires': required,
      'packages': [
          'asynqueue', 'asynqueue.test',
          'mcmandelbrot', 'mcmandelbrot.test'
      ],
      'package_data': {
          'mcmandelbrot': [
              'server-install.sh', 'mcm.*', 'blank.jpg'
          ],
      },
      'entry_points': {
          'console_scripts': [
              'mcmandelbrot = mcmandelbrot.main:run',
          ],
      },
      'test_suite': "asynqueue.test",
      'long_description_content_type': "text/markdown",
}

kw['keywords'] = [
    'twisted', 'asynchronous', 'async', 'defer', 'deferred',
    'threads', 'parallel', 'distributed',
    'task', 'queue', 'priority', 'multicore', 'fractal',
]


kw['classifiers'] = [
    'Development Status :: 5 - Production/Stable',

    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 2 :: Only',
    'Framework :: Twisted',

    'Topic :: System :: Clustering',
    'Topic :: System :: Distributed Computing',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: System :: Hardware :: Symmetric Multi-processing',
]

# You get 77 characters. Use them wisely.
#----------------------------------------------------------------------------
#        10        20        30        40        50        60        70
#2345678901234567890123456789012345678901234567890123456789012345678901234567
kw['description'] =\
"Asynchronous task queueing with Twisted: threaded, multicore, and remote."

kw['long_description'] = """
Asynchronous task queueing based on the *Twisted* framework, with task
prioritization and a powerful worker interface. Worker implementations
are included for running tasks asynchronously in the main thread, in
separate threads, in separate Python interpreters (multiprocessing),
and even on separate devices using Twisted's Asynchronous Message
Protocol.

Includes deferred iteration capability: Calling a task that returns an
iterator can return a
[Deferator](http://edsuom.com/AsynQueue/asynqueue.iteration.Deferator.html)
instead, which does the iteration in a Twisted-friendly fashion, even
over a network connection. You can also supply an object conforming to
Twisted's *IConsumer* interface and iterations will be fed to it as they
become available.

The *util* module contains a
[DeferredTracker](http://edsuom.com/AsynQueue/asynqueue.util.DeferredTracker.html)
object that makes the import worthwhile all on its own. You can use
its **put** method to track Twisted *Deferred* objects without inserting
anything into their callback chains. Then you can wait in non-blocking
Twisted fashion for all, any, or some of the tracked deferreds to fire
(again, without getting tangled up with any of their callbacks) using
the tracker's **deferToAll**, **deferToAny**, and **deferUntilFewer**
methods.

Includes an example package
[mcMandelbrot](http://edsuom.com/mcMandelbrot.html) that generates
Mandelbrot set images, row by row, demonstrating the power of
asynchronous multi-core processing. An instance of
[ProcessQueue](http://edsuom.com/AsynQueue/asynqueue.process.ProcessQueue.html)
dispatches the computations for each row of pixels to workers running
on separate Python processes. The color-mapped RGB results are
collected as they come back and intelligently buffered for iterating
in a proper sequence to a third-party PNG library that wouldn't
ordinarily play nice with Twisted.

You can try things out after installation by running `mcmandelbrot`
(with a few options and arguments) from the console. The output of the
script is a PNG file, which you can view by piping to the free Feh
image viewer: Just add `|feh -` at the end of the command line.

There was some effort toward Python 3 compatiblity a while ago, but
it's still not yet supported.
"""

### Finally, run the setup
setup(name=NAME, **kw)
