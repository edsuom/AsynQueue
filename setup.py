#!/usr/bin/env python
#
# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
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
kw = {'version':'0.9.1',
      'license':'Apache License (2.0)',
      'platforms':'OS Independent',

      'url':"http://edsuom.com/{}.html".format(NAME),
      'author':"Edwin A. Suominen",
      'author_email':"valueprivacy-foss@yahoo.com",
      'maintainer':'Edwin A. Suominen',

      'install_requires':required,
      'packages':['asynqueue', 'mcmandelbrot'],
      'package_data':{
          'mcmandelbrot': ['mcm.css', 'mcm.js', 'blank.jpg'],
          },
      'entry_points':{
          'console_scripts': [
              'mcmandelbrot = mcmandelbrot.main:run',
          ],
      },
      
      'zip_safe':True,
      }

kw['keywords'] = [
    'Twisted', 'asynchronous', 'threads',
    'taskqueue', 'queue', 'priority', 'tasks', 'jobs', 'nodes', 'cluster']


kw['classifiers'] = [
    'Development Status :: 5 - Production/Stable',

    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Framework :: Twisted',

    'Topic :: System :: Distributed Computing',
    'Topic :: Software Development :: Object Brokering',
    'Topic :: Software Development :: Libraries :: Python Modules',
    ]


kw['description'] = " ".join("""
Asynchronous task queueing based on the Twisted framework.
""".split("\n"))

kw['long_description'] = " ".join("""
Asynchronous task queueing based on the Twisted framework, with task
prioritization and a powerful worker interface. Worker implementations
are included for running tasks asynchronously in the main thread, in
separate threads, and in separate Python interpreters (multiprocessing).

Includes an example package "mcMandelbrot" that generates Mandelbrot
set images, row by row, demonstrating the power of asynchronous
multi-core processing. An instance of ProcessQueue dispatches the
computations for each row of pixels to workers running on separate
Python process. The color-mapped RGB results are collected as they
come back and intelligently buffered for iterating in a proper
sequence to a third-party PNG library that wouldn't ordinarily play
nice with Twisted.
""".split("\n"))

### Finally, run the setup
setup(name=NAME, **kw)
