#!/usr/bin/env python
#
# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the file COPYING for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

NAME = "AsynQueue"


### Imports and support
import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages


### Define setup options
kw = {'version':'0.5',
      'license':'GPL',
      'platforms':'OS Independent',

      'url':"http://edsuom.com/foss/%s/" % NAME,
      'author':'Edwin A. Suominen',
      'maintainer':'Edwin A. Suominen',
      
      'packages':find_packages(exclude=["*.test"]),
      'zip_safe':True
      }

kw['keywords'] = [
    'Twisted', 'asynchronous', 'threads',
    'taskqueue', 'queue', 'priority', 'tasks', 'jobs', 'nodes', 'cluster']


kw['classifiers'] = [
    'Development Status :: 4 - Beta',

    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',

    'License :: OSI Approved :: GNU General Public License (GPL)',
    'Operating System :: OS Independent',
    'Programming Language :: Python',

    'Topic :: System :: Distributed Computing',
    'Topic :: Software Development :: Object Brokering',
    'Topic :: Software Development :: Libraries :: Python Modules',
    ]


kw['description'] = " ".join("""
Asynchronous task queueing based on the Twisted framework.
""".split("\n"))

kw['long_description'] = " ".join("""
Asynchronous task queueing based on the Twisted framework, with task
prioritization and a powerful worker/manager interface. Worker implementations
are included for running tasks via threads, separate Python interpreters, and
remote worker nodes.
""".split("\n"))

### Finally, run the setup
setup(name=NAME, **kw)
