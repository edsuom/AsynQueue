#!/usr/bin/env python
#
# AsynQueue:
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

NAME = "AsynQueue"


### Imports and support
from setuptools import setup

### Define requirements
required = ['Twisted']


### Define setup options
kw = {'version':'0.8.2',
      'license':'GPL',
      'platforms':'OS Independent',

      'url':"http://edsuom.com/{}.html".format(NAME),
      'author':"Edwin A. Suominen",
      'author_email':"valueprivacy-foss@yahoo.com",
      'maintainer':'Edwin A. Suominen',

      'install_requires':required,
      'packages':['asynqueue'],
      
      'zip_safe':True
      }

kw['keywords'] = [
    'Twisted', 'asynchronous', 'threads',
    'taskqueue', 'queue', 'priority', 'tasks', 'jobs', 'nodes', 'cluster']


kw['classifiers'] = [
    'Development Status :: 5 - Production/Stable',

    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    
    'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
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
""".split("\n"))

### Finally, run the setup
setup(name=NAME, **kw)
