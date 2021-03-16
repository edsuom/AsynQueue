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

"""
I provide a ready-made convenience object for all your Python
version agnosticism needs.
"""

import six


class VA(object):
    """
    I provide version-agnostic attributes and a I{py3} attribute that
    indicates if we're running under Python 3.
    """
    def __init__(self):
        import sys
        self.py3 = (sys.version_info.major >= 3)
        if not self.py3:
            # Python 2.x
            self.buffer = buffer
            self.long = long
            self.unicode = unicode
            import StringIO as StringIOModule
            import cPickle as p
        else:
            # Python 3.x
            self.buffer = memoryview
            self.long = int
            self.unicode = str
            import io as StringIOModule
            import pickle as p
        self.pickle = p
        self.StringIO = StringIOModule.StringIO

    def iteritems(self, dictionary):
        """
        Call this with your dict instead of calling C{iteritems} on the
        dict.
        """
        if self.py3:
            return iter(dictionary.items())
        return dictionary.iteritems()

    def keys(self, dictionary):
        """
        Call this with your dict to get a static list of keys instead of
        calling C{keys} on the dict.
        """
        result = dictionary.keys()
        if self.py3:
            result = list(result)
        return result
    

# Ready-made!
va = VA()
