# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The clusterdock package houses the core clusterdock code that handles Docker container cluster
orchestration, as well as topologies, the abstraction the defines the behavior of these clusters.
It also contains a number of utility modules to wrap common Docker API functionality."""

import logging
from ConfigParser import SafeConfigParser
from os.path import dirname, join

logging.basicConfig(level=logging.ERROR)

class Constants(object):
    """A class just designed to make the contents of constants.cfg available to clusterdock modules
    in a pretty way. Yes, this could have been done as a nested dictionary, but accessing
    Constants.docker_images.maven looks less gross to me than constants['docker_images']['maven'].

    Note that, to keep Pylint happy, we'll have to add
        # pylint: disable=no-member
    at the end of every line in which we reference this class.
    """

    # pylint: disable=too-few-public-methods

    _config = SafeConfigParser()
    _config.read(join(dirname(__file__), 'constants.cfg'))
    for section in _config.sections() + ['DEFAULT']:
        locals()[section] = type(section, (), {item[0]: item[1] for item in _config.items(section)})
