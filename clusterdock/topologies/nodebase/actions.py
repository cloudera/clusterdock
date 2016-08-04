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

import importlib
import logging
import os
from os.path import dirname, join

from clusterdock import Constants
from clusterdock.cluster import Cluster, Node, NodeGroup
from clusterdock.docker_utils import is_image_available_locally, pull_image

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_CLOUDERA_NAMESPACE = Constants.DEFAULT.cloudera_namespace # pylint: disable=no-member

def start(args):
    image = "{0}/{1}/clusterdock:{2}_nodebase".format(args.registry_url,
                                                      args.namespace or DEFAULT_CLOUDERA_NAMESPACE,
                                                      args.operating_system)
    if args.always_pull or not is_image_available_locally(image):
        pull_image(image)

    node_groups = [NodeGroup(name='nodes', nodes=[Node(hostname=hostname, network=args.network,
                                                       image=image, volumes=[])
                                                  for hostname in args.nodes])]
    cluster = Cluster(topology='nodebase', node_groups=node_groups, network_name=args.network)
    cluster.start()
