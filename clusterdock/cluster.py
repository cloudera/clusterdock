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

"""This module implements the main abstractions used to model distributed clusters on a single
host through the use of networked Docker containers. In particular, classes for clusters (Cluster),
nodes (Node), and groups of nodes that may be convenient referenced together (NodeGroup) are
implemented here.
"""

import logging
import re
import threading
from os.path import dirname, join
from time import time, sleep

from docker import Client
from docker.errors import APIError
from docker.utils import create_ipam_pool

from clusterdock.docker_utils import (get_container_ip_address,
                                      get_network_container_hostnames, get_network_subnet,
                                      get_available_network_subnet, is_container_reachable,
                                      is_network_present, NetworkNotFoundException)
from clusterdock.ssh import ssh

# We disable a couple of Pylint conventions because it assumes that module level variables must be
# named as if they're constants (which isn't the case here).
logger = logging.getLogger(__name__) # pylint: disable=invalid-name
logger.setLevel(logging.INFO)

client = Client() # pylint: disable=invalid-name

class Cluster(object):
    """The central abstraction for dealing with Docker container clusters. Instances of this class
    can be created as needed, but no Docker-specific behavior is done until start() is invoked.
    """
    def __init__(self, topology, node_groups, network_name):
        """Creates a cluster instance from a given topology name, list of NodeGroups, and network
        name."""
        self.topology = topology
        self.ssh_key = join(dirname(__file__), 'topologies', self.topology, 'ssh', 'id_rsa')

        self.node_groups = node_groups
        self.network_name = network_name

        self.nodes = [node for node_group in self.node_groups for node in node_group.nodes]

    def setup_network(self):
        """If the network doesn't already exist, create it, being careful to pick a subnet that
        doesn't collide with that of any other Docker networks already present."""
        if not is_network_present(self.network_name):
            logger.info("Network (%s) not present, creating it...", self.network_name)
            next_network_subnet = get_available_network_subnet()
            while True:
                try:
                    client.create_network(name=self.network_name, driver='bridge', ipam={
                        'Config': [create_ipam_pool(subnet=next_network_subnet)]
                    })
                except APIError as api_error:
                    if 'networks have overlapping IPv4' not in api_error.explanation:
                        raise api_error
                    else:
                        # The hash after "conflicts with network" is the name with the overlapping
                        # subnet. Save this to speed up finding the next available subnet the next
                        # time around in the while loop.
                        conflicting_network = re.findall(r'conflicts with network (\S+)',
                                                         api_error.explanation)[0]
                        logger.info("Conflicting network:(%s)", conflicting_network)
                        # Try up get the next network subnet up to 5 times (looks like there's a
                        # race where the conflicting network is known, but not yet visible through
                        # the API).
                        for _ in range(0, 5):
                            try:
                                next_network_subnet = get_available_network_subnet(
                                    get_network_subnet(conflicting_network)
                                )
                            except NetworkNotFoundException as network_not_found_exception:
                                if 'Cannot find network' not in network_not_found_exception.message:
                                    raise network_not_found_exception
                                sleep(1)
                            else:
                                break
                else:
                    logger.info("Successfully setup network (name: %s).", self.network_name)
                    break

    def ssh(self, command, nodes=None):
        """Execute command on all nodes (unless a list of Node instances is passed) in parallel."""
        ssh(command=command,
            hosts=[node.ip_address for node in self.nodes if not nodes or node in nodes],
            ssh_key=self.ssh_key)

    def start(self):
        """Actually start Docker containers, mimicking the cluster layout specified in the Cluster
        instance."""
        start = time()
        self.setup_network()

        # Before starting any containers, make sure that there aren't any containers in the
        # network with the same hostname.
        network_container_hostnames = (
            get_network_container_hostnames(self.network_name))
        for node in self.nodes:
            # Set the Node instance's cluster attribute to the Cluster instance to give the node
            # access to the topology's SSH keys.
            node.cluster = self

            if node.hostname in network_container_hostnames:
                raise Exception(
                    "A container with hostname {0} already exists in network {1}".format(
                        node.hostname, self.network_name))
        threads = [threading.Thread(target=node.start) for
                   node in self.nodes]
        for thread in threads:
            thread.start()
            # Sleep shortly between node starts to bring some determinacy to the order of the IP
            # addresses that we get.
            sleep(0.25)
        for thread in threads:
            thread.join()
        etc_hosts_string = ''.join("{0}   {1}.{2} # Added by clusterdock\n".format(node.ip_address,
                                                                                   node.hostname,
                                                                                   node.network) for
                                   node in self.nodes)
        with open('/etc/hosts', 'a') as etc_hosts:
            etc_hosts.write(etc_hosts_string)

        end = time()
        logger.info("Started cluster in %.2f seconds.", end - start)

    def __iter__(self):
        for node in self.nodes:
            yield node

    def __len__(self):
        return len(self.nodes)


class NodeGroup(object):
    """A node group denotes a set of Nodes that share some characteristic so as to make it desirable
    to refer to them separately from other sets of Nodes. For example, in a typical HDFS cluster,
    one node would run the HDFS NameNode while the remaining nodes would run HDFS DataNodes. In
    this case, the former might comprise the "primary" node group while the latter may be part of
    the "secondary" node group.
    """

    def __init__(self, name, nodes=None):
        """Initialize a Group instance called name with a list of nodes."""
        self.name = name
        self.nodes = nodes

    def __iter__(self):
        for node in self.nodes:
            yield node

    def add_node(self, node):
        """Add a Node instance to the list of nodes in the NodeGroup."""
        self.nodes.append(node)

    def ssh(self, command):
        """Run command over SSH across all nodes in the NodeGroup in parallel."""
        ssh_key = self[0].cluster.ssh_key
        ssh(command=command, hosts=[node.ip_address for node in self.nodes], ssh_key=ssh_key)

class Node(object):
    """The abstraction will eventually be actualized as a running Docker container. This container,
    unlike the typical Docker container, does not house a single process, but tends to run an
    init to make the container act more or less like a regular cluster node.
    """

    # pylint: disable=too-many-instance-attributes
    # 11 instance attributes to keep track of node properties isn't too many (Pylint sets the limit
    # at 7), and while we could create a single dictionary attribute, that doesn't really improve
    # readability.

    def __init__(self, hostname, network, image, **kwargs):
        """volumes must be a list of dictionaries with keys being the directory on the host and the
        values being the corresponding directory in the container to mount."""
        self.hostname = hostname
        self.network = network
        self.fqdn = "{0}.{1}".format(hostname, network)
        self.image = image

        # Optional arguments are relegated to the kwargs dictionary, in part to keep Pylint happy.
        self.command = kwargs.get('command')
        self.ports = kwargs.get('ports')
        # /etc/localtime is always volume mounted so that containers have the same timezone as their
        # host machines.
        self.volumes = [{'/etc/localtime': '/etc/localtime'}] + kwargs.get('volumes', [])

        # Define a number of instance attributes that will get assigned proper values when the node
        # starts.
        self.cluster = None
        self.container_id = None
        self.host_config = None
        self.ip_address = None

    def _get_binds(self):
        """docker-py takes binds in the form "/host/dir:/container/dir:rw" as host configs. This
        method returns a list of binds in that form."""
        return ["{0}:{1}:rw".format(host_location, volume[host_location]) for volume in self.volumes
                for host_location in volume]

    def start(self):
        """Actually start a Docker container-based node on the host."""

        # Create a host_configs dictionary to populate and then pass to Client.create_host_config().
        host_configs = {}
        # To make them act like real hosts, Nodes must have all Linux capabilities enabled. For
        # some reason, we discovered that doing this causes less trouble than starting containers in
        # privileged mode (see KITCHEN-10073). We also disable the default seccomp profile (see #3)
        # and pass in the volumes list at this point.
        host_configs['cap_add'] = ['ALL']
        host_configs['security_opt'] = ['seccomp:unconfined']
        host_configs['publish_all_ports'] = True

        if self.volumes:
            host_configs['binds'] = self._get_binds()

        self.host_config = client.create_host_config(**host_configs)

        # docker-py runs containers in a two-step process: first it creates a container and then
        # it starts the container using the container ID.
        container_configs = {
            'hostname': self.fqdn,
            'image': self.image,
            'host_config': self.host_config,
            'detach': True,
            'command': self.command,
            'ports': self.ports,
            'volumes': [volume[host_location] for volume in self.volumes
                        for host_location in volume if self.volumes],
            'labels': {"volume{0}".format(i): volume
                       for i, volume in enumerate([volume.keys()[0]
                                                   for volume in self.volumes
                                                   if volume.keys()[0] not in ['/etc/localtime']],
                                                  start=1)
                      }
        }
        self.container_id = client.create_container(**container_configs)['Id']

        # Don't start up containers on the default 'bridge' network for better isolation.
        client.disconnect_container_from_network(container=self.container_id, net_id='bridge')
        client.connect_container_to_network(container=self.container_id, net_id=self.network,
                                            aliases=[self.hostname])
        client.start(container=self.container_id)

        self.ip_address = get_container_ip_address(container_id=self.container_id,
                                                   network=self.network)
        if not is_container_reachable(container_id=self.container_id, network=self.network,
                                      ssh_key=self.cluster.ssh_key):
            raise Exception("Timed out waiting for {0} to become reachable.".format(self.hostname))
        else:
            logger.info("Successfully started %s (IP address: %s).", self.fqdn, self.ip_address)

    def ssh(self, command):
        """Run command over SSH on the node."""
        ssh(command=command, hosts=[self.ip_address], ssh_key=self.cluster.ssh_key)
