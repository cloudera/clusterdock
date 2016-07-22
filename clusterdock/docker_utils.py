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

"""A hodgepodge collection of utility functions that interact with or use Docker."""

import logging
from os.path import dirname, join
from sys import stdout
from time import time

from docker import Client
from docker.errors import NotFound
from fabric.api import local, quiet
from netaddr import IPNetwork

from clusterdock import Constants
from clusterdock.ssh import quiet_ssh
from clusterdock.utils import get_nested_value

logger = logging.getLogger(__name__) # pylint: disable=invalid-name
logger.setLevel(logging.INFO)

# Change timeout for Docker client commands from default (60 s) to 30 min. This prevents timeouts
# seen when removing really large containers.
DOCKER_CLIENT_TIMEOUT = 1800
client = Client(timeout=DOCKER_CLIENT_TIMEOUT) # pylint: disable=invalid-name

class NetworkNotFoundException(Exception):
    """An exception to raise when a particular Docker network cannot be found."""
    pass

class ContainerNotFoundException(Exception):
    """An exception to raise when a particular Docker container cannot be found."""
    pass

class ContainerExitCodeException(Exception):
    """An exception to raise when a container exits with a non-zero exit code."""
    pass

def is_container_reachable(container_id, network=None):
    """Return true if a container can be reached via SSH (timeout after 60 s), false otherwise."""
    try:
        start = time()
        quiet_ssh("whoami", get_container_ip_address(container_id, network))
        end = time()
        logger.debug("Verified SSH connectivity in %s seconds.", end-start)
        return True
    except BaseException:
        return False

def is_container_running(name):
    """Return true if a Docker container is running, false otherwise."""
    try:
        running = get_container_attribute(name, "State.Running")
        return running
    # If docker.errors.NotFound is raised, container is not running.
    except NotFound:
        return False

def is_image_available_locally(name):
    """Return true if the Docker image 'name' is present on the Docker host, false otherwise."""

    # Docker's API hides the registry URL if it's docker.io (e.g. docker.io/example/image:latest
    # becomes example/image:latest).
    if name.startswith('docker.io/'):
        name = name[len('docker.io/'):]
    return any(tag == name for image in client.images() for tag in image['RepoTags'])

def pull_image_if_missing(name):
    """Simple wrapper function that will pull the Docker image 'name' if it's not present on the
    Docker host."""
    if not is_image_available_locally(name=name):
        pull_image(name=name)

def build_image(dockerfile, tag):
    """Python wrapper for the docker build command line argument. Could also be implemented using
    docker-py, but then we lose the progress indicators that the Docker command line gives us."""
    local("docker build -t {0} --no-cache {1}".format(tag, dirname(dockerfile)))

def pull_image(name):
    """Python wrapper for the docker pull command line argument. Could also be implemented using
    docker-py, but then we lose the progress indicators that the Docker command line gives us."""
    local("docker pull {0}".format(name))

def push_image(name):
    """Python wrapper for the docker push command line argument. Could also be implemented using
    docker-py, but then we lose the progress indicators that the Docker command line gives us."""
    local("docker push {0}".format(name))

def is_network_present(name):
    """Returns true if Docker network 'name' is present on the Docker host, false otherwise."""
    return name in get_network_names()

def get_container_ip_address(container_id, network=None):
    """Returns the [internally-accessible] IP address of a particular container. If a Docker network
    is also specified, it will return the IP address assigned from within that network."""
    ip_address_attribute = ("NetworkSettings.Networks.{0}.IPAddress".format(network) if network else
                            "NetworkSettings.IPAddress")
    ip_address = get_container_attribute(container_id, ip_address_attribute)
    return ip_address

def get_container_hostname(container_id):
    """Returns the hostname of the specified container. Note that this is not the fully qualified
    domain name."""
    return get_container_attribute(container_id, "Config.Hostname")

def get_container_attribute(container_id, dot_separated_key):
    """Helper function that gets a specified container's attribute, as requested in the form of a
    dot-separated string. That is, every level of nesting in the container's metadata is denoted
    by a period."""
    container_attributes = _get_container_attributes(container_id)
    return get_nested_value(container_attributes, dot_separated_key)

def get_host_port_binding(container_id, container_port):
    """Return the port on the Docker host to which a particular container's port is being
    redirected."""
    ports = get_container_attribute(container_id,
                                    "NetworkSettings.Ports.{0}/tcp".format(container_port))
    return ports[0].get('HostPort') if ports else None

def get_networks():
    """Returns a list of Docker networks present on the host."""
    return [network for network in client.networks()]

def get_network_names():
    """Returns a list of every Docker network name on the host."""
    return [network["Name"] for network in get_networks()]

def get_network_id(name):
    """Returns the Docker network ID corresponding to a particular network name on the host."""
    for network in get_networks():
        if network["Name"] == name:
            return network["Id"]

def raise_for_exit_code(container_id):
    """Raises ContainerExitCodeException if a particular container exits with a non-zero exit
    code."""
    exit_code = get_container_attribute(container_id, "State.ExitCode")
    if exit_code != 0:
        raise ContainerExitCodeException("Container {0} exited with code {1}.".format(container_id,
                                                                                      exit_code))

def remove_network(name):
    """Removes the specified Docker network from the host."""
    client.remove_network(name)

DEFAULT_NETWORKS = ["bridge", "host", "none"]
def remove_all_networks():
    """Removes all Docker networks from the host (except for the DEFAULT_NETWORKS)."""
    for network in get_network_names():
        if network not in DEFAULT_NETWORKS:
            remove_network(network)

def get_network_subnet(network_id):
    """Get a particular Docker network's subnet."""
    networks = get_networks()
    for network in networks:
        if network['Id'] == network_id:
            return network['IPAM']['Config'][0]['Subnet']

    # If we get through the loop and never find the network, something has gone very wrong.
    raise NetworkNotFoundException(
        "Cannot find network (Id: {0}). Networks present: {1}".format(network_id, networks)
    )

def get_network_subnets():
    """Returns a list of all the subnets to which Docker networks on the host have been assigned."""
    return [network["IPAM"]["Config"][0]["Subnet"] for network in get_networks() if
            network["IPAM"]["Config"]]

def get_network_container_hostnames(name):
    """Returns a list of every container hostname in the specified network."""
    for network in get_networks():
        if network["Name"] == name:
            return [get_container_hostname(container) for container in network["Containers"]]

def get_container_id(hostname, network_name):
    """Returns the container ID corresponding to a given hostname in a particular Docker network.
    This is relevant because a single Docker host can happily run containers duplicate hostnames
    as long as they are isolated by Docker networks."""
    for network in get_networks():
        if network['Name'] == network_name:
            for container in network['Containers']:
                if get_container_hostname(container) == hostname:
                    return container

def get_container_ip_from_hostname(hostname, network='bridge'):
    """Returns the IP address of a container given its hostname within the Docker network."""
    container_id = get_container_id(hostname, network)
    return get_container_ip_address(container_id, network)

def overlaps_network_subnet(subnet):
    """Takes subnet as string in CIDR format (e.g. 192.168.123.0/24) and returns true if it overlaps
    any existing Docker network subnets."""
    subnets = [IPNetwork(docker_subnet) for docker_subnet in get_network_subnets()]
    return IPNetwork(subnet) in subnets

NETWORK_SUBNET_START = Constants.network.subnet_start # pylint: disable=no-member
def get_available_network_subnet(start_subnet=NETWORK_SUBNET_START):
    """Returns the next unused network subnet available to a Docker network in CIDR format."""
    subnet = IPNetwork(start_subnet)
    while overlaps_network_subnet(str(subnet)):
        subnet = subnet.next(1)
    return str(subnet)

def _get_container_attributes(container_id):
    """Return a dictionary containing all of a container's attributes."""
    if not container_id:
        raise Exception("Tried to inspect container with null id.")
    return client.inspect_container(container=container_id)

def get_all_containers():
    """Returns a list of Docker containers on the host. This list contains dictionaries full of
    container metadata."""
    return client.containers(all=True)

def _get_images():
    return client.images(quiet=True)

def _get_running_containers():
    return client.containers(all=False, quiet=True)

def kill_container(name):
    """Kills a particular running Docker container on the host."""
    logger.info("Killing container %s...", name)
    return client.kill(container=name)

def kill_all_containers():
    """Kills all running Docker containers on the host."""
    for container in _get_running_containers():
        kill_container(name=container["Id"])

def remove_container(name):
    """Removes a particular Docker container on the host. If it is currently running, it will first
    be killed (via force)."""
    client.remove_container(container=name, force=True)

def remove_all_images():
    """Removes all Docker images on the host, using force to handle any images currently being run
    as containers. This will skip any clusterdock images, to avoid killing the process running the
    function itself."""
    for image in client.images():
        if 'org.apache.hbase.is-clusterdock' not in image['Labels']:
            client.remove_image(image, force=True)

def get_clusterdock_container_id():
    """Returns the container ID of the Docker container running clusterdock.
    """
    with quiet():
        for cgroup in local('cat /proc/self/cgroup', capture=True).stdout.split('\n'):
            if 'docker' in cgroup:
                return cgroup.rsplit('/')[-1]

        # If we get through the loop and never find the cgroup, something has gone very wrong.
        raise ContainerNotFoundException('Could not find container name from /proc/self/cgroup.')


def remove_all_containers():
    """Removes all containers on the Docker host. This will also handle cleanup of any volumes
    mounted to the host."""
    clusterdock_container_id = get_clusterdock_container_id()

    for container in get_all_containers():
        # Before removing containers, get a list of host folders being mounted inside (denoted by
        # a label during container creation (e.g. "volume0=/directory/on/host") and delete them.
        host_folders_to_delete = [value
                                  for key, value in container['Labels'].iteritems()
                                  if 'volume' in key
                                  # Don't delete any host folder like '/name' (eventually we may
                                  # want to allow this, but we want to avoid accidentally removing
                                  # / if someone is careless...). By checking rsplit('/',1)[0] and
                                  # stopping if it's an empty string, we can do this.
                                  and value.rsplit('/', 1)[0]]
        if host_folders_to_delete:
            # Since clusterdock is intended to be run out of a Docker container, we delete host
            # folders by mounting their parent into another container and then simply running rm -r.
            binds = ["{0}:/tmp{1}".format(mount.rsplit('/', 1)[0], i)
                     for i, mount in enumerate(host_folders_to_delete, start=1)]
            volumes = [mount.rsplit(':', 1)[-1] for mount in binds]
            rm_command = ['rm', '-r'] + [join("/tmp{0}".format(i), mount.rsplit('/', 1)[-1])
                                         for i, mount in enumerate(host_folders_to_delete, start=1)]
            logger.info("Removing host volumes (%s)...", host_folders_to_delete)

            utility_image = 'busybox:latest'
            pull_image_if_missing(name=utility_image)
            container_configs = {
                'image': utility_image,
                'command': rm_command,
                'volumes': volumes,
                'host_config': client.create_host_config(binds=binds)
            }
            container_id = client.create_container(**container_configs)['Id']
            client.start(container=container_id)
            for line in client.logs(container=container_id, stream=True):
                stdout.write(line)
                stdout.flush()
            delete_host_folders_exit_code = client.wait(container=container_id)
            if delete_host_folders_exit_code != 0:
                logger.warning("Exit code of %d encountered when deleting folders %s. "
                               "Continuing...", delete_host_folders_exit_code,
                               host_folders_to_delete)
            client.remove_container(container=container_id)

        if container['Id'] != clusterdock_container_id:
            remove_container(name=container['Id'])
