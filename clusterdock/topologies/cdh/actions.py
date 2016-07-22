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

import logging
from argparse import Namespace
from collections import OrderedDict
from datetime import datetime
from os import makedirs
from os.path import dirname, join
from socket import getfqdn
from sys import stdout
from time import sleep
from uuid import uuid4

from docker import Client

from clusterdock import Constants
from clusterdock.cluster import Cluster, Node, NodeGroup
from clusterdock.docker_utils import (get_host_port_binding, is_image_available_locally,
                                      pull_image)
from clusterdock.topologies.cdh.cm import ClouderaManagerDeployment
from clusterdock.utils import wait_for_port_open

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_CLOUDERA_NAMESPACE = Constants.DEFAULT.cloudera_namespace # pylint: disable=no-member

def start(args):
    primary_node_image = "{0}/{1}/clusterdock:{2}_{3}_primary-node".format(
        args.registry_url, args.namespace or DEFAULT_CLOUDERA_NAMESPACE,
        args.cdh_string, args.cm_string
    )

    secondary_node_image = "{0}/{1}/clusterdock:{2}_{3}_secondary-node".format(
        args.registry_url, args.namespace or DEFAULT_CLOUDERA_NAMESPACE,
        args.cdh_string, args.cm_string
    )

    for image in [primary_node_image, secondary_node_image]:
        if args.always_pull or not is_image_available_locally(image):
            logger.info("Pulling image %s. This might take a little while...", image)
            pull_image(image)

    CM_SERVER_PORT = 7180

    primary_node = Node(hostname=args.primary_node[0], network=args.network,
                        image=primary_node_image, ports=[CM_SERVER_PORT],
                        volumes=[])

    secondary_nodes = [Node(hostname=hostname, network=args.network, image=secondary_node_image,
                            volumes=[])
                       for hostname in args.secondary_nodes]

    secondary_node_group = NodeGroup(name='secondary', nodes=secondary_nodes)
    node_groups = [NodeGroup(name='primary', nodes=[primary_node]),
                   secondary_node_group]

    cluster = Cluster(node_groups=node_groups, network_name=args.network)
    cluster.start()

    '''
    A hack is needed here. In short, Docker mounts a number of files from the host into
    the container (and so do we). As such, when CM runs 'mount' inside of the containers
    during setup, it sees these ext4 files as suitable places in which to install things.
    Unfortunately, CM doesn't have a blacklist to ignore filesystem types and only including
    our containers' filesystem in the agents' config.ini whitelist is insufficient, since CM
    merges that list with the contents of /proc/filesystems. To work around this, we copy
    the culprit files inside of the container, which creates those files in aufs. We then
    unmount the volumes within the container and then move the files back to their original
    locations. By doing this, we preserve the contents of the files (which is necessary for
    things like networking to work properly) and keep CM happy.
    '''
    filesystem_fix_commands = []
    for file in ['/etc/hosts', '/etc/resolv.conf', '/etc/hostname', '/etc/localtime']:
        filesystem_fix_commands.append("cp {0} {0}.1; umount {0}; mv {0}.1 {0};".format(file))
    filesystem_fix_command = ' '.join(filesystem_fix_commands)
    cluster.ssh(filesystem_fix_command)

    change_cm_server_host(cluster, primary_node.fqdn)
    if len(secondary_nodes) > 1:
        additional_nodes = [node for node in secondary_nodes[1:]]
        remove_files(cluster, files=['/var/lib/cloudera-scm-agent/uuid',
                                     '/dfs*/dn/current/*'],
                     nodes=additional_nodes)

    # It looks like there may be something buggy when it comes to restarting the CM agent. Keep
    # going if this happens while we work on reproducing the problem.
    try:
        restart_cm_agents(cluster)
    except:
        pass

    logger.info('Waiting for Cloudera Manager server to come online...')
    cm_server_startup_time = wait_for_port_open(primary_node.ip_address,
                                                CM_SERVER_PORT, timeout_sec=180)
    logger.info("Detected Cloudera Manager server after %.2f seconds.", cm_server_startup_time)
    cm_server_web_ui_host_port = get_host_port_binding(primary_node.container_id,
                                                       CM_SERVER_PORT)

    logger.info("CM server is now accessible at http://%s:%s",
                getfqdn(), cm_server_web_ui_host_port)

    deployment = ClouderaManagerDeployment(cm_server_address=primary_node.ip_address)
    deployment.setup_api_resources()

    if len(cluster) > 2:
        deployment.add_hosts_to_cluster(secondary_node_fqdn=secondary_nodes[0].fqdn,
                                        all_fqdns=[node.fqdn for node in cluster])

    deployment.update_database_configs()
    deployment.update_hive_metastore_namenodes()

    if args.include_service_types:
        # CM maintains service types in CAPS, so make sure our args.include_service_types list
        # follows the same convention.
        service_types_to_leave = args.include_service_types.upper().split(',')
        for service in deployment.cluster.get_all_services():
            if service.type not in service_types_to_leave:
                logger.info('Removing service %s from %s...', service.name, deployment.cluster.displayName)
                deployment.cluster.delete_service(service.name)
    elif args.exclude_service_types:
        service_types_to_remove = args.exclude_service_types.upper().split(',')
        for service in deployment.cluster.get_all_services():
            if service.type in service_types_to_remove:
                logger.info('Removing service %s from %s...', service.name, deployment.cluster.displayName)
                deployment.cluster.delete_service(service.name)

    logger.info("Deploying client configuration...")
    deployment.cluster.deploy_client_config().wait()

    if not args.dont_start_cluster:
        logger.info('Starting cluster...')
        if not deployment.cluster.start().wait().success:
            raise Exception('Failed to start cluster.')
        logger.info('Starting Cloudera Management service...')
        if not deployment.cm.get_service().start().wait().success:
            raise Exception('Failed to start Cloudera Management service.')

        deployment.validate_services_started()

    logger.info("We'd love to know what you think of our CDH topology for clusterdock! Please "
                "direct any feedback to our community forum at "
                "http://tiny.cloudera.com/hadoop-101-forum.")

def restart_cm_agents(cluster):
    logger.info('Restarting CM agents...')
    cluster.ssh('service cloudera-scm-agent restart')

def change_cm_server_host(cluster, server_host):
    change_server_host_command = (
        r'sed -i "s/\(server_host\).*/\1={0}/" /etc/cloudera-scm-agent/config.ini'.format(
            server_host
        )
    )
    logger.info("Changing server_host to %s in /etc/cloudera-scm-agent/config.ini...",
                server_host)
    cluster.ssh(change_server_host_command)

def remove_files(cluster, files, nodes):
    logger.info("Removing files (%s) from hosts (%s)...",
                ', '.join(files), ', '.join([node.fqdn for node in nodes]))
    cluster.ssh('rm -rf {0}'.format(' '.join(files)), nodes=nodes)
