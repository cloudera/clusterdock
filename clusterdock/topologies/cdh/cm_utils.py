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
from time import sleep, time

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def add_hosts_to_cluster(api, cluster, secondary_node_fqdn, all_fqdns):
    """Add all CM hosts to cluster."""

    # Wait up to 60 seconds for CM to see all hosts.
    TIMEOUT_IN_SECS = 60
    TIMEOUT_TIME = time() + TIMEOUT_IN_SECS
    while time() < TIMEOUT_TIME:
        all_hosts = api.get_all_hosts()
        # Once hostname changes have propagated through CM, we switch all_hosts to be a list of
        # hostIds (since that's what CM uses).
        if set([host.hostname for host in all_hosts]) == set(all_fqdns):
            all_hosts = [host.hostId for host in all_hosts]
            break
        sleep(1)
    else:
        raise Exception("Timed out waiting for CM to recognize all hosts (saw: {0}).".format(
            ', '.join(all_hosts)
        ))

    hosts_in_cluster = [host.hostId for host in cluster.list_hosts()]
    # Use Set.difference to get all_hosts - hosts_in_cluster.
    hosts_to_add = list(set(all_hosts).difference(hosts_in_cluster))
    logger.info("Adding hosts (Ids: %s) to %s...", ', '.join(hosts_to_add), cluster.displayName)
    cluster.add_hosts(hosts_to_add)

    secondary_node_template = get_secondary_node_template(
        api=api, cluster=cluster, secondary_node_fqdn=secondary_node_fqdn
    )
    logger.info('Sleeping for 30 seconds to ensure that parcels are activated...')
    sleep(30)

    logger.info('Applying secondary host template...')
    secondary_node_template.apply_host_template(host_ids=hosts_to_add, start_roles=False)

def get_secondary_node_template(api, cluster, secondary_node_fqdn):
    template = cluster.create_host_template("template")
    hosts = api.get_all_hosts(view='full')
    for host in hosts:
      if host.hostname == secondary_node_fqdn:
        logger.info('Creating secondary node host template...')
        secondary_node_role_group_refs = []
        for role_ref in host.roleRefs:
          service = cluster.get_service(role_ref.serviceName)
          role = service.get_role(role_ref.roleName)
          secondary_node_role_group_refs.append(role.roleConfigGroupRef)
        template.set_role_config_groups(secondary_node_role_group_refs)
        return template
    else:
        raise Exception("Could not find secondary node ({0}) among hosts ({1}).".format(
            secondary_node_fqdn, ', '.join([host.hostname for host in hosts])
        ))

def set_hdfs_replication_configs(cluster):
    HDFS_SERVICE_NAME = 'HDFS-1'
    hdfs = cluster.get_service(HDFS_SERVICE_NAME)
    hdfs.update_config({
        'dfs_replication': len(cluster.list_hosts()) - 1,

        # Change dfs.replication.max, this helps ACCUMULO and HBASE to start.
        # If this configuration is not changed both services will complain about the Requested
        # replication factor.
        'dfs_replication_max': len(cluster.list_hosts())
    })

def update_database_configs(api, cluster):
    # In our case, the databases are always co-located with the CM host, so we grab that from the
    # ApiResource object and then update various configurations accordingly.
    logger.info('Updating database configurations...')
    cm_service = api.get_cloudera_manager().get_service()
    cm_host_id = cm_service.get_all_roles()[0].hostRef.hostId
    # Called hostname, actually a fully-qualified domain name.
    cm_hostname = api.get_host(cm_host_id).hostname

    for service in cluster.get_all_services():
        if service.type == 'HIVE':
            service.update_config({'hive_metastore_database_host': cm_hostname})
        elif service.type == 'OOZIE':
            for role in service.get_roles_by_type('OOZIE_SERVER'):
                role.update_config({'oozie_database_host': "{0}:7432".format(cm_hostname)})
        elif service.type == 'HUE':
            service.update_config({'database_host': cm_hostname})
        elif service.type == 'SENTRY':
            service.update_config({'sentry_server_database_host': cm_hostname})

    for role in cm_service.get_roles_by_type('ACTIVITYMONITOR'):
      role.update_config({'firehose_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('REPORTSMANAGER'):
      role.update_config({'headlamp_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('NAVIGATOR'):
      role.update_config({'navigator_database_host': "{0}:7432".format(cm_hostname)})
    for role in cm_service.get_roles_by_type('NAVIGATORMETASERVER'):
      role.update_config({'nav_metaserver_database_host': "{0}:7432".format(cm_hostname)})
