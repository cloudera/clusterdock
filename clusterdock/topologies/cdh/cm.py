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
import sys
from ConfigParser import ConfigParser
from os.path import dirname, join
from time import sleep, time

import requests

# We use this PYTHONPATH hack because we want to use the cm_api package without installing it into
# our virtualenv (since we want to leave out Cloudera-specific artifacts from the clusterdock
# installation process). As a result, to get the cm_api in, we need to put the parent folder of
# cm_api on the path so that imports done within the module (e.g. "from cm_api.api_client
# import...") resolve properly. Otherwise, they would need to be changed to include a prefix
# referencing clusterdock.
sys.path.insert(0, dirname(__file__))
from cm_api.api_client import ApiResource

from clusterdock.topologies.cdh import cm_utils
from clusterdock.utils import XmlConfiguration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_CM_PORT = '7180'
DEFAULT_CM_USERNAME = 'admin'
DEFAULT_CM_PASSWORD = 'admin'

def xml(properties):
    return XmlConfiguration(properties=properties).to_string(hide_root=True)

class ClouderaManagerDeployment(object):
    def __init__(self, cm_server_address, cm_server_port=DEFAULT_CM_PORT,
                 username=DEFAULT_CM_USERNAME, password=DEFAULT_CM_PASSWORD):
        self.cm_server_address = cm_server_address
        self.cm_server_port = cm_server_port
        self.username = username
        self.password = password

    def setup_api_resources(self):
        self.api = ApiResource(server_host=self.cm_server_address, server_port=self.cm_server_port,
                               username=self.username, password=self.password,
                               version=self._get_api_version())

        self.cm = self.api.get_cloudera_manager()
        self.cluster = self.api.get_cluster('Cluster 1 (clusterdock)')

    def prep_for_start(self):
        pass

    def validate_services_started(self, timeout_min=10, healthy_time_threshold_sec=30):
        start_validating_time = time()
        healthy_time = None

        logger.info('Beginning service health validation...')
        while healthy_time is None or (time() - healthy_time < healthy_time_threshold_sec):
            if (time() - start_validating_time < timeout_min * 60):
                all_services = list(self.cluster.get_all_services()) + [self.cm.get_service()]
                at_fault_services = list()
                for service in all_services:
                    if (service.serviceState != "NA" and service.serviceState != "STARTED"):
                        at_fault_services.append([service.name, "NOT STARTED"])
                    elif (service.serviceState != "NA" and service.healthSummary != "GOOD"):
                        checks = list()
                        for check in service.healthChecks:
                            if (check["summary"] not in ("GOOD", "DISABLED")):
                                checks.append(check["name"])
                        at_fault_services.append([service.name,
                                                 "Failed health checks: {0}".format(checks)])

                if not healthy_time or at_fault_services:
                    healthy_time = time() if not at_fault_services else None
                sleep(3)
            else:
                raise Exception(("Timed out after waiting {0} minutes for services to start "
                                "(at fault: {1}).").format(timeout_min, at_fault_services))
        logger.info("Validated that all services started (time: %.2f s).",
                    time() - start_validating_time)

    def add_hosts_to_cluster(self, secondary_node_fqdn, all_fqdns):
        cm_utils.add_hosts_to_cluster(api=self.api, cluster=self.cluster,
                                      secondary_node_fqdn=secondary_node_fqdn,
                                      all_fqdns=all_fqdns)

    def update_hive_metastore_namenodes(self):
        for service in self.cluster.get_all_services():
            if service.type == 'HIVE':
                logger.info('Updating NameNode references in Hive metastore...')
                update_metastore_namenodes_cmd = service.update_metastore_namenodes().wait()
                if not update_metastore_namenodes_cmd.success:
                    logger.warning(("Failed to update NameNode references in Hive metastore "
                                    "(command returned %s)."), update_metastore_namenodes_cmd)

    def update_database_configs(self):
        cm_utils.update_database_configs(api=self.api, cluster=self.cluster)

    def _get_api_version(self):
        api_version_response = requests.get(
            "http://{0}:{1}/api/version".format(self.cm_server_address,
                                                self.cm_server_port),
            auth=(self.username, self.password))
        api_version_response.raise_for_status()
        api_version = api_version_response.content
        if 'v' not in api_version:
            raise Exception("/api/version returned unexpected result (%s).", api_version)
        else:
            logger.info("Detected CM API %s.", api_version)
            return api_version.strip('v')
