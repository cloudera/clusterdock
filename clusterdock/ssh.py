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

"""This module contains some basic wrappers of the Fabric API to facilitate using SSH to execute
commands on cluster nodes."""

import os

import fabric.api
from fabric.api import env, execute, run, parallel
from fabric.context_managers import quiet, settings, show
import fabric.state

SSH_TIMEOUT_IN_SECONDS = 1
SSH_MAX_RETRIES = 60

env.disable_known_hosts = True
fabric.state.output['running'] = False

@parallel(pool_size=8)
@fabric.api.task
def _quiet_task(command, ssh_key):
    with settings(quiet(), always_use_pty=False, output_prefix=False, key_filename=ssh_key,
                  connection_attempts=SSH_MAX_RETRIES, timeout=SSH_TIMEOUT_IN_SECONDS):
        return run(command)

@parallel(pool_size=8)
@fabric.api.task
def _task(command, ssh_key):
    with settings(show('stdout'), always_use_pty=False, output_prefix=False, key_filename=ssh_key,
                  connection_attempts=SSH_MAX_RETRIES, timeout=SSH_TIMEOUT_IN_SECONDS):
        return run(command)

def quiet_ssh(command, hosts, ssh_key):
    """Execute command over SSH on hosts, suppressing all output. This is useful for instances where
    you may only want to see if a command succeeds or times out, since stdout is otherwise
    discarded."""
    return execute(_quiet_task, command=command, hosts=hosts, ssh_key=ssh_key)

def ssh(command, hosts, ssh_key):
    """Execute command over SSH on hosts."""
    return execute(_task, command=command, hosts=hosts, ssh_key=ssh_key)
