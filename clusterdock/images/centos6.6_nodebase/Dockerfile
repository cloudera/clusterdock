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
FROM centos:6.6

# Install useful things that are missing from the centos:6.6 image.
RUN yum install -y openssh-clients \
    openssh-server \
    rsyslog \
    sudo \
    tar \
    wget \
    which

# Add pre-created SSH keys into .ssh folder.
ADD ssh /root/.ssh/

# Copy public key into authorized_keys and limit access to the private key to ensure SSH can use it.
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/id_rsa

# The official CentOS Docker images retain a lot of things that only apply on a real machine.
# Among the most problematic is starting udev, which was seen to hang when containers were started
# simultaneously. Simple solution (and one done by the lxc-centos.in template) is to simply
# not trigger it. The other is the inclusion of the 90-nproc.conf file, which overrides reasonable
# defaults for things like the maximum number of user processes when running commands as a non-root
# user. Get rid of it (and see tinyurl.com/zqdfzpg).
RUN sed -i 's|/sbin/start_udev||' /etc/rc.d/rc.sysinit && \
    rm /etc/security/limits.d/90-nproc.conf

# Disable strict host key checking and set the known hosts file to /dev/null to make
# SSH between containers less of a pain.
RUN sed -i -r "s|\s*Host \*\s*|&\n        StrictHostKeyChecking no|" /etc/ssh/ssh_config && \
    sed -i -r "s|\s*Host \*\s*|&\n        UserKnownHostsFile=/dev/null|" /etc/ssh/ssh_config

CMD ["/sbin/init"]
