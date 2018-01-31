# Copyright (C) 2012 - 2018 Cloudera, Inc.
# All Rights Reserved.

FROM debian:wheezy

# Use a label indicating that a container is running the clusterdock framework to allow the
# framework to handle things like stopping containers on the host machine without accidentally
# killing itself.
LABEL org.apache.hbase.is-clusterdock=

ENV DOCKER_BUCKET get.docker.com
ENV DOCKER_VERSION 1.11.1

# Install Docker, just to have the client available; the framework assumes /var/run/docker.sock
# will be volume mounted from the host. That is, executing `docker run` inside a container created
# from this image will start a container on the host machine, not inside said container.
RUN apt-get -y update \
    && apt-get -y install curl \
        git \
        libffi-dev \
        libssl-dev \
        libxml2-dev \
        libxslt1-dev \
        libz-dev \
        python-dev \
        python-pip \
    && curl -fSL "https://${DOCKER_BUCKET}/builds/Linux/x86_64/docker-${DOCKER_VERSION}.tgz" \
        -o docker.tgz \
    && tar -xzvf docker.tgz \
    && mv docker/* /usr/local/bin/ \
    && rmdir docker \
    && rm docker.tgz \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
ADD . /root/clusterdock

# Make sure the SSH private key for each topology has the correct permissions.
RUN find /root/clusterdock -type f -name id_rsa -exec chmod 600 {} \; \
    && pip install --upgrade -r /root/clusterdock/requirements.txt \
    && rm -rf /root/.cache/pip/*

WORKDIR /root/clusterdock
ENTRYPOINT ["python"]
