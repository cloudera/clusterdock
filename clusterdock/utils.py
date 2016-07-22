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

"""This module contains utility functions that may be relevant to more than one topology."""

import operator
from socket import socket
from time import sleep, time

from lxml import etree

def get_nested_value(the_map, dot_separated_key):
    """Give a nested dictionary map, get the value specified by a dot-separated key where dots
    denote an additional depth. Taken from stack overflow (http://stackoverflow.com/a/12414913).
    """
    keys = dot_separated_key.split(".")
    return reduce(operator.getitem, keys[:-1], the_map)[keys[-1]]

def strip_components_from_tar(tar, leading_elements_to_remove=1):
    """Designed to feed tarfile.extractall's members parameter."""
    for the_file in tar:
        split_on_first_dir = the_file.name.split('/', leading_elements_to_remove)
        if len(split_on_first_dir) > leading_elements_to_remove:
            the_file.name = split_on_first_dir[leading_elements_to_remove]
            yield the_file

def wait_for_port_open(address, port, timeout_sec=60):
    """Check the accessibility of address:port in a loop until it succeeds or times out."""
    start_waiting_time = time()
    stop_waiting_time = start_waiting_time + timeout_sec

    while time() < stop_waiting_time:
        if port_is_open(address=address, port=port):
            return time() - start_waiting_time
        sleep(1)

    # If we get here without having returned, we've timed out.
    raise Exception("Timed out after {0} seconds waiting for {1}:{2} to be open.".format(
        timeout_sec, address, port
    ))

def port_is_open(address, port):
    """Returns True if port at address is open."""
    return socket().connect_ex((address, port)) == 0


class XmlConfiguration(object):
    """A class to handle the creation of XML configuration files."""
    def __init__(self, properties=None, root_name='configuration', source_file=None):
        if source_file:
            parser = etree.XMLParser(remove_blank_text=True)
            self.tree = etree.parse(source_file, parser)
            self.root = self.tree.getroot()
        else:
            self.root = etree.Element(root_name)
            self.tree = etree.ElementTree(self.root)

        if properties:
            for the_property in properties:
                self.add_property(the_property, properties[the_property])

    def __str__(self):
        return self.to_string()

    def add_property(self, name, value):
        """Adds a property to the XML configuration."""
        the_property = etree.SubElement(self.root, 'property')
        etree.SubElement(the_property, 'name').text = name
        etree.SubElement(the_property, 'value').text = value

    def to_string(self, hide_root=False):
        """Converts the XmlConfiguration instance into a string."""
        if hide_root:
            # We build this string with concatenation instead of by doing a join on a list
            # because StackOverflow says it's faster this way. I'll take its word for it.
            properties = str()
            for the_property in self.root:
                properties += etree.tostring(the_property, pretty_print=True)
            return properties
        else:
            return etree.tostring(self.tree, pretty_print=True)

    def write_to_file(self, filename):
        """Writes a string representation of the XmlConfiguration instance into a file."""
        self.tree.write(filename, pretty_print=True)
