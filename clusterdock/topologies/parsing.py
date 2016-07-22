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

"""This module handles the parsing of topologies' profile.cfg files into the command line
arguments used by some of the scripts in the ./bin folder."""

import argparse
import ConfigParser
import os
from collections import OrderedDict

from braceexpand import braceexpand

def get_profile_config_item(topology, section, item):
    """Return a string represenation of a particular topology's section's item's value."""
    config_filename = os.path.join(os.path.dirname(__file__), topology, TOPOLOGIES_CONFIG_NAME)
    config = ConfigParser.ConfigParser(allow_no_value=True)
    config.read(config_filename)
    return config.get(section, item)

ARG_PREFIX = 'arg.'
ARG_HELP_SUFFIX = '.help'
ARG_METAVAR_SUFFIX = '.metavar'

def parse_args_from_config(parser, config, section):
    """Iterate through a particular ConfigParser instance, feeding its contents into an argparse
    ArgumentParser instance."""

    # By creating an argument group (without a title), we clean up our help messages by
    # separating groups by a blank line.
    group = parser.add_argument_group()
    config_args = OrderedDict()
    if config.has_section(section):
        for option in config.options(section):
            if option.startswith(ARG_PREFIX):
                if option.endswith(ARG_HELP_SUFFIX):
                    help_message = config.get(section, option)
                    stripped_option = option[len(ARG_PREFIX):-len(ARG_HELP_SUFFIX)]
                    config_args[stripped_option]['help'] = help_message
                elif option.endswith(ARG_METAVAR_SUFFIX):
                    metavar = config.get(section, option)
                    stripped_option = option[len(ARG_PREFIX):-len(ARG_METAVAR_SUFFIX)]
                    config_args[stripped_option]['metavar'] = metavar
                else:
                    stripped_option = option[len(ARG_PREFIX):]
                    config_args[stripped_option] = dict.fromkeys(['default', 'help', 'metavar'])
                    default = config.get(section, option)
                    config_args[stripped_option]['default'] = default

        # If the default arg is a boolean, the presence of the argument should set a boolean (i.e.
        # it doesn't expect to store the string following the argument).
        for arg in config_args:
            add_argument_options = dict()
            for option in ['default', 'help', 'metavar']:
                if config_args[arg].get(option):
                    add_argument_options[option] = config_args[arg].get(option)

            if config_args[arg].get('default'):
                if config_args[arg].get('default').lower() == 'false':
                    add_argument_options['action'] = 'store_true'
                    del add_argument_options['default']
                elif config_args[arg].get('default').lower() == 'true':
                    add_argument_options['action'] = 'store_false'
                    del add_argument_options['default']

            group.add_argument("--{0}".format(arg), **add_argument_options)

TOPOLOGIES_CONFIG_NAME = 'profile.cfg'
def parse_profiles(parser, action='start'):
    """Given an argparse parser and a cluster action, generate subparsers for each topology."""
    topologies_directory = os.path.dirname(__file__)

    subparsers = parser.add_subparsers(help='The topology to use when starting the cluster',
                                       dest='topology')

    parsers = dict()
    for topology in os.listdir(topologies_directory):
        if os.path.isdir(os.path.join(topologies_directory, topology)):
            # Generate help and optional arguments based on the options under our topology's
            # profile.cfg file's node_groups section.
            config_filename = os.path.join(os.path.dirname(__file__), topology,
                                           TOPOLOGIES_CONFIG_NAME)
            config = ConfigParser.ConfigParser(allow_no_value=True)
            config.read(config_filename)

            parsers[topology] = subparsers.add_parser(
                topology, help=config.get('general', 'description'),
                formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

            # Arguments in the [all] group should be available to all actions.
            parse_args_from_config(parsers[topology], config, 'all')

            if action == 'start':
                for option in config.options('node_groups'):
                    # While we use our custom StoreBraceExpandedAction to process the given values,
                    # we need to separately brace-expand the default to make it show up correctly
                    # in help messages.
                    default = list(braceexpand(config.get('node_groups', option)))
                    parsers[topology].add_argument("--{0}".format(option), metavar='NODES',
                                                   default=default, action=StoreBraceExpandedAction,
                                                   help="Nodes of the {0} group".format(option))
                parse_args_from_config(parsers[topology], config, 'start')
            elif action == 'build':
                parse_args_from_config(parsers[topology], config, 'build')

class StoreBraceExpandedAction(argparse.Action):
    """A custom argparse Action that brace-expands values using the braceexpands module before
    storing them in dest as a list. This lets us not have to do any post-processing of strings like
    'node-{1..4}.internal' later on."""

    # pylint: disable=too-few-public-methods

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(StoreBraceExpandedAction, self).__init__(option_strings, dest,
                                                       **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, list(braceexpand(values)))
