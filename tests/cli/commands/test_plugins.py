# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import textwrap
import unittest

from click.testing import CliRunner

from airflow.cli.commands import plugins
from airflow.hooks.base import BaseHook
from airflow.listeners.listener import get_listener_manager
from airflow.plugins_manager import AirflowPlugin
from tests.plugins.test_plugin import AirflowTestPlugin as ComplexAirflowPlugin
from tests.test_utils.mock_plugins import mock_plugin_manager


class PluginHook(BaseHook):
    pass


class TestPlugin(AirflowPlugin):
    name = "test-plugin-cli"
    hooks = [PluginHook]


class TestPluginsCommand(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()

    @mock_plugin_manager(plugins=[])
    def test_should_display_no_plugins(self):
        result = self.runner.invoke(plugins.dump_plugins, ["--output=json"])
        assert "No plugins loaded" in result.output

    @mock_plugin_manager(plugins=[ComplexAirflowPlugin])
    def test_should_display_one_plugins(self):
        result = self.runner.invoke(plugins.dump_plugins, ["--output=json"])
        print(result.output)
        info = json.loads(result.output)
        assert info == [
            {
                'name': 'test_plugin',
                'macros': ['tests.plugins.test_plugin.plugin_macro'],
                'executors': ['tests.plugins.test_plugin.PluginExecutor'],
                'flask_blueprints': [
                    "<flask.blueprints.Blueprint: name='test_plugin' import_name='tests.plugins.test_plugin'>"
                ],
                'appbuilder_views': [
                    {
                        'name': 'Test View',
                        'category': 'Test Plugin',
                        'view': 'tests.plugins.test_plugin.PluginTestAppBuilderBaseView',
                    }
                ],
                'global_operator_extra_links': [
                    '<tests.test_utils.mock_operators.AirflowLink object>',
                    '<tests.test_utils.mock_operators.GithubLink object>',
                ],
                'timetables': ['tests.plugins.test_plugin.CustomCronDataIntervalTimetable'],
                'operator_extra_links': [
                    '<tests.test_utils.mock_operators.GoogleLink object>',
                    '<tests.test_utils.mock_operators.AirflowLink2 object>',
                    '<tests.test_utils.mock_operators.CustomOpLink object>',
                    '<tests.test_utils.mock_operators.CustomBaseIndexOpLink object>',
                ],
                'hooks': ['tests.plugins.test_plugin.PluginHook'],
                'listeners': ['tests.listeners.empty_listener'],
                'source': None,
                'appbuilder_menu_items': [
                    {'name': 'Google', 'href': 'https://www.google.com', 'category': 'Search'},
                    {
                        'name': 'apache',
                        'href': 'https://www.apache.org/',
                        'label': 'The Apache Software Foundation',
                    },
                ],
                'ti_deps': ['<TIDep(CustomTestTriggerRule)>'],
            }
        ]
        get_listener_manager().clear()

    @mock_plugin_manager(plugins=[TestPlugin])
    def test_should_display_one_plugins_as_table(self):
        result = self.runner.invoke(plugins.dump_plugins, ["--output=table"])
        # Remove leading spaces
        stdout = "\n".join(line.rstrip(" ") for line in result.output.splitlines())
        # Assert that only columns with values are displayed
        expected_output = textwrap.dedent(
            """\
            name            | hooks
            ================+===========================================
            test-plugin-cli | tests.cli.commands.test_plugins.PluginHook
            """
        )
        self.assertEqual(stdout, expected_output)
