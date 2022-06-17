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
from typing import Dict, List

from airflow import plugins_manager
from airflow.cli import airflow_cmd, click_output, click_verbose
from airflow.cli.simple_table import AirflowConsole
from airflow.plugins_manager import get_plugin_info
from airflow.utils.cli import suppress_logs_and_warning_click_compatible


@airflow_cmd.command("plugins")
@click_output
@click_verbose
@suppress_logs_and_warning_click_compatible
def dump_plugins(output, verbose):
    """Dump information about loaded plugins"""
    plugins_info: List[Dict[str, str]] = get_plugin_info()
    console = AirflowConsole()
    if not plugins_manager.plugins:
        console.print("No plugins loaded")
        return

    # Remove empty info
    if output == "table":
        # We can do plugins_info[0] as the element it will exist as there's
        # at least one plugin at this point
        for col in list(plugins_info[0]):
            if all(not bool(p[col]) for p in plugins_info):
                for plugin in plugins_info:
                    del plugin[col]

    console.print_as(plugins_info, output=output)
