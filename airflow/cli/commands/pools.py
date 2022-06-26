#
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
"""Pools sub-commands"""
import json
import warnings
from json import JSONDecodeError

import rich_click as click

from airflow.api.client import get_current_api_client
from airflow.cli import airflow_cmd, click_output, click_verbose
from airflow.cli.simple_table import AirflowConsole
from airflow.exceptions import PoolNotFound
from airflow.utils import cli as cli_utils


@airflow_cmd.group("pools")
def pools():
    """Manage pools"""


def _show_pools(pools, output):
    AirflowConsole().print_as(
        data=pools,
        output=output,
        mapper=lambda x: {
            "pool": x[0],
            "slots": x[1],
            "description": x[2],
        },
    )


@pools.command("list")
@click_output
@click_verbose
@cli_utils.suppress_logs_and_warning_click_compatible
def pool_list(output, verbose):
    """Displays info of all the pools"""
    api_client = get_current_api_client()
    pools = api_client.get_pools()
    _show_pools(pools=pools, output=output)


@pools.command("get")
@click.argument("name")
@click_output
@click_verbose
@cli_utils.suppress_logs_and_warning_click_compatible
def pool_get(name, output, verbose):
    """Displays pool info by a given name"""
    api_client = get_current_api_client()
    try:
        pools = [api_client.get_pool(name=name)]
        _show_pools(pools=pools, output=output)
    except PoolNotFound:
        raise SystemExit(f"Pool {name} does not exist")


@pools.command("set")
@click.argument("name")
@click.argument("slots")
@click.argument("description")
@click_output
@click_verbose
@cli_utils.action_cli(check_cli_args=False)
@cli_utils.suppress_logs_and_warning_click_compatible
def pool_set(name, slots, description, output, verbose):
    """Creates new pool with a given name and slots"""
    warnings.warn("Option `--output` is unused and will be removed in a future version.", DeprecationWarning)
    api_client = get_current_api_client()
    api_client.create_pool(name=name, slots=slots, description=description)
    AirflowConsole().print(f"Pool {name} created")


@pools.command("delete")
@click.argument("name")
@click_output
@click_verbose
@cli_utils.action_cli(check_cli_args=False)
@cli_utils.suppress_logs_and_warning_click_compatible
def pool_delete(name, output, verbose):
    """Deletes pool by a given name"""
    warnings.warn("Option `--output` is unused and will be removed in a future version.", DeprecationWarning)
    api_client = get_current_api_client()
    try:
        api_client.delete_pool(name=name)
        AirflowConsole().print(f"Pool {name} deleted")
    except PoolNotFound:
        raise SystemExit(f"Pool {name} does not exist")


@pools.command("import")
@click.argument("filepath", type=click.Path(exists=True, dir_okay=False))
@click_verbose
@cli_utils.action_cli(check_cli_args=False)
@cli_utils.suppress_logs_and_warning_click_compatible
def pool_import(filepath, verbose):
    """Imports pools from a JSON file.

    \b
    Example file format:
        {
            "pool_1": {"slots": 5, "description": ""},
            "pool_2": {"slots": 10, "description": "test"}
        }

    """
    pools, failed = pool_import_helper(filepath)
    if len(failed) > 0:
        raise SystemExit(f"Failed to update pool(s): {', '.join(failed)}")
    AirflowConsole().print(f"Uploaded {len(pools)} pool(s)")


@pools.command("export")
@click.argument("filepath", type=click.Path())
def pool_export(filepath):
    """Exports all of the pools to the file"""
    pools = pool_export_helper(filepath)
    AirflowConsole().print(f"Exported {len(pools)} pools to {filepath}")


def pool_import_helper(filepath):
    """Helps import pools from the json file"""
    api_client = get_current_api_client()

    with open(filepath) as poolfile:
        data = poolfile.read()
    try:
        pools_json = json.loads(data)
    except JSONDecodeError as e:
        raise SystemExit(f"Invalid json file: {e}")
    pools = []
    failed = []
    for k, v in pools_json.items():
        if isinstance(v, dict) and len(v) == 2:
            pools.append(api_client.create_pool(name=k, slots=v["slots"], description=v["description"]))
        else:
            failed.append(k)
    return pools, failed


def pool_export_helper(filepath):
    """Helps export all of the pools to the json file"""
    api_client = get_current_api_client()
    pool_dict = {}
    pools = api_client.get_pools()
    for pool in pools:
        pool_dict[pool[0]] = {"slots": pool[1], "description": pool[2]}
    with open(filepath, 'w') as poolfile:
        poolfile.write(json.dumps(pool_dict, sort_keys=True, indent=4))
    return pools
