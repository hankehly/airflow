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

import importlib
import logging
import os
import unittest

import pytest
from click.testing import CliRunner
from parameterized import parameterized
from rich.console import Console

from airflow.cli.commands import info
from airflow.config_templates import airflow_local_settings
from airflow.logging_config import configure_logging
from airflow.version import version as airflow_version
from tests.test_utils.config import conf_vars


class TestPiiAnonymizer(unittest.TestCase):
    def setUp(self) -> None:
        self.instance = info.PiiAnonymizer()

    def test_should_remove_pii_from_path(self):
        home_path = os.path.expanduser("~/airflow/config")
        assert "${HOME}/airflow/config" == self.instance.process_path(home_path)

    @parameterized.expand(
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://p...s@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres/airflow",
                "postgresql+psycopg2://postgres/airflow",
            ),
        ]
    )
    def test_should_remove_pii_from_url(self, before, after):
        assert after == self.instance.process_url(before)


class TestAirflowInfo:
    @classmethod
    def teardown_class(cls) -> None:
        for handler_ref in logging._handlerList[:]:  # type: ignore
            logging._removeHandlerRef(handler_ref)  # type: ignore
        importlib.reload(airflow_local_settings)
        configure_logging()

    @staticmethod
    def unique_items(items):
        return {i[0] for i in items}

    @conf_vars(
        {
            ("core", "executor"): "TEST_EXECUTOR",
            ("core", "dags_folder"): "TEST_DAGS_FOLDER",
            ("core", "plugins_folder"): "TEST_PLUGINS_FOLDER",
            ("logging", "base_log_folder"): "TEST_LOG_FOLDER",
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
            ('logging', 'remote_logging'): 'True',
            ('logging', 'remote_base_log_folder'): 's3://logs-name',
        }
    )
    def test_airflow_info(self):
        importlib.reload(airflow_local_settings)
        configure_logging()
        instance = info.AirflowInfo(info.NullAnonymizer())
        expected = {
            'executor',
            'version',
            'task_logging_handler',
            'plugins_folder',
            'base_log_folder',
            'remote_base_log_folder',
            'dags_folder',
            'sql_alchemy_conn',
        }
        assert self.unique_items(instance._airflow_info) == expected

    def test_system_info(self):
        instance = info.AirflowInfo(info.NullAnonymizer())
        expected = {'uname', 'architecture', 'OS', 'python_location', 'locale', 'python_version'}
        assert self.unique_items(instance._system_info) == expected

    def test_paths_info(self):
        instance = info.AirflowInfo(info.NullAnonymizer())
        expected = {'airflow_on_path', 'airflow_home', 'system_path', 'python_path'}
        assert self.unique_items(instance._paths_info) == expected

    def test_tools_info(self):
        instance = info.AirflowInfo(info.NullAnonymizer())
        expected = {
            'cloud_sql_proxy',
            'gcloud',
            'git',
            'kubectl',
            'mysql',
            'psql',
            'sqlite3',
            'ssh',
        }
        assert self.unique_items(instance._tools_info) == expected

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info(self):
        runner = CliRunner()
        result = runner.invoke(info.show_info)
        output = result.output.strip()
        assert result.exit_code == 0
        assert airflow_version in output
        assert "postgresql+psycopg2://postgres:airflow@postgres/airflow" in output

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info_anonymize(self):
        runner = CliRunner()
        result = runner.invoke(info.show_info, ["--anonymize"])
        output = result.output.strip()
        assert airflow_version in output
        assert "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow" in output


class TestInfoCommandMockHttpx:
    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info_anonymize_fileio(self, httpx_mock):
        httpx_mock.add_response(
            url="https://file.io",
            method="post",
            json={
                "success": True,
                "key": "f9U3zs3I",
                "link": "https://file.io/TEST",
                "expiry": "14 days",
            },
            status_code=200,
        )
        runner = CliRunner()
        result = runner.invoke(info.show_info, ["--file-io"])
        output = result.output.strip()
        assert "https://file.io/TEST" in output
