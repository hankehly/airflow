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
#
import json
import os
import unittest

from click.testing import CliRunner

from airflow import models, settings
from airflow.cli.commands import pools
from airflow.models import Pool
from airflow.settings import Session
from airflow.utils.db import add_default_pool_if_not_exists


class TestCliPools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.runner = CliRunner()

    def setUp(self):
        super().setUp()
        settings.configure_orm()
        self.session = Session
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    @staticmethod
    def _cleanup(session=None):
        if session is None:
            session = Session()
        session.query(Pool).filter(Pool.pool != Pool.DEFAULT_POOL_NAME).delete()
        session.commit()
        add_default_pool_if_not_exists()
        session.close()

    def test_pool_list(self):
        self.runner.invoke(pools.pools, ["set", "foo", "1", "test"])
        result = self.runner.invoke(pools.pools, ["list"])
        assert "foo" in result.output

    def test_pool_list_with_args(self):
        result = self.runner.invoke(pools.pools, ["list", "--output", "json"])
        assert result.exit_code == 0

    def test_pool_create(self):
        self.runner.invoke(pools.pools, ["set", "foo", "1", "test"])
        assert self.session.query(Pool).count() == 2

    def test_pool_get(self):
        self.runner.invoke(pools.pools, ["set", "foo", "1", "test"])
        result = self.runner.invoke(pools.pools, ["get", "foo"])
        assert result.exit_code == 0

    def test_pool_delete(self):
        self.runner.invoke(pools.pools, ["set", "foo", "1", "test"])
        self.runner.invoke(pools.pools, ["delete", "foo"])
        assert self.session.query(Pool).count() == 1

    def test_pool_import_nonexistent(self):
        result = self.runner.invoke(pools.pools, ["import", "nonexistent.json"])
        assert isinstance(result.exception, SystemExit)

    def test_pool_import_invalid_json(self):
        with open('pools_import_invalid.json', mode='w') as file:
            file.write("not valid json")

        result = self.runner.invoke(pools.pools, ["import", "pools_import_invalid.json"])
        assert isinstance(result.exception, SystemExit)

    def test_pool_import_invalid_pools(self):
        pool_config_input = {"foo": {"description": "foo_test"}}
        with open('pools_import_invalid.json', mode='w') as file:
            json.dump(pool_config_input, file)

        result = self.runner.invoke(pools.pools, ["import", "pools_import_invalid.json"])
        assert isinstance(result.exception, SystemExit)

    def test_pool_import_export(self):
        # Create two pools first
        pool_config_input = {
            "foo": {"description": "foo_test", "slots": 1},
            'default_pool': {'description': 'Default pool', 'slots': 128},
            "baz": {"description": "baz_test", "slots": 2},
        }
        with open('pools_import.json', mode='w') as file:
            json.dump(pool_config_input, file)

        # Import json
        self.runner.invoke(pools.pools, ["import", "pools_import.json"])

        # Export json
        self.runner.invoke(pools.pools, ["export", "pools_export.json"])

        with open('pools_export.json') as file:
            pool_config_output = json.load(file)
            assert pool_config_input == pool_config_output, "Input and output pool files are not same"
        os.remove('pools_import.json')
        os.remove('pools_export.json')
