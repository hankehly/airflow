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

"""Kerberos command"""
import daemon
import rich_click as click
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.cli import airflow_cmd, click_daemon, click_log_file, click_pid, click_stderr, click_stdout
from airflow.configuration import conf
from airflow.security import kerberos as krb
from airflow.utils.cli import setup_locations


@airflow_cmd.command()
@click.argument("principal")
@click_stdout
@click_stderr
@click_pid
@click_daemon
@click_log_file
@click.option("-k", "--keytab", metavar="KEYTAB", help="keytab", default=conf.get("kerberos", "keytab"))
def kerberos(principal, stdout, stderr, pid, daemon, log_file, keytab):
    """Start a kerberos ticket renewer"""
    print(settings.HEADER)

    if daemon:
        pid, stdout, stderr, _ = setup_locations("kerberos", pid, stdout, stderr, log_file)
        with open(stdout, "w+") as stdout_handle, open(stderr, "w+") as stderr_handle:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                stdout=stdout_handle,
                stderr=stderr_handle,
            )

            with ctx:
                krb.run(principal=principal, keytab=keytab)
    else:
        krb.run(principal=principal, keytab=keytab)
