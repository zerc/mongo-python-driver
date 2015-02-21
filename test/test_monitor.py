# Copyright 2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the monitor module."""

import gc
import sys
import time
from functools import partial

sys.path[0:0] = [""]

from pymongo import MongoClient
from pymongo.periodic_executor import _EXECUTORS
from test import client_knobs
from test.pymongo_mocks import MockServer, OP_QUERY
from test import unittest, port, host, IntegrationTest
from test.utils import single_client, one, connected, wait_until


def unregistered(ref):
    gc.collect()
    return ref not in _EXECUTORS


class TestMonitor(IntegrationTest):
    def test_atexit_hook(self):
        client = single_client(host, port)
        executor = one(client._topology._servers.values())._monitor._executor
        connected(client)

        # The executor stores a weakref to itself in _EXECUTORS.
        ref = one([r for r in _EXECUTORS.copy() if r() is executor])

        del executor
        del client

        wait_until(partial(unregistered, ref), 'unregister executor',
                   timeout=5)


class MonitorUnitTest(unittest.TestCase):
    def test_heartbeat(self):
        self.ismaster_times = []

        def handler(operation, docs):
            self.ismaster_times.append(time.time())
            if operation == OP_QUERY and docs[0] == {'ismaster': 1}:
                return {'ok': 1}

        server = MockServer(handler)
        server.run()
        self.addCleanup(server.stop)

        start = time.time()
        frequency = 0.1
        with client_knobs(heartbeat_frequency=frequency):
            client = MongoClient(server.host, server.port)
            wait_until(lambda: client.is_primary, 'connect')
            time.sleep(1)

        duration = time.time() - start
        self.assertAlmostEqual(duration / frequency,
                               # TODO: initial check calls ismaster twice!
                               len(self.ismaster_times) - 1,
                               delta=1)


if __name__ == "__main__":
    unittest.main()
