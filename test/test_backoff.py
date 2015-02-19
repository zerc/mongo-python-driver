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

import sys
import threading
from functools import partial

try:
    import queue  # Python 3.
except ImportError:
    import Queue as queue

sys.path[0:0] = [""]

from pymongo.monitor import run_monitor
from test import unittest
from test.utils import wait_until


class BackoffMonitor(object):
    def __init__(self, test_case):
        self.test_case = test_case
        self.stopped = False
        self.waits = []
        self.lock = threading.Lock()
        # Series of fake durations.
        self.q = queue.Queue()
        self._selections_failing_since = None
        self._clock = 0

    def open(self):
        self.stopped = False

    def close(self):
        self.stopped = True
        self.request_check()

    def request_check(self):
        with self.lock:
            if self._selections_failing_since is None:
                self._selections_failing_since = self._clock

            self.q.put(0)

    def cancel_backoff(self):
        with self.lock:
            self._selections_failing_since = None
            self.q.put(0)

    def _run(self):
        # Actually check the server, perhaps with one retry, and call
        # Topology.on_change(server_description).
        pass

    def _wait(self, timeout):
        self.waits.append(timeout)
        # Pretend some seconds have passed.
        unpaused_after = self.q.get()
        self._clock += unpaused_after

    def _timer(self):
        return self._clock

    def _random(self):
        return 0.5

    # For testing.
    def unpause_after(self, seconds):
        with self.lock:
            # Pretend seconds have passed.
            self.q.put(seconds)


class TestBackoff(unittest.TestCase):
    def setUp(self):
        self.monitor_stopped = False
        self.monitor = BackoffMonitor(self)
        self.thread = threading.Thread(
            target=partial(run_monitor, self.monitor))
        self.addCleanup(self.monitor.close)
        self.thread.daemon = True
        self.thread.start()

    def assertWaitsEqual(self, t):
        def equals():
            try:
                self.assertAlmostEqual(t, sum(self.monitor.waits))
                return True
            except AssertionError:
                return False

        msg = "wait %.2f seconds" % t
        wait_until(equals, msg)

    def test_normal_heartbeat(self):
        self.assertWaitsEqual(10)
        self.monitor.unpause_after(0)
        self.assertWaitsEqual(20)

    def test_backoff(self):
        self.assertWaitsEqual(10)

        # Start failing.
        self.monitor.request_check()
        self.assertWaitsEqual(10.01)

        self.monitor.unpause_after(0)
        self.assertWaitsEqual(10.03)

    def test_cancel_backoff(self):
        self.assertWaitsEqual(10)

        # Start failing.
        self.monitor.request_check()
        self.assertWaitsEqual(10.01)

        # Stop failing.
        self.monitor.cancel_backoff()
        self.assertWaitsEqual(20.01)


if __name__ == "__main__":
    unittest.main()
