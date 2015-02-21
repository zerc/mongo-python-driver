# Copyright 2013-2014 MongoDB, Inc.
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

"""Tools for mocking parts of PyMongo to test other parts."""

import contextlib
import fcntl
import random
import struct
import errno
import os
import socket
from functools import partial
import threading
import weakref
import select
import time
import bson

from pymongo import common
from pymongo import MongoClient
from pymongo.ismaster import IsMaster
from pymongo.message import MAX_INT32, MIN_INT32
from pymongo.monitor import Monitor
from pymongo.pool import Pool, PoolOptions
from pymongo.server_description import ServerDescription

from test import host as default_host, port as default_port


class MockPool(Pool):
    def __init__(self, client, pair, *args, **kwargs):
        # MockPool gets a 'client' arg, regular pools don't. Weakref it to
        # avoid cycle with __del__, causing ResourceWarnings in Python 3.3.
        self.client = weakref.proxy(client)
        self.mock_host, self.mock_port = pair

        # Actually connect to the default server.
        Pool.__init__(self,
                      (default_host, default_port),
                      PoolOptions(connect_timeout=20))

    @contextlib.contextmanager
    def get_socket(self, all_credentials, checkout=False):
        client = self.client
        host_and_port = '%s:%s' % (self.mock_host, self.mock_port)
        if host_and_port in client.mock_down_hosts:
            raise socket.error('mock error')

        assert host_and_port in (
            client.mock_standalones
            + client.mock_members
            + client.mock_mongoses), "bad host: %s" % host_and_port

        with Pool.get_socket(self, all_credentials) as sock_info:
            sock_info.mock_host = self.mock_host
            sock_info.mock_port = self.mock_port
            yield sock_info


class MockMonitor(Monitor):
    def __init__(
            self,
            client,
            server_description,
            topology,
            pool,
            topology_settings):
        # MockMonitor gets a 'client' arg, regular monitors don't.
        self.client = client
        Monitor.__init__(
            self,
            server_description,
            topology,
            pool,
            topology_settings)

    def _check_once(self):
        address = self._server_description.address
        response = self.client.mock_is_master('%s:%d' % address)
        return ServerDescription(address, IsMaster(response), 0)


class MockClient(MongoClient):
    def __init__(
            self, standalones, members, mongoses, ismaster_hosts=None,
            *args, **kwargs):
        """A MongoClient connected to the default server, with a mock topology.

        standalones, members, mongoses determine the configuration of the
        topology. They are formatted like ['a:1', 'b:2']. ismaster_hosts
        provides an alternative host list for the server's mocked ismaster
        response; see test_connect_with_internal_ips.
        """
        self.mock_standalones = standalones[:]
        self.mock_members = members[:]

        if self.mock_members:
            self.mock_primary = self.mock_members[0]
        else:
            self.mock_primary = None

        if ismaster_hosts is not None:
            self.mock_ismaster_hosts = ismaster_hosts
        else:
            self.mock_ismaster_hosts = members[:]

        self.mock_mongoses = mongoses[:]

        # Hosts that should raise socket errors.
        self.mock_down_hosts = []

        # Hostname -> (min wire version, max wire version)
        self.mock_wire_versions = {}

        # Hostname -> max write batch size
        self.mock_max_write_batch_sizes = {}

        kwargs['_pool_class'] = partial(MockPool, self)
        kwargs['_monitor_class'] = partial(MockMonitor, self)

        super(MockClient, self).__init__(*args, **kwargs)

    def kill_host(self, host):
        """Host is like 'a:1'."""
        self.mock_down_hosts.append(host)

    def revive_host(self, host):
        """Host is like 'a:1'."""
        self.mock_down_hosts.remove(host)

    def set_wire_version_range(self, host, min_version, max_version):
        self.mock_wire_versions[host] = (min_version, max_version)

    def set_max_write_batch_size(self, host, size):
        self.mock_max_write_batch_sizes[host] = size

    def mock_is_master(self, host):
        min_wire_version, max_wire_version = self.mock_wire_versions.get(
            host,
            (common.MIN_WIRE_VERSION, common.MAX_WIRE_VERSION))

        max_write_batch_size = self.mock_max_write_batch_sizes.get(
            host, common.MAX_WRITE_BATCH_SIZE)

        # host is like 'a:1'.
        if host in self.mock_down_hosts:
            raise socket.timeout('mock timeout')

        if host in self.mock_standalones:
            return {
                'ok': 1,
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'maxWriteBatchSize': max_write_batch_size}

        if host in self.mock_members:
            ismaster = (host == self.mock_primary)

            # Simulate a replica set member.
            response = {
                'ok': 1,
                'ismaster': ismaster,
                'secondary': not ismaster,
                'setName': 'rs',
                'hosts': self.mock_ismaster_hosts,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'maxWriteBatchSize': max_write_batch_size}

            if self.mock_primary:
                response['primary'] = self.mock_primary

            return response

        if host in self.mock_mongoses:
            return {
                'ok': 1,
                'ismaster': True,
                'minWireVersion': min_wire_version,
                'maxWireVersion': max_wire_version,
                'msg': 'isdbgrid',
                'maxWriteBatchSize': max_write_batch_size}

        # In test_internal_ips(), we try to connect to a host listed
        # in ismaster['hosts'] but not publicly accessible.
        raise socket.error('Unknown host: %s' % host)

    def _process_kill_cursors_queue(self):
        # Avoid the background thread causing races, e.g. a surprising
        # reconnect while we're trying to test a disconnected client.
        pass


OP_REPLY = 1
OP_QUERY = 2004


class MockServer(object):
    def __init__(self, handler):
        self._handler = handler
        self._address = None
        self._sock = None
        self._accept_thread = None
        self._server_threads = weakref.WeakSet()
        self._stopped = False
        self._stopped_ev = threading.Event()

    def run(self):
        self._sock, port = bind_socket()
        self._address = ('localhost', port)
        self._accept_thread = threading.Thread(target=self.accept_loop)
        self._accept_thread.daemon = True
        self._accept_thread.start()

    def stop(self):
        self._stopped = True
        self._stopped_ev.wait(timeout=10)

    @property
    def address(self):
        return self._address

    @property
    def host(self):
        return self._address[0]

    @property
    def port(self):
        return self._address[1]

    def accept_loop(self):
        """Accept client connections and spawn a thread for each."""
        self._sock.setblocking(0)
        while not self._stopped:
            # Wait a short time to accept.
            if select.select([self._sock.fileno()], [], [], 0.1):
                try:
                    client, client_addr = self._sock.accept()
                except socket.error as error:
                    if error.errno != errno.EAGAIN:
                        print('server-side error: %s' % error)
                else:
                    server_thread = threading.Thread(
                        target=partial(self.server_loop, client))
                    server_thread.daemon = True
                    server_thread.start()
                    self._server_threads.add(server_thread)

        self._stopped_ev.set()

    def server_loop(self, client):
        """Reply to requests. 'client' is a client socket."""
        UNPACK_INT = struct.Struct("<i").unpack
        while True:
            try:
                header = mock_server_receive(client, 16)
                length = UNPACK_INT(header[:4])[0]
                request_id = UNPACK_INT(header[4:8])[0]
                operation = UNPACK_INT(header[12:])[0]

                msg = mock_server_receive(client, length - 16)
                if operation == OP_QUERY:
                    flags, = UNPACK_INT(msg[:4])
                    collection, collection_len = bson._get_c_string(msg, 4)
                    offset = 4 + collection_len
                    num_to_skip, = UNPACK_INT(msg[offset:offset + 4])
                    offset += 4
                    docs = bson.decode_all(msg[offset:])
                    if len(docs) == 2:
                        query, fields = docs
                    else:
                        query, fields = docs, {}

                    reply_doc = self._handler(operation, query)
                    reply_msg = reply_message(request_id, [reply_doc])
                    client.sendall(reply_msg)
                else:
                    raise AssertionError('todo')
            except socket.error as error:
                if error.errno == errno.EAGAIN:
                    time.sleep(0.01)
                    continue
                elif error.errno == errno.ECONNRESET:
                    return
                elif error.args[0] != 'closed':
                    print('server thread error %s' % error)


def bind_socket():
    for res in set(socket.getaddrinfo('localhost', None, socket.AF_INET,
                                      socket.SOCK_STREAM, 0,
                                      socket.AI_PASSIVE)):

        af, socktype, proto, _, sock_addr = res
        sock = socket.socket(af, socktype, proto)
        flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFD)
        fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Automatic port allocation with port=None.
        sock.bind(sock_addr)
        bound_port = sock.getsockname()[1]
        sock.listen(128)
        return sock, bound_port

    raise socket.error('could not bind socket')


def mock_server_receive(sock, length):
    msg = b''
    while length:
        chunk = sock.recv(length)
        if chunk == b'':
            raise socket.error('closed')

        length -= len(chunk)
        msg += chunk

    return msg


def reply_message(response_to, docs):
    flags = struct.pack("<i", 0)
    cursor_id = struct.pack("<q", 0)
    starting_from = struct.pack("<i", 0)
    number_returned = struct.pack("<i", 1)
    reply_id = random.randint(MIN_INT32, MAX_INT32)

    data = b''.join([flags, cursor_id, starting_from, number_returned])
    data += b''.join([bson.BSON.encode(doc) for doc in docs])

    message = struct.pack("<i", 16 + len(data))
    message += struct.pack("<i", reply_id)
    message += struct.pack("<i", response_to)
    message += struct.pack("<i", OP_REPLY)
    return message + data
