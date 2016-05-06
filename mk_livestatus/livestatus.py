#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import socket
import json
import time

__all__ = ['Query', 'Socket']


class Query(object):
    def __init__(self, conn, resource):
        self._conn = conn
        self._resource = resource
        self._columns = []
        self._filters = []
        self._command = []

    def call(self):
        try:
            data = bytes(str(self), 'utf-8')
        except TypeError:
            data = str(self)
        return self._conn.call(data)

    __call__ = call

    def __str__(self):
        if self._resource.upper() != 'SEND':
          request = 'GET %s' % (self._resource)
          if self._columns and any(self._columns):
              request += '\nColumns: %s' % (' '.join(self._columns))
          if self._filters:
              for filter_line in self._filters:
                  request += '\nFilter: %s' % (filter_line)
          request += '\nOutputFormat: json\nColumnHeaders: on\n'
        else:
          timestamp = str(int(time.time()))
          request = "COMMAND [{0}] {1}\n".format(timestamp, ' '.join(self._command).encode('utf-8'))
        return request

    def columns(self, *args):
        self._columns = args
        return self

    def filter(self, filter_str):
        self._filters.append(filter_str)
        return self

    def command(self, command_str):
        self._command.append(command_str)
        return self

class Socket(object):
    def __init__(self, peer):
        self.peer = peer

    def __getattr__(self, name):
        return Query(self, name)

    def call(self, request):
        try:
            if len(self.peer) == 2:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            else:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.connect(self.peer)
            s.send(request)
            s.shutdown(socket.SHUT_WR)
            rawdata = s.makefile().read()
            if not rawdata:
                return []
            data = json.loads(rawdata)
            return [dict(zip(data[0], value)) for value in data[1:]]
        finally:
            s.close()
