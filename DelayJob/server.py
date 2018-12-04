#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import logging
import tornado.ioloop
import tornado.escape

from . import web
from . import storage


class Server(object):
    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger(config.get("logger", None))
        self.debug = self.config.get("debug", False)
        if config["storage"] == "mem":
            self.storage = storage.MemoryStorage(self, self.log)
        elif config["storage"] == "redis":
            self.storage = storage.RedisStorage(self, self.log)
        else:
            print("not support storage %s" % config["storage"])
            sys.exit(-1)

        self.web_app = web.WebApplication(self, debug=self.debug)

    def start(self):
        self.http_server = self.web_app.listen(self.config["port"],
                                               self.config["host"])
        self.storage.start()

    def stop(self):
        self.storage.stop()
        self.http_server.stop()

    def json_loads(self, value):
        try:
            return tornado.escape.json_decode(value), None
        except ValueError:
            return None, "json decode error"

    def json_dumps(self, value):
        try:
            return tornado.escape.json_encode(value), None
        except TypeError:
            return None, "json encode error"

    def time(self):
        return tornado.ioloop.IOLoop.current().time()

    def _tornado_start(self):
        tornado.ioloop.IOLoop.current().start()

