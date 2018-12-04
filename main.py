#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import signal

import tornado
from tornado import options

from DelayJob.server import Server

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    options.define("logger", type=str, help="logger name")
    options.define("config", type=str, help="config file path",
                   default=os.path.join(ROOT_DIR, "config.py"))
    options.define("debug", type=bool, default=False)
    options.define("host", type=str, default="localhost",
                   help="http server host")
    options.define("port", type=int, default=3333, help="http server port")

    options.define("storage", type=str, default="mem",
                   metavar="mem|redis", help="storage choose")
    options.define("mem_sweep_seconds", type=int, default=1,
                   help="memory storage sweep seconds")
    options.define("redis_host", type=str, default="localhost",
                   help="redis storage host")
    options.define("redis_port", type=int, default=6349,
                   help="redis storage port")
    options.define("redis_password", type=str, help="redis storage password")
    options.define("redis_path_prefix", type=str, default="",
                   help="redis storage path prefix")
    options.define("redis_bucket_name_format", type=str, default="bucket_%d",
                   help="redis storage bucket name format")
    options.define("redis_bucket_size", type=int, default=10,
                   help="redis storage bucket size")
    options.define("redis_sweep_seconds", type=int, default=1,
                   help="redis storage sweep seconds")

    options.parse_command_line(sys.argv, False)
    if options.options.config:
        options.options.parse_config_file(options.options.config, False)

    config = options.options.as_dict()

    server = Server(config)
    server.start()
    print("server start to run")

    @tornado.gen.coroutine
    def shutdown():
        print("stopping...")
        server.stop()
        yield tornado.gen.sleep(0.5)
        tornado.ioloop.IOLoop.current().stop()

    def exit_handler(sig, frame):
        tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)

    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
