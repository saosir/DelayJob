#!/usr/bin/env python
# -*- coding: utf-8 -*-
from tornado.testing import AsyncHTTPTestCase
from tornado.escape import json_encode, json_decode
from tornado.concurrent import Future

from DelayJob.server import Server


class RouteTestCase(AsyncHTTPTestCase):
    def get_app(self):
        server = Server({"debug": False,
                         "host": "localhost",
                         "port": 12345,
                         "storage": "mem"})
        return server.web_app

    def post(self, url, json_data):
        response = self.fetch(url, method="POST", body=json_encode(json_data))
        self.assertEqual(response.code, 200)
        return json_decode(response.body)

    def async_post(self, url, json_data, **kwargs):
        http_ft = self.http_client.fetch(self.get_url(url),
                                         method='POST',
                                         body=json_encode(json_data),
                                         **kwargs)
        future = Future()

        def response_to_json(resp_ft):
            future.set_result(json_decode(resp_ft.result().body))

        http_ft.add_done_callback(response_to_json)
        return future
