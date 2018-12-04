#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.testing
from tornado.escape import json_decode

from tests import RouteTestCase


class TestRoute(RouteTestCase):
    test_job_id = "test#job"
    test_topic = "test#topic"

    def test_error_get(self):
        data = self.post("/get", json_data={"id": 123})
        self.assertEqual(data["code"], "Failure")
        self.assertEqual(len(data["details"]), 1)

    def test_error_json(self):
        resp = self.fetch("/get", method="POST", body="x=1&y=2")
        data = json_decode(resp.body)
        self.assertEqual(data["code"], "Failure")

    def test_get(self):
        data = self.post("/get", json_data={"id": self.test_job_id})
        self.assertEqual(data["code"], "Success")
        self.assertEqual(data["data"]["job"], None)

    @tornado.testing.gen_test
    def test_pop(self):
        data = yield self.async_post("/pop",
                                     {
                                         "topic": self.test_topic,
                                         "timeout": 1
                                     })
        self.assertEqual(data["code"], "Success")

    def test_push(self):
        job_data = {"id": self.test_job_id,
                    "topic": self.test_topic,
                    "delay": 1,
                    "ttr": 30,
                    "retry": 0,
                    "body": "hello world"}
        data = self.post("/push", json_data=job_data)
        self.assertEqual(data["code"], "Success")

        data = self.post("/get", json_data={"id": self.test_job_id})
        self.assertEqual(data["data"]["job"], job_data)

    def test_finish(self):
        data = self.post("/get", json_data={"id": self.test_job_id})
        self.assertEqual(data["data"]["job"], None)

        job_data = {"id": self.test_job_id,
                    "topic": self.test_topic,
                    "delay": 1,
                    "ttr": 30,
                    "retry": 0,
                    "body": "hello world"}
        data = self.post("/push", json_data=job_data)
        self.assertEqual(data["code"], "Success")
        data = self.post("/get", json_data={"id": self.test_job_id})
        self.assertEqual(data["data"]["job"], job_data)

        data = self.post("/finish", json_data={"id": self.test_job_id})
        self.assertEqual(data["code"], "Success")

        data = self.post("/get", json_data={"id": self.test_job_id})
        self.assertEqual(data["data"]["job"], None)
