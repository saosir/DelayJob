#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools

import jsonschema
import tornado.gen
import tornado.web
import tornado.util

RESP_CODE_SUCCESS = "Success"
RESP_CODE_FAIL = "Failure"
RESP_CODE_TIMEOUT = "Timeout"


def validate_json_schema(schema):
    validator = jsonschema.Draft4Validator(
        schema, format_checker=jsonschema.FormatChecker())

    def wrapper(func):
        @functools.wraps(func)
        def wrapped(req, *args, **kwargs):
            json_data, err = req.service.json_loads(req.request.body)
            if err:
                return req.resp_err_json(RESP_CODE_FAIL, err)

            errors = [
                error.message for error in validator.iter_errors(json_data)
            ]
            if errors:
                req.log.warning("[%s]json validate error: %s",
                                func.__name__,
                                errors)
                return req.resp_err_json(RESP_CODE_FAIL,
                                         "json data validate",
                                         details=errors)

            return func(req, *args, json_data=json_data, **kwargs)

        return wrapped

    return wrapper


class BaseHandler(tornado.web.RequestHandler):
    @property
    def service(self):
        return self.application.service

    @property
    def storage(self):
        return self.service.storage

    @property
    def log(self):
        return self.service.log

    @property
    def config(self):
        return self.service.config

    def resp_err_json(self, code=RESP_CODE_FAIL, message="", details=None):
        resp = {
            "code": code,
            "message": message,
        }
        if details:
            resp["details"] = details
        self.log.debug("%s %s data:%s resp:%s",
                       self.request.method,
                       self.request.uri,
                       self.request.body, resp)
        self.write(resp)

    def resp_json(self, code=RESP_CODE_SUCCESS, message=None, data=None):
        resp = {
            "code": code,
            "message": message or "",
            "data": {} if data is None else data
        }

        self.log.debug("%s %s data:%s resp:%s",
                       self.request.method,
                       self.request.uri,
                       self.request.body, resp)
        self.write(resp)


class PopHandler(BaseHandler):
    @validate_json_schema({
        "type": "object",
        "properties": {
            "topic": {"type": "string", "minLength": 1, "maxLength": 256},
            "timeout": {"type": "integer", "minimum": 0, "maximum": 30}
        },
        "required": ["topic"]
    })
    @tornado.gen.coroutine
    def post(self, json_data):
        topic = json_data["topic"]
        job = None

        timeout = json_data.get("timeout", None)
        job, err = yield self.storage.pop_job(topic, timeout)
        self.resp_json(data=job, message=err)

    def on_connection_close(self):
        self.log.debug("client close stream")


class PushHandler(BaseHandler):
    @validate_json_schema({
        "type": "object",
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 256},
            "topic": {"type": "string", "minLength": 1, "maxLength": 256},
            "delay": {"type": "integer",
                      "maximum": 366 * 3600 * 24, "minimum": 0},
            "ttr": {"type": "integer", "maximum": 86400, "minimum": 1},
            "retry": {"type": "integer", "minimum": 0},
            "body": {"type": "string"}
        },
        "required": ["topic", "delay", "ttr", "retry", "body"]
    })
    def post(self, json_data):
        ret, err = self.storage.set_job(json_data["id"], json_data)
        if err is None:
            self.resp_json()
        else:
            self.resp_err_json(message=err)


class GetHandler(BaseHandler):
    @validate_json_schema({
        "type": "object",
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 256}
        },
        "required": ["id"]
    })
    def post(self, json_data):
        job = self.storage.get_job(json_data["id"])
        return self.resp_json(data={"job": job})


class FinishHandler(BaseHandler):
    @validate_json_schema({
        "type": "object",
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 256}
        },
        "required": ["id"]
    })
    def post(self, json_data):
        job = self.storage.remove_job(json_data["id"])
        self.resp_json(data={"delete": job is not None})


class WebApplication(tornado.web.Application):
    def __init__(self, service, *args, **kwargs):
        self.service = service
        handles = [
            (r"/pop", PopHandler),
            (r"/push", PushHandler),
            (r"/finish", FinishHandler),
            (r"/get", GetHandler)
        ]
        super(WebApplication, self).__init__(handles, *args, **kwargs)
