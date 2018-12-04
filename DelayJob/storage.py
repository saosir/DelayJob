#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import bisect
import collections
import functools

import tornado.ioloop
import tornado.queues
import tornado.gen
from tornado.ioloop import PeriodicCallback
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor

import redis


class StorageBase(object):
    def __init__(self, server, log):
        self.server = server
        self.log = log

    def set_job(self, key, job):
        raise NotImplementedError

    def get_job(self, key):
        raise NotImplementedError

    def pop_job(self, topic, timeout=None):
        raise NotImplementedError

    def remove_job(self, key):
        raise NotImplementedError

    def stop(self):
        pass

    def start(self):
        pass

    def time(self):
        return self.server.time()


class MemoryStorage(StorageBase):
    def __init__(self, *args, **kwargs):
        super(MemoryStorage, self).__init__(*args, **kwargs)

        self.job_pool = {}
        self.ready_queue = collections.defaultdict(tornado.queues.Queue)
        self.delay_bucket = []
        self.timer = None
        sweep_seconds = self.server.config.get("mem_sweep_seconds", 1) * 1000
        self.timer = PeriodicCallback(self._sweep_wait_job_cb, sweep_seconds)

    def start(self):
        self.timer.start()

    def stop(self):
        self.timer.stop()

    def _add_wait_queue(self, item):
        bisect.insort_right(self.delay_bucket, item)

    def set_job(self, key, job):
        if key in self.job_pool:
            return False, "job#{id} exists".format(id=key)
        self.job_pool[key] = job
        self._add_wait_queue((self.time() + job["delay"], key))
        return True, None

    def get_job(self, key):
        return self.job_pool.get(key)

    @tornado.gen.coroutine
    def pop_job(self, topic, timeout=None):
        """

        :param topic:
        :param timeout: -1
        :return:
        """
        assert topic
        job = None
        key = None
        # FIXME 客户端尝试不同topic会使ready_queue默认许多空queue
        try:
            if timeout is None:
                key = self.ready_queue[topic].get_nowait()
            else:
                timeout = self.time() + timeout
                key = yield self.ready_queue[topic].get(timeout)
            assert key, "ready_queue pop key is None"
            job = self.get_job(key)
        except tornado.gen.TimeoutError:
            self.log.debug("pop topic#{topic} job timeout", topic=topic)
            raise tornado.gen.Return((None, "Timeout"))
        except tornado.queues.QueueEmpty:
            raise tornado.gen.Return((None, "QueueEmpty"))
        except Exception as e:
            raise tornado.gen.Return((None, str(e)))

        if job and job["retry"] > 0:
            # 防止在ttr内未执行完成
            job["retry"] -= 1
            self.job_pool[key] = job
            self._add_wait_queue((self.time() + job["ttr"], key))
        raise tornado.gen.Return((job, None))

    def remove_job(self, key):
        return self.job_pool.pop(key, None)

    def _add_ready_queue(self, topic, job_id):
        self.ready_queue[topic].put(job_id)

    def _sweep_wait_job_cb(self):
        now = self.time()
        self.log.debug("sweep...")
        while self.delay_bucket:
            if self.delay_bucket[0][0] <= now:
                item = self.delay_bucket.pop(0)
                job = self.job_pool.get(item[1])
                if not job:
                    continue
                else:
                    self.log.debug("push job %s-%s to ready_queue",
                                   job["topic"], job["id"])
                    self._add_ready_queue(job["topic"], job["id"])
            else:
                break


def on_try_except_finally(on_except=(None,), on_finally=(None,)):
    except_func = on_except[0]
    finally_func = on_finally[0]

    def decorator(func):
        def wrapper(*args, **kwargs):
            ret = None
            try:
                ret = func(*args, **kwargs)
            except Exception:
                if except_func:
                    return except_func(*on_except[1:])
            finally:
                if finally_func:
                    finally_func(*on_finally[1:])
                return ret

        return wrapper

    return decorator


class RedisStorage(StorageBase):
    executor = ThreadPoolExecutor(5, "redis_storage_thread_executor")

    def __init__(self, *args, **kwargs):
        super(RedisStorage, self).__init__(*args, **kwargs)
        self.redis = redis.Redis(self.server.config["redis_host"],
                                 self.server.config["redis_port"],
                                 password=self.server.config.get(
                                     "redis_password", None))
        self.path_prefix = self.server.config.get("redis_path_prefix", "")
        self.bucket_size = self.server.config.get("redis_bucket_size", 10)
        self.bucket_name_format = self.server.config.get(
            "redis_bucket_name_format", "bucket_%d")
        if self.bucket_size <= 0:
            self.bucket_size = 1

        sweep_seconds = self.server.config.get("redis_sweep_seconds", 1) * 1000
        self.timers = []
        for i in range(self.bucket_size):
            bucket_name = self.bucket_name_format % i
            callback = functools.partial(self._sweep_wait_job_cb, bucket_name)
            self.timers.append(PeriodicCallback(callback, sweep_seconds))

    @property
    def bucket_key(self):
        if not hasattr(self, "_bucket_index"):
            self._bucket_index = 0
        try:
            return self.bucket_name_format % (
                        self._bucket_index % self.bucket_size)
        finally:
            self._bucket_index += 1

    def _add_path_prefix(self, path):
        return os.path.join(self.path_prefix, path)

    @on_try_except_finally()
    def get_job(self, key):
        job = self.redis.get(self._add_path_prefix(key))
        if not job:
            return None
        else:
            json_data, err = self.server.json_loads(job)
            if err:
                self.log.error("redis storage get_job json_loads [%s]", err)
            return json_data

    @on_try_except_finally()
    def remove_job(self, key):
        return self.redis.delete(self._add_path_prefix(key))

    def start(self):
        map(lambda x: x.start(), self.timers)

    def stop(self):
        map(lambda x: x.stop(), self.timers)

    @on_try_except_finally(on_except=(lambda: False,))
    def _put_job(self, key, job):
        return self.redis.set(key, job)

    def set_job(self, key, job):
        try:
            json_data, err = self.server.json_dumps(job)
            if err:
                self.log.error("redis storage set_job json_dumps [%s]", err)
                return False, err

            ret = self._bucket_push(self.time() + job["delay"], job["id"])
            if ret < 0:
                return False, "job#{id} push bucket fail"

            ret = self.redis.set(self._add_path_prefix(key),
                                 json_data,
                                 nx=True)
            if not ret:
                return False, "job#{id} exists".format(id=key)

            return True, None
        except Exception as e:
            return False, str(e)

    @on_try_except_finally(on_except=(lambda: -1,))
    def _bucket_push(self, timestamp, job_id):
        return self.redis.zadd(self._add_path_prefix(self.bucket_key),
                               {job_id: timestamp})

    @on_try_except_finally()
    def _bucket_pop(self, bucket_name):
        return self.redis.zrange(self._add_path_prefix(bucket_name),
                                 0, 0, withscores=True)

    @on_try_except_finally(on_except=(lambda: -1,))
    def _bucket_remove(self, bucket_name, key):
        return self.redis.zrem(self._add_path_prefix(bucket_name), key)

    @run_on_executor
    @on_try_except_finally(on_except=(lambda: None,))
    def _ready_queue_pop(self, topic, timeout):
        # 线程运行防止阻塞
        return self.redis.blpop(topic, timeout)

    @on_try_except_finally(on_except=(lambda: -1,))
    def _ready_queue_push(self, topic, key):
        return self.redis.rpush(self._add_path_prefix(topic), key)

    @tornado.gen.coroutine
    def pop_job(self, topic, timeout=None):
        key = None
        job = None
        try:
            if timeout is None:
                key = self.redis.lpop(topic)
            else:
                ret = yield self._ready_queue_pop(topic, timeout)
                if ret:
                    _, key = ret

            if key:
                job = self.get_job(key)
        except Exception as e:
            raise tornado.gen.Return((None, str(e)))
        if not key:
            raise tornado.gen.Return((None, "QueueEmpty"))
        elif job and job["retry"]:
            job["retry"] -= 1
            json_data, _= self.server.json_dumps(job)
            ret = self.redis.set(self._add_path_prefix(key), json_data, xx=True)
            if ret:
                self._bucket_push(self.time() + job["ttr"], key)

        raise tornado.gen.Return((job, None))

    def _sweep_wait_job_cb(self, bucket_name):
        try:
            while True:
                items = self._bucket_pop(bucket_name)
                if not items:
                    return

                key, timestamp = items[0]
                if timestamp > self.time():
                    return

                job = self.get_job(key)
                if not job:
                    self._bucket_remove(bucket_name, key)
                    continue
                ret = self._ready_queue_push(job["topic"], key)
                if ret < 0:
                    self.log.error("sweep bucket[%s] _ready_queue_push error")
                self._bucket_remove(bucket_name, key)
        except Exception as e:
            self.log.error("sweep bucket[%s] error: %s", bucket_name, str(e))
