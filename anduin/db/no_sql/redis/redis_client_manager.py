# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import time

from anduin.common import ENGINE_REDIS,dbg,get_db_index
from anduin.db.no_sql.redis.redis_client import RedisClient
from anduin.frames.manager_base import ManagerBase

TIMEOUT = 30
class RedisManager(ManagerBase):
    def __init__(self,config):
        super().__init__(config)

    def create_connection(self)->RedisClient:
        dbg('无可用空闲链接，创建链接...', get_db_index(self.t_data))
        db_client = RedisClient(self.host, self.user, self.password, self.port, self.database, ENGINE_REDIS,
                                self.charset)
        my_pool = self.get_cur_client_pool_by_thread_id()
        sid = id(db_client)
        my_pool[sid] = db_client
        return db_client

    def get_free_client(self)->RedisClient:
        self.clean_pool()
        my_client = None
        cur_time = int(time.time())
        my_pool = self.get_cur_client_pool_by_thread_id()
        for sid, client in my_pool.items():
            if cur_time - client.last_connect_time < 30:
                if client.is_lock is False:
                    my_client = client
                    break
        if my_client is None:
            my_client = self.create_connection()
        my_client.lock()
        return my_client



