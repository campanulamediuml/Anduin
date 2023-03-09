# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import abc
import os
import threading
import time
from typing import List, Dict

from anduin.common import dbg
from anduin.frames.client_base import ClientBase

TIMEOUT = 30

class ManagerBase(abc.ABC):
    def __init__(self, db_config):
        self.use_cache = False
        self.t_data = db_config
        self.host = db_config.get('host')
        self.user = db_config.get('user')
        self.password = db_config.get('password')
        self.database = db_config.get('database')
        self.charset = db_config.get('charset')
        self.port = db_config.get('port')
        self.connect_pool = {}
        # {
        #     tid:{
        #         connect_sid:connect
        #     }
        # }
        self.out_time_pool = {}
        # {
        #     tid:[connect_sid]
        # }
        dbg('creating DB connection pool...')

    @staticmethod
    def get_cur_thread_id() -> str:
        mypid = str(os.getpid())
        mytid = str(threading.currentThread().ident)
        thread_id = mypid + '.' + mytid
        return thread_id

    def get_cur_client_pool_by_thread_id(self) -> Dict[int, ClientBase]:
        tid = ManagerBase.get_cur_thread_id()
        if tid not in self.connect_pool:
            self.connect_pool[tid] = {}
        return self.connect_pool.get(tid)

    def get_cur_outtime_pool_by_thread_id(self) -> List[int]:
        tid = ManagerBase.get_cur_thread_id()
        if tid not in self.connect_pool:
            self.out_time_pool[tid] = []
        return self.out_time_pool.get(tid)

    @abc.abstractmethod
    def create_connection(self):
        pass

    @abc.abstractmethod
    def get_free_client(self):
        pass

    def clean_pool(self):
        cur_time = int(time.time())
        cur_out_time_pool = self.get_cur_outtime_pool_by_thread_id()
        cur_pool = self.get_cur_client_pool_by_thread_id()
        for sid,client in cur_pool.items():
            if cur_time - client.last_connect_time > TIMEOUT:
                if client.is_lock is False:
                    cur_out_time_pool.append(sid)
        for sid in cur_out_time_pool:
            if sid in cur_pool:
                cur_pool.pop(sid)



