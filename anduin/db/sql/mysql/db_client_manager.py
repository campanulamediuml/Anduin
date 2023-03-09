# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import time

from anduin.common import ENGINE_MYSQL,get_db_index,dbg
from anduin.db.sql.mysql.db_client import MySQLClient
from anduin.frames.manager_base import ManagerBase


class MySQLManager(ManagerBase):
    def __init__(self, config):
        super().__init__(config)

    def create_connection(self):
        dbg('无可用空闲链接，创建链接...', get_db_index(self.t_data))
        db_client = MySQLClient(self.host, self.user, self.password, self.port, self.database, ENGINE_MYSQL,
                                self.charset)
        my_pool = self.get_cur_client_pool_by_thread_id()
        sid = id(db_client)
        my_pool[sid] = db_client
        return db_client

    def get_free_client(self):
        # print(self.get_cur_thread_id())
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
        self.clean_pool()
        return my_client
