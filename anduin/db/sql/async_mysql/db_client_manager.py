# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import asyncio
import time


from anduin.common import dbg,get_db_index,ENGINE_MYSQL
from anduin.db.sql.async_mysql.db_client import AsyncMySQLClient
from anduin.frames.manager_base import ManagerBase


class AsyncMySQLManager(ManagerBase):
    def __init__(self, config):
        super().__init__(config)

    async def create_connection(self):
        db_client = AsyncMySQLClient(self.host, self.user, self.password, self.port, self.database, ENGINE_MYSQL,
                                     self.charset)
        await db_client.connect_db()
        my_pool = self.get_cur_client_pool_by_thread_id()
        sid = id(db_client)
        my_pool[sid] = db_client
        dbg('无可用空闲链接，创建链接...', get_db_index(self.t_data))
        return db_client

    async def get_free_client(self):
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
            my_client = await self.create_connection()
        my_client.lock()
        return my_client

