# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from anduin.common import get_obj_name, dbg
from anduin.frames.client_base import ClientBase
import clickhouse_driver

class ClickHouse(ClientBase):
    def __init__(self, *args):
        super().__init__(*args)
        pass

    def connect_db(self):
        try:
            res = clickhouse_driver.connect(host=self._host, user=self._user, password=self._psw, database=self._dbname,
                                    port=self._port,
                                      connect_timeout=self.time_out)
            dbg('连接创建成功', get_obj_name(self))
            return res
        except Exception as e:
            dbg('连接创建失败', e, get_obj_name(self))
            return

    def delete(self):
        pass
