# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import json
import time
from typing import Union, Dict

import aredis
from aredis import StrictRedis

from anduin.frames.client_base import ClientBase
from anduin.common import dbg,get_obj_name


class AsyncRedisClient(ClientBase):
    def __init__(self,*args):
        super().__init__(*args)
        self.db:Union[StrictRedis,None] = self.connect_db()

    def lock(self):
        self.is_lock = True

    def unlock(self):
        self.is_lock = False

    def connect_db(self)->Union[StrictRedis,None]:
        try:
            res = aredis.StrictRedis(host=self._host, port=self._port, password=self._psw,db=self._dbname,)
            dbg('连接创建成功', get_obj_name(self))
            return res
        except Exception as e:
            dbg('连接创建失败', e, get_obj_name(self))
            return


    async def hmget(self,key:str,*args) -> Dict[str,Dict]:
        res = await self.db.hmget(key,*args)
        r = {}
        for i in range(0, len(args)):
            k = args[i]
            v = res[i - 1]
            # print(k, v, v.decode('utf-8'))
            r[k] = json.loads(v.decode('utf-8').replace("'", '"'))
        self.last_connect_time = int(time.time())
        return r

    async def hmset(self,*args):
        res = await self.db.hmset(*args)
        self.last_connect_time = int(time.time())
        return res
    async def get(self,*args):
        res = await self.db.get(*args)
        if isinstance(res,bytes):
            return res.decode('utf-8')
        self.last_connect_time = int(time.time())
        return res

    async def set(self,*args):
        res = await self.db.set(*args)
        self.last_connect_time = int(time.time())
        return res

    async def expire(self,*args):
        res = await self.db.expire(*args)
        self.last_connect_time = int(time.time())
        return res







