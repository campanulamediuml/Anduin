# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import asyncio
import json

from anduin import Redis, ARedis
from anduin import MySQL
from anduin import SMySQL
# Redis

conf = {
    'host':'127.0.0.1',
    'port':6379,
    'database':0,
    # 'charset':'utf8'
}

redis_cm = ARedis(conf)
client = redis_cm.get_free_client()
async def set_data(k,v):
    data = await client.set(k,v)
    print(data)
    return

async def get_data(k):
    data = await client.get(k)
    print(data)
    print(data.decode())
    return data


async def hmset(n,h):
    res = await client.hmset(n, h)
    print(res)
async def hmget(*args):
    r = await client.hmget(*args)
    print(r)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(set_data('tmp_data', 'xxx'))
    # loop.run_until_complete(hmget('tmp_data','data_k_list','data_content'))
    loop.run_until_complete(get_data('tmp_data'))