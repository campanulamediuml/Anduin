import asyncio
import os
import time

import config.db_config
import gevent as gevent

from anduin import MySQL,  AsyncMySQL
from anduin.Scheduler import normal_task
from anduin.common.tools import async_decorators, func_time

# session = Data.add_new_sql()

cnf =  {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '033248hyzh',
        'database': 'eyewave_sec',
        'port': 3306,
        'engine':'mysql'
    }


def find_data():
    r = MySQL(cnf)
    Data = r.get_free_client()
    r = Data.find('user',[('username','=','youtube1')])
    print(r)

async def async_find_data():
    r = AsyncMySQL(cnf)
    Data = await r.get_free_client()
    r = await Data.find('user',[('username','=','youtube1')])
    print(r)

if __name__ == '__main__':
    find_data()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_find_data())

