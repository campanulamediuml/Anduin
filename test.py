import asyncio
import os

import gevent as gevent

from anduin import Data
from anduin.Scheduler import normal_task
from anduin.common.tools import async_decorators, func_time

# session = Data.add_new_sql()

@func_time
def get_wd_data(wd_id):
    session = Data.add_new_sql()
    data = session.select('user',[('id','>',wd_id)])
    return data


def get_user(wd_id):
    data = get_wd_data(wd_id)
    if data is not None:
        print(len(data['result']))
    return data


if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    normal_task(0,get_user,1)
    # loop.run_until_complete(asyncio.wait(tasks))
