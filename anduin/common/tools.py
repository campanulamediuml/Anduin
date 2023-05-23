import datetime
import functools
import inspect
import os
import random
import sqlite3
import string
import sys
import time
from functools import wraps
from typing import Any

import aredis
import pymysql

ENGINE_SQLITE = 'sqlite'
ENGINE_MYSQL = 'mysql'
ENGINE_REDIS = 'async_redis'
danger_sig = ['--', '-+', '#']
start_time = int(time.time())
ENGINE_DICT = {
    ENGINE_MYSQL: pymysql,
    ENGINE_SQLITE: sqlite3,
    ENGINE_REDIS: aredis,
}
sql_clean_time = 45
LOG_PATH = '%s/.anduin/' % os.path.expanduser('~')


def time_to_str(times):
    date_array = datetime.datetime.utcfromtimestamp(times + (8 * 3600))
    return date_array.strftime("%Y-%m-%d %H:%M:%S")

def str_to_time(time_str):
    timeArray = time.strptime(str(time_str), "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(timeArray))
    return time_stamp


def get_filename():
    if sys.platform != 'win32':
        fn = '%s%s-%s.log' % (LOG_PATH, sys.argv[0].split('/')[-1], start_time)
    else:
        fn = '%s\\.anduin\\%s-%s.log' % (os.path.expanduser('~'), sys.argv[0].split('\\')[-1], start_time)
    return fn


fn = get_filename()
if sys.platform != 'win32':
    try:
        os.mkdir('%s/.anduin' % os.path.expanduser('~'))
    except Exception as e:
        print(str(e))
    print('系统内核为', sys.platform, '本插件调用日志保存在%s' % get_filename())
else:
    print('windows下不支持持久化日志，忽略本通知')

if sys.platform != 'win32':
    write_log = True
    fh = open(fn, 'a')
else:
    write_log = False


def dbg(*args):
    res = ['[%s Anduin Engine]' % time_to_str(int(time.time()))] + list(args)
    print(*res)
    if write_log is True:
        for i in res:
            fh.write(str(i) + ' ')
        fh.write('\n')
        # fh.close()


def get_db_index(db_config_dict):
    return '%s@%s:%s' % (db_config_dict.get('user'), db_config_dict.get('host'), db_config_dict.get('database'))


def func_time(f):
    """
    简单记录执行时间
    :param f:
    :return:
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        info = str(f.__name__) + ' took ' + str(end - start) + ' seconds '
        dbg(info)
        return result

    return wrapper


def can_return_directly(res):
    if res is None:
        return True
    if res is False:
        return True
    if isinstance(res, Exception):
        return True


def clean_old_log(filename='*.log'):
    if sys.platform != 'win32':
        os.system('rm %s%s' % (LOG_PATH, filename))


def async_decorators(method):
    """
    异步装饰器装饰异步方法
    :param method: 被装饰协程（异步方法）
    :return:
    """

    @functools.wraps(method)
    async def wrapper(*args, **kwargs):
        # 异步执行
        # 此处必须await 切换调用被装饰协程，否则不会运行该协程
        data = method(*args, **kwargs)
        return data

    return wrapper


def get_obj_name(obj: Any) -> str:
    """获取对象的名称"""
    if inspect.isfunction(obj) or inspect.isclass(obj):
        return obj.__name__
    return obj.__class__.__name__


def create_rand_string(length=16):
    ran_str = ''.join(random.sample(string.ascii_letters + string.digits, length))
    return ran_str
