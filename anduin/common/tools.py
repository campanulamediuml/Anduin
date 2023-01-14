import datetime
import os
import sqlite3
import sys
import time
from functools import wraps

import pymysql

ENGINE_SQLITE = 'sqlite'
ENGINE_MYSQL = 'mysql'
danger_sig = ['--', '-+', '#']
start_time = int(time.time())
ENGINE_DICT = {
    ENGINE_MYSQL: pymysql,
    ENGINE_SQLITE:sqlite3
}
sql_clean_time = 45
LOG_PATH = '%s/.anduin/'%os.path.expanduser('~')

def time_to_str(times):
    date_array = datetime.datetime.utcfromtimestamp(times + (8 * 3600))
    return date_array.strftime("%Y-%m-%d %H:%M:%S")

def get_filename():
    # if sys.platform != 'win32':
    fn = '%s%s-%s.log'%(LOG_PATH,sys.argv[0].split('/')[-1],start_time)
    # else:
    #     fn = '%s\\.anduin\\%s-%s.log'%(os.path.expanduser('~'),sys.argv[0].split('\\')[-1],start_time)
    return fn

fn = get_filename()
# if sys.platform != 'win32':
try:
    os.mkdir('%s/.anduin'%os.path.expanduser('~'))
except Exception as e:
    print(str(e))
print('anduin调用日志保存在%s'%get_filename())
# else:
#     print('该操作系统为windows系统，暂时无法保存日志')
fh = open(fn, 'a')
def dbg(*args):
    res = ['[%s Anduin Engine]'%time_to_str(int(time.time()))]+list(args)
    print(*res)
    if sys.platform != 'win32':
        for i in res:
            fh.write(str(i)+' ')
        fh.write('\n')
        # fh.close()

def get_db_index(db_config_dict):
    return db_config_dict['user'] + '@' + db_config_dict['host'] + ':' + db_config_dict['database']

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
        os.system('rm %s%s'%(LOG_PATH,filename))

