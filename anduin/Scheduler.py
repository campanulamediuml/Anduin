import datetime
import sys
import threading
import time

# import _thread as thread
from functools import wraps

TIME_ACCURACY = 1  # 时间精度，时间精度不是越小越好！你的Task每次循环超过了这个值,将影响准确度
start_time = int(time.time())

def time_to_str(times=time.time()):
    if times == 0:
        return '2019-09-24 00:00:00'
    date_array = datetime.datetime.utcfromtimestamp(times + (8 * 3600))
    return date_array.strftime("%Y-%m-%d %H:%M:%S")

def get_filename():
    fn = '.anduin/%s-%s.log'%(sys.argv[0].split('/')[-1],start_time)
    return fn

def dbg(*args):
    res = ['[%s]'%time_to_str(int(time.time()))]+list(args)
    print(*res)
    fh = open(get_filename(),'a')
    for i in res:
        fh.write(str(i)+' ')
    fh.write('\n')
    fh.close()

def IntervalTask(sec, func, params=(), immediatly=True, thread_name=''):
    def run(*func_params):
        if immediatly is False:
            time.sleep(sec)
        while 1:
            func(*func_params)
            time.sleep(sec)

    # dbg(params)
    t = threading.Thread(target=run, args=params)
    if thread_name != '':
        t.name = thread_name
    t.start()
    # t.join()


def normal_task(sec, func, params=(), thread_name=''):
    def run(*func_params):
        time.sleep(sec)
        func(*func_params)

    t = threading.Thread(target=run, args=params)
    if thread_name != '':
        t.name = thread_name
    t.start()
    # t.join()
    # threading.enumerate()
    # dbg(threading.enumerate())

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

if "__main__" == __name__:
    # res = [(1,2,3),(4,5,6),(7,8,9)]
    # dbg(turbo(foo,res))
    get_filename()
