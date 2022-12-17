import threading
import time
TIME_ACCURACY = 1  # 时间精度，时间精度不是越小越好！你的Task每次循环超过了这个值,将影响准确度

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