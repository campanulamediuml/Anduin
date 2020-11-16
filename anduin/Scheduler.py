import threading
import time
# import _thread as thread

TIME_ACCURACY = 1  # 时间精度，时间精度不是越小越好！你的Task每次循环超过了这个值,将影响准确度


def IntervalTask(sec, func, params=(), immediatly=True, thread_name=''):
    def run(*params):
        if immediatly is False:
            time.sleep(sec)
        while 1:
            func(*params)
            time.sleep(sec)

    # print(params)
    t = threading.Thread(target=run, args=params)
    if thread_name != '':
        t.name = thread_name
    t.start()
    # t.join()


def normal_task(sec, func, params=(), thread_name=''):
    def run(*params):
        time.sleep(sec)
        func(*params)

    t = threading.Thread(target=run, args=params)
    if thread_name != '':
        t.name = thread_name
    t.start()
    # t.join()
    # threading.enumerate()
    # print(threading.enumerate())



if "__main__" == __name__:
    # res = [(1,2,3),(4,5,6),(7,8,9)]
    # print(turbo(foo,res))
    pass
