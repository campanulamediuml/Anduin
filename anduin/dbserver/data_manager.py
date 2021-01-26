# import asyncio
import os
import threading
import time
# from ..Scheduler import IntervalTask
from ..dbserver.base import Base, ENGINE_DICT, mysql

# from config.config import db_config
def dump_table(table, sql):
    if sql.is_busy():
        return
    sql.become_busy()
    result = sql.select(table, [])
    sql.become_free()
    return result

class data_manager(object):
    keep_cycle = 10

    def __init__(self, db_config):
        self.t_data = db_config
        self.min_keep_connection = db_config['min_keep_connection'] if 'min_keep_connection' in db_config else 1
        if 'engine' not in self.t_data or self.t_data['engine'] not in ENGINE_DICT:
            self.t_data['engine'] = mysql
        if 'charset' not in self.t_data:
            self.t_data['charset'] = 'utf8mb4'

        self.threading_pool = {}
        self.threading_out_time_pool = {}
        self.sql_pool = {}
        print('creating DB connection pool...')
        # for i in range(0,self.min_keep_connection):
        self.new()
        print('connect done!')
        # IntervalTask(data_manager.keep_cycle, self.keep_connect)
        self.mem_cache = {}


    def get_table_name(self,table_name):
        if '.' in table_name:
            return table_name
        else:
            return self.t_data['database']+'.'+table_name

    def keep_connect(self):
        pass
        # print('查看死亡链接')
        # can_delete_sql = []
        # count = 0
        # for sql in self.sql_pool.values():
        #     if int(time.time()) - sql.last_execute_time > 40:
        #         count += 1
        #         can_delete_sql.append(id(sql))
        #
        # self.kill_unused_connection(can_delete_sql)
        # for sql in self.sql_pool.values():
        #     if sql.is_busy() == False:
        #         sql.become_busy()
        #         sql.keep_connect()
        #         sql.become_free()
        # return
        # <===================>
        # can_delete_sql_id_list = []
        # count = 0
        # for sql in self.sql_pool.values():
        #     if int(time.time()) - sql.last_execute_time > 40:
        #         count += 1
        #         can_delete_sql_id_list.append(id(sql))
        # self.kill_unused_connection(can_delete_sql_id_list)
        # for sql in self.sql_pool.values():
        #     if sql.is_busy() == False:
        #         sql.become_busy()
        #         sql.keep_connect()
        #         sql.become_free()


    # def kill_unused_connection(self, kill_connect):
    #     count = 0
    #     for sql_id in kill_connect:
    #         if sql_id in self.sql_pool:
    #             self.sql_pool.pop(sql_id)
    #             count += 1
    #     return

    def get_table_data(self,table,query):
        try:
            return self.mem_cache[table][query]
        except:
            return None

    def clean_table(self,table):
        try:
            return self.mem_cache.pop(table) if table in self.mem_cache else None
        except:
            return None

    def find_free_sql(self):
        # try:
        #     sql_list = self.sql_pool.values()
        #     for sql in sql_list:
        #         if sql.is_busy() is False and int(time.time()) - sql.last_execute_time < 29:
        #             sql.become_busy()
        #             return sql
        # except Exception as e:
        #     print(str(e),'查找空闲sql链接时遍历错误')
        #     # pass
        # print('数据库连接池全忙状态，创建新的数据库链接')
        #
        # sql = self.create_new_sql()
        # sql.become_busy()
        # self.add_new_sql(sql)
        # print('创建完毕新连接id:', id(sql))
        # print('数据库连接池全忙状态，创建新的数据库链接', '新连接id:', id(sql))
        # return sql
        mypid = str(os.getpid())
        mytid = str(threading.currentThread().ident)
        thread_id = mypid+'.'+mytid
        # print('本次数据库请求线程id', thread_id)
        if thread_id in self.threading_pool:
            sql_status = self.threading_pool[thread_id]
            if int(time.time()) - sql_status[1] < 45:
                sql = sql_status[0]
                sql.become_busy()
                self.threading_pool[thread_id] = (sql, int(time.time()))
                return sql

        sql = self.create_new_sql()
        sql.become_busy()
        # self.add_new_sql(sql)
        self.threading_pool[thread_id] = (sql,int(time.time()))
        # self.threading_out_time_pool[thread_id] = int(time.time())
        # print('为线程%s更新数据库链接%s'%(thread_id,id(sql)))
        return sql

    # def add_new_sql(self, sql):
    #     self.sql_pool[id(sql)] = sql
    #     return

    def new(self):
        sql = self.create_new_sql()
        # print(id(sql))
        # self.add_new_sql(sql)
        return sql

    def create_new_sql(self, ):
        sql = Base(self.t_data['host'], self.t_data['user'], self.t_data['password'], self.t_data['database'],self.t_data['engine'],self.t_data['charset'])
        # sql.become_busy()
        # self.sql_pool.append(sql)
        return sql

    def create(self, table, colums, table_comment = '',show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.create(table, colums, table_comment,show_sql)
        sql.become_free()
        return result

    def insert(self, table, params, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.insert(table, params, show_sql)
        sql.become_free()
        self.clean_table(table)
        return result

    def find(self, table, conditions, or_cond, fields=('*',), order=None, show_sql=False,from_cache = False,for_update=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        if from_cache == True:
            query,params = sql.find_info(table, conditions, or_cond, fields, None, order, None,for_update)
            query += " limit 1"
            query = query%params
            res = self.get_table_data(table,query)
            if res is not None:
                sql.become_free()
                return res
        result = sql.find(table, conditions, or_cond, fields, order, show_sql,for_update)
        if result is None:
            return
        self.mem_cache[table] = {
            result['query']: result['result']
        }
        sql.become_free()
        return result['result']

    def select(self, table, conditions, or_cond, fields=('*',), group=None, order=None, limit=None, show_sql=False,from_cache=False,for_update=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        if from_cache == True:
            query,params = sql.find_info(table, conditions, or_cond, fields, group, order, limit,for_update)
            query = query % params
            res = self.get_table_data(table, query)
            if res is not None:
                sql.become_free()
                return res

        result = sql.select(table, conditions, or_cond, fields, group, order, limit, show_sql,for_update)
        if result is None:
            return
        self.mem_cache[table] = {
            result['query']:result['result']
        }
        sql.become_free()
        return result['result']

    def update(self, table, conditions,or_cond, params, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        # print('这次更新的params是',params)
        result = sql.update(table, conditions, or_cond, params, show_sql)
        self.clean_table(table)
        sql.become_free()
        return result

    def delete(self, table, conditions, or_cond, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.delete(table, conditions, or_cond,  show_sql)
        self.clean_table(table)
        sql.become_free()
        return result

    def query(self, sql_query, show_sql=False,return_dict=False):
        # print(sql)
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.query(sql_query, show_sql,return_dict=return_dict)
        sql.become_free()
        return result

    def truncate(self, table, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.truncate(table, show_sql)
        sql.become_free()
        return result
