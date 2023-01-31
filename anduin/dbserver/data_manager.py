import os
import threading
import time

from ..common.tools import dbg, ENGINE_DICT, ENGINE_MYSQL, can_return_directly, sql_clean_time
from ..dbserver.base import Base


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
        self.use_cache = False
        self.t_data = db_config
        if 'engine' not in self.t_data or self.t_data['engine'] not in ENGINE_DICT:
            self.t_data['engine'] = ENGINE_MYSQL
        if 'charset' not in self.t_data:
            self.t_data['charset'] = 'utf8mb4'
        if 'cache' in self.t_data:
            self.use_cache = self.t_data['cache']
        self.threading_pool = {}
        self.threading_out_time_pool = {}
        self.sql_pool = {}
        dbg('creating DB connection pool...')
        # for i in range(0,self.min_keep_connection):
        self.new()
        dbg('connect done!')
        # IntervalTask(data_manager.keep_cycle, self.keep_connect)
        self.mem_cache = {}

    # 拼接'库.表'结构
    def get_table_name(self, table_name):
        if self.t_data['engine'] != 'mysql':
            return table_name
        if '.' in table_name:
            return table_name
        else:
            return self.t_data['database'] + '.' + table_name

    # 从缓存中获取sql数据
    def get_table_data(self, table, query):
        try:
            return self.mem_cache[table][query]
        except Exception as e:
            dbg(str(e))
            return None

    def clean_table(self, table):
        try:
            return self.mem_cache.pop(table) if table in self.mem_cache else None
        except Exception as e:
            dbg(str(e))
            return None

    def find_free_sql(self):
        # 获取当前线程绑定的sql-session
        mypid = str(os.getpid())
        mytid = str(threading.currentThread().ident)
        thread_id = mypid + '.' + mytid
        # dbg('本次数据库请求线程id', thread_id)
        if thread_id in self.threading_pool:
            session_status = self.threading_pool[thread_id]
            if int(time.time()) - session_status[1] < sql_clean_time:
                session = session_status[0]
                if session._is_busy != 1:
                    session.become_busy()
                    self.threading_pool[thread_id] = (session, int(time.time()))
                    return session

        session = self.create_new_sql()
        session.become_busy()
        # self.add_new_sql(sql)
        self.threading_pool[thread_id] = (session, int(time.time()))
        # dbg('为线程%s更新数据库链接%s'%(thread_id,id(sql)))
        return session


    def new(self):
        sql = self.create_new_sql()
        # dbg(id(sql))
        # self.add_new_sql(sql)
        return sql

    def create_new_sql(self, ):
        sql = Base(self.t_data['host'], self.t_data['user'], self.t_data['password'], self.t_data['database'],
                   self.t_data['engine'], self.t_data['charset'])
        # sql.become_busy()
        # self.sql_pool.append(sql)
        return sql

    def create(self, table, colums, table_comment='', show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # dbg('执行这次sql请求的链接是', id(sql))
        result = sql.create(table, colums, table_comment, show_sql)
        sql.become_free()
        return result

    def insert(self, table, params, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # dbg('执行这次sql请求的链接是', id(sql))
        result = sql.insert(table, params, show_sql)
        sql.become_free()
        self.clean_table(table)
        return result

    def find(self, table, conditions, or_cond=None, fields=('*',), order=None, show_sql=False, from_cache=False,
             for_update=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        if from_cache is True:
            query, params = sql.find_info(table, conditions, or_cond, fields, None, order, None, for_update)
            query += " limit 1"
            query = query % params
            res = self.get_table_data(table, query)
            if res is not None:
                sql.become_free()
                return res

        result = sql.find(table, conditions, or_cond, fields, order, show_sql, for_update)
         # 获取结果
        # result = get_async_result(async_r)
        sql.become_free()
        if can_return_directly(result) is True:
            return result
        if self.use_cache is True:
            self.mem_cache[table] = {
                result['query']: result['result']
            }
        # sql.become_free()
        return result['result']

    def select(self, table, conditions, or_cond=None, fields=('*',), group=None, order=None, limit=None, show_sql=False,
               from_cache=False, for_update=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # dbg('执行这次sql请求的链接是', id(sql))
        if from_cache is True:
            query, params = sql.find_info(table, conditions, or_cond, fields, group, order, limit, for_update)
            query = query % params
            res = self.get_table_data(table, query)
            if res is not None:
                sql.become_free()
                return res

        result = sql.select(table, conditions, or_cond, fields, group, order, limit, show_sql, for_update)
        sql.become_free()
        if can_return_directly(result) is True:
            return result
        if self.use_cache is True:
            self.mem_cache[table] = {
                result['query']: result['result']
            }
        # sql.become_free()
        return result['result']

    def update(self, table, conditions, or_cond=None, params=None, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        result = sql.update(table, conditions, or_cond, params, show_sql)
        self.clean_table(table)
        sql.become_free()
        return result

    def delete(self, table, conditions, or_cond=None, show_sql=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        result = sql.delete(table, conditions, or_cond, show_sql)
        self.clean_table(table)
        sql.become_free()
        return result

    def query(self, sql_query, show_sql=False, return_dict=False):
        # dbg(sql)
        sql = self.find_free_sql()
        # sql.become_busy()
        # dbg('执行这次sql请求的链接是', id(sql))
        result = sql.query(sql_query, show_sql, return_dict=return_dict)
        sql.become_free()
        return result

    def truncate(self, table, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # dbg('执行这次sql请求的链接是', id(sql))
        result = sql.truncate(table, show_sql)
        sql.become_free()
        return result

    def commit(self):
        sql = self.find_free_sql()
        sql.commit()
        return

