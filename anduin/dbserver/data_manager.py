import time
from ..Scheduler import IntervalTask
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
    min_keep_connection = 10
    keep_cycle = 10

    def __init__(self, db_config):
        self.t_data = db_config
        if 'engine' not in self.t_data or self.t_data['engine'] not in ENGINE_DICT:
            self.t_data['engine'] = mysql

        self.sql_pool = {}
        print('creating DB connection pool...')
        self.new()
        # print('connect done!')
        IntervalTask(data_manager.keep_cycle, self.keep_connect)

        self.mem_cache = {}
    # def kill_hanged_connection(self):
    #     dead_sql = []
    #
    def get_table_name(self,table_name):
        if '.' in table_name:
            return table_name
        else:
            return self.t_data['database']+'.'+table_name

    def keep_connect(self):
        can_update_sql = []
        for sql in self.sql_pool.values():
            if int(time.time()) - sql.last_execute_time > 30:
                can_update_sql.append(sql)
        if len(can_update_sql) == 1:
            sql = can_update_sql[0]
            sql.become_busy()
            sql.keep_connect()
            sql.become_free()
        else:
            self.kill_unused_connection(can_update_sql)
        return

    def kill_unused_connection(self, kill_connect):
        count = 0
        for sql_id in kill_connect:
            if sql_id in self.sql_pool:
                if int(time.time()) - self.sql_pool[sql_id].last_execute_time > 30:
                    self.sql_pool.pop(sql_id)
                    count += 1
        # print('计划清理',len(kill_connect),'个空闲连接，实际清理',count,'个')
        return

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
        for sql in self.sql_pool.values():
            if sql.is_busy() is False:
                sql.become_busy()
                return sql
        # print('数据库连接池全忙状态，创建新的数据库链接')
        sql = self.create_new_sql()
        sql.become_busy()
        self.add_new_sql(sql)
        # print('创建完毕')
        # print('数据库连接池全忙状态，创建新的数据库链接', '新连接id:', id(sql))
        return sql

    def add_new_sql(self, sql):
        self.sql_pool[id(sql)] = sql
        return

    def new(self):
        sql = self.create_new_sql()
        # print(id(sql))
        self.add_new_sql(sql)
        return

    def create_new_sql(self, ):
        sql = Base(self.t_data['host'], self.t_data['user'], self.t_data['password'], self.t_data['database'],self.t_data['engine'])
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

    def find(self, table, conditions, or_cond, fields=('*',), order=None, show_sql=False,from_cache = False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        if from_cache == True:
            query = sql.find_info(table, conditions, or_cond, fields, None, order, None)
            query += " limit 1"
            res = self.get_table_data(table,query)
            if res is not None:
                sql.become_free()
                return res

        result = sql.find(table, conditions, or_cond, fields, order, show_sql)
        if result is None:
            return
        self.mem_cache[table] = {
            result['query']: result['result']
        }
        sql.become_free()
        return result['result']

    def select(self, table, conditions, or_cond, fields=('*',), group=None, order=None, limit=None, show_sql=False,from_cache=False):
        sql = self.find_free_sql()
        table = self.get_table_name(table)
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        if from_cache == True:
            query = sql.find_info(table, conditions, or_cond, fields, group, order, limit)
            res = self.get_table_data(table, query)
            if res is not None:
                sql.become_free()
                return res

        result = sql.select(table, conditions, or_cond, fields, group, order, limit, show_sql)
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

    def query(self, sql_query, show_sql=False):
        # print(sql)
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.query_one(sql_query, show_sql)
        sql.become_free()
        return result

    def truncate(self, table, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.truncate(table, show_sql)
        sql.become_free()
        return result
