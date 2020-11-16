import time
from ..Scheduler import IntervalTask
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
    min_keep_connection = 10
    keep_cycle = 10

    def __init__(self, db_config):
        self.t_data = db_config
        self.sql_pool = {}
        print('creating DB connection pool...')
        self.new()
        # print('connect done!')
        IntervalTask(data_manager.keep_cycle, self.keep_connect)

    # def kill_hanged_connection(self):
    #     dead_sql = []
    #
    def keep_connect(self):
        can_update_sql = []
        for sql in self.sql_pool.values():
            if int(time.time()) - sql.get_last_connect_time() > 30:
                can_update_sql.append(sql)

        count = 0
        kill_connect = []
        for sql in can_update_sql:
            if count > data_manager.min_keep_connection:
                kill_connect.append(id(sql))
                continue
            if sql.is_busy() is False:
                sql.become_busy()
                sql.keep_connect()
                # print(sql.id,'进行心跳')
                sql.become_free()
                count += 1
            # if count > data_manager.min_keep_connection:
            #
            #     break
        self.kill_unused_connection(kill_connect)

        return

    def kill_unused_connection(self, kill_connect):
        count = 0
        for sql_id in kill_connect:
            if sql_id in self.sql_pool:
                if self.sql_pool[sql_id].is_busy() is False:
                    self.sql_pool.pop(sql_id)
                    count += 1
        # print('计划清理',len(kill_connect),'个空闲连接，实际清理',count,'个')
        return

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
        # dbg_db('数据库连接池全忙状态，创建新的数据库链接', '新连接id:', id(sql))
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
        sql = Base(self.t_data['host'], self.t_data['user'], self.t_data['password'], self.t_data['database'])
        # sql.become_busy()
        # self.sql_pool.append(sql)
        return sql

    def create(self, table, colums, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        print('执行这次sql请求的链接是', id(sql))
        result = sql.create(table, colums, show_sql)
        sql.become_free()
        return result

    def insert(self, table, params, is_commit=True, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.insert(table, params, is_commit, show_sql)
        sql.become_free()
        return result

    def find(self, table, conditions, or_cond, fields=('*',), order=None, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.find(table, conditions, or_cond, fields, order, show_sql)
        sql.become_free()
        return result

    def select(self, table, conditions, or_cond, fields=('*',), group=None, order=None, limit=None, show_sql=False):

        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.select(table, conditions, or_cond, fields, group, order, limit, show_sql)
        sql.become_free()
        return result

    def update(self, table, conditions, or_cond, params, is_commit=True, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.update(table, conditions, or_cond, params, is_commit, show_sql)
        sql.become_free()
        return result

    def delete(self, table, conditions, or_cond, is_commit=True, show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.delete(table, conditions, or_cond, is_commit, show_sql)
        sql.become_free()
        return result

    def find_last(self, table, conditions, info, limit, fields="*", show_sql=False):
        sql = self.find_free_sql()
        # sql.become_busy()
        # print('执行这次sql请求的链接是', id(sql))
        result = sql.find_last(table, conditions, info, limit, fields, show_sql)
        sql.become_free()
        return result

    def query(self, sql_query, show_sql=False):
        # print(sql)
        sql = self.find_free_sql()
        # sql.become_busy()
        print('执行这次sql请求的链接是', id(sql))
        result = sql.query(sql_query, show_sql)
        sql.become_free()
        return result
