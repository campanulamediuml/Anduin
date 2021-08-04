import time
import pymysql
import sqlite3

from pymysql.cursors import DictCursor

from .base_method import base_method
from ..Scheduler import dbg

mysql = 'mysql'
sqlite = 'sqlite'

danger_sig = ['--', '-+', '#']

ENGINE_DICT = {
    'mysql': pymysql,
    'sqlite': sqlite3
}


def can_return_directly(res):
    if res is None:
        return True
    if res is False:
        return True
    if isinstance(res, Exception):
        return True


class Base(base_method):
    def __init__(self, host, user, psw, dbname, engine, charset):
        self.id = id(self)
        self._host = host
        self._user = user
        self._psw = psw
        self._dbname = dbname
        self._engine = engine
        self._charset = charset
        self.db_engine = None
        if self._engine in ENGINE_DICT:
            self.db_engine = ENGINE_DICT[self._engine]
        # 选择数据库引擎
        self._is_busy = 0
        self._tables = {}
        self.init_time = int(time.time())
        self.executing_query = ''
        self.last_execute_time = int(time.time())
        self.db = None
        self.connect_db()
        self.last_connect_time = int(time.time())

    def connect_db(self):
        try:
            self.db = self.connect()
            if self._engine != sqlite:
                self._load_tables()
        except Exception as e:
            dbg('connect fail', str(e))
            pass

    def connect(self):
        if self._engine == mysql:
            self.db = self.db_engine.connect(host=self._host, user=self._user, password=self._psw, database=self._dbname, charset=self._charset,
                                             connect_timeout=60)
            return self.db
        if self._engine == sqlite:
            self.db = self.db_engine.connect(self._dbname)
            return self.db

    def become_busy(self):
        self._is_busy = 1
        self.last_execute_time = int(time.time())
        self.last_connect_time = int(time.time())
        return

    def become_free(self):
        self.commit()
        self._is_busy = 0
        self.executing_query = ''
        return

    def disconnect(self):
        self.db.close()

    def is_busy(self):
        if self._is_busy == 1:
            return True
        else:
            return False

    def keep_connect(self):
        if self.db is None:
            self.connect_db()
        if self._engine != sqlite:
            self.query('select 1')

    def load_an_table(self, table):
        if self._engine == sqlite:
            sql = 'PRAGMA table_info(%s)' % table
        else:
            sql = 'show fields from ' + table
        res = self.query(sql, show_sql=False)
        if res is None:
            return
        column_list = list(map(lambda x: x[0], res))
        if 'signal' in column_list:
            column_list.remove('signal')
        self._tables[table] = column_list
        return column_list

    # 加载所有数据库表名
    def _load_tables(self, show_sql=False):
        sql = 'show tables'
        res = self.query(sql, show_sql)
        # dbg(res)
        tables = list(map(lambda x: x[0], res))
        # dbg(tables)
        for table in tables:
            table_name = self._dbname + '.' + table
            self.load_an_table(table_name)

    def _load_all_fileds(self):
        pass

    def create(self, table, colums, table_comment='', show_sql=False):
        sql = 'create table %s(' % table

        tail = ''

        for item in colums:
            col = ''
            if self._engine == sqlite:
                item = item[:-1]
            for i in item:
                col += str(i)
                col += ' '
            col += ','
            tail += col

        tail = tail[:-1] + ')'
        sql += tail
        if self._engine == mysql:
            sql += 'charset=utf8mb4,engine=innodb,comment="%s"' % table_comment
        if self._engine == sqlite:
            sql = sql.replace('int AUTO_INCREMENT primary key', 'INTEGER PRIMARY KEY AUTOINCREMENT')
        self.query(sql, show_sql)
        # asyncio.get_event_loop()
        self.commit()
        return


    # 查找数据（单条）
    def find(self, table, conditions, or_cond, fields=('*',), order=None, show_sql=False, for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]
            fields = fieldList

        sql, sql_params = self.find_info(table, conditions, or_cond, fields, None, order, None, for_update)
        if sql is None:
            return

        sql += " limit 1"
        # self.db.commit()
        res = self.query(sql, show_sql, sql_params, return_dict=True)

        if can_return_directly(res) is True:
            return res

        if 0 == len(res):
            return None

        if self._engine == sqlite:
            result = dict(zip(fields, res[0]))
        else:
            result = res[0]

        result = {
            'table': table,
            'query': sql % tuple(sql_params) if sql_params is not None else sql,
            'result': result
        }
        return result

    # 查找数据
    def select(self, table, conditions, or_cond, fields=('*',), group=None, order=None, limit=None, show_sql=False,
               for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]
            fields = fieldList

        sql, sql_params = self.find_info(table, conditions, or_cond, fields, group, order, limit, for_update)
        if sql is None:
            return
        #
        res = self.query(sql, show_sql, sql_params, return_dict=True)

        if can_return_directly(res) is True:
            return res

        if 0 == len(res):
            return None

        result = []
        if self._engine == sqlite:
            for data in res:
                data = dict(zip(fields, data))
                result.append(data)
        else:
            result = res
        res = {
            'table': table,
            'query': sql % tuple(sql_params) if sql_params is not None else sql,
            'result': result
        }
        return res

    def insert(self, table, content, show_sql=False):
        params = content
        keys = str(tuple(params.keys()))
        keys = keys.replace("'", "")
        if 1 == len(params):
            keys = keys[:-2] + ")"
        sql_params = []
        sql = 'insert into %s%s values (' % (table, keys)
        for i in params.values():
            sql_params.append(i)
        for _ in params.values():
            sql += '%s,'
        sql = sql[:-1]
        sql += ')'
        #
        r = self.query(sql, show_sql, sql_params)
        return r

    def update(self, table, conditions, or_cond, params, show_sql=False):
        # dbg('开始执行')
        if params == {} or params is None:
            # dbg('没有params，结束执行')
            return
        sql = 'update %s set ' % table
        sql_params_header = []
        for param, value in params.items():
            sql = sql + " %s = " % param + '%s,'
            sql_params_header += [value]

        sql = sql[:-1] + ' where  '
        sql, sql_params = Base.bind_conditions(sql, conditions, or_cond)
        # dbg(sql)
        if sql_params is None:
            sql_params = []
        sql_params = sql_params_header + sql_params

        r = self.query(sql, show_sql, sql_params)
        # dbg('自动提交完毕')
        return r

    def delete(self, table, condition, or_cond, show_sql=False):
        sql = 'delete from %s where  ' % table
        sql, sql_params = Base.bind_conditions(sql, condition, or_cond)
        #  #
        r = self.query(sql, show_sql, sql_params)
        return r

    def query(self, sql, show_sql=False, sql_params=None, return_dict=False):
        # for i in danger_sig:
        #     if i in sql:
        #         return None
        dummy_sql = sql % tuple(sql_params) if sql_params is not None else sql
        if self._engine == sqlite:
            sql = sql.replace('binary', '')
            sql = sql.replace('%s', '?')
        self.executing_query = dummy_sql
        if sql == 'select 1':
            show_sql = False
        if show_sql is True:
            dbg('sql_id', id(self), dummy_sql)
        if self._engine != sqlite:
            try:
                self.db.ping(reconnect=True)
            except Exception as e:
                dbg(dummy_sql, 'db error', str(e), 'reconnecting...')
                self.connect()
        if self._engine == mysql and return_dict is True:
            cursor = self.db.cursor(DictCursor)
        else:
            cursor = self.db.cursor()

        try:
            if sql != 'select 1':
                self.update_last_execute_time()
                self.update_last_connect_time()
            if sql_params is not None:
                cursor.execute(sql, tuple(sql_params))
            else:
                cursor.execute(sql)
            results = cursor.fetchall()
            if self._engine == sqlite:
                self.commit()
        except Exception as e:
            dbg('<--------DBERROR-------->')
            dbg(dummy_sql)
            dbg('execute fail!', str(e))
            dbg('<--------DBERROR-------->')
            results = e
        return results

    def commit(self):
        self.db.commit()

    def update_last_connect_time(self):
        self.last_connect_time = int(time.time())
        # dbg(id(self),'执行',self.executing_query,'更新时间')
        return

    def update_last_execute_time(self):
        self.last_execute_time = int(time.time())
        return

    def get_last_connect_time(self):
        return self.last_connect_time

    async def truncate(self, table, show_sql=False):
        sql = 'TRUNCATE TABLE %s' % table
        self.query(sql, show_sql)
        return

    @property
    def tables(self):
        return self._tables
