import time
import pymysql
import sqlite3

mysql = 'mysql'
sqlite = 'sqlite'

ENGINE_DICT = {
    'mysql':pymysql,
    'sqlite':sqlite3
}

class Base(object):

    def __init__(self, host, user, psw, dbname,engine):
        self.id = id(self)
        self._host = host
        self._user = user
        self._psw = psw
        self._dbname = dbname
        self._engine = engine
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
        # print(self.id)

    def connect_db(self):
        try:
            self.db = self.connect()
            if self._engine != sqlite:
                self._load_tables()
            # print('数据库模块连接成功')
            print('connect success!')
            # IntervalTask(30, self.keep_connect)
        except Exception as e:
            # print(e)
            print('connect fail', str(e))
            # print('数据库模块连接成功',str(e))
            pass

    def connect(self):
        if self._engine == mysql:
            self.db = self.db_engine.connect(self._host, self._user, self._psw, self._dbname, charset='utf8')
            return self.db
        if self._engine == sqlite:
            self.db = self.db_engine.connect(self._dbname)
            return self.db


    def become_busy(self):
        self._is_busy = 1
        return

    def become_free(self):
        self._is_busy = 0
        return

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
            sql = 'PRAGMA table_info(%s)'%table
            res = self.query(sql, show_sql=False)
            if res is None:
                return
            column_list = list(map(lambda x: x[1], res))
            self._tables[table] = column_list
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
        tables = tuple(map(lambda x: x[0], res))
        # print(tables)
        for table in tables:
            self.load_an_table(table)

    def _load_all_fileds(self):
        pass

    def create(self, table, colums, table_comment='',show_sql=False):
        sql = 'create table %s(' % table

        tail = ''

        for item in colums:
            col = ''
            if self._engine == sqlite:
                item= item[:-1]
            for i in item:
                col += str(i)
                col += ' '
            col += ','
            tail += col

        tail = tail[:-1] + ')'
        sql += tail
        # sql += 'charset=utf8mb4'
        if self._engine == mysql:
            sql += 'charset=utf8mb4,engine=innodb,comment="%s"'%(table_comment)
        if self._engine == sqlite:
            sql = sql.replace('int AUTO_INCREMENT primary key','INTEGER PRIMARY KEY AUTOINCREMENT')
        self.query(sql, show_sql)
        self.db.commit()
        return

    def find_info(self, table, conditions, or_cond, fields=None, group=None, order=None, limit=None):
        if len(fields) == 1 and fields[0] == '*':
            if table in self._tables:
                fields = self._tables[table]
            else:
                fields = self.load_an_table(table)
        if fields is None:
            return
        sql = 'select %s from %s where  ' % (','.join(fields), table)
        # if conditions == []:
        sql = Base.bind_conditions(sql, conditions, or_cond)

        if group is not None:
            sql += 'group by '
            for i in group:
                sql += '%s ,' % i
            sql = sql[:-1]

        if order is not None and order[0] != 'id':
            sql += 'order by %s %s , id %s ' % (order[0], order[1], order[1])

        if limit is not None:
            sql += 'limit '
            for i in limit:
                sql += '%s, ' % i
            sql = sql[:-2]
        return sql

    @staticmethod
    def bind_conditions(sql, conditions, or_cond):
        # print(sql)
        if conditions is None:
            conditions = []
        if or_cond is None:
            or_cond = []
        if len(or_cond) + len(conditions) == 0:
            return sql[:-7]

        if len(conditions) > 0:
            for unit in conditions:
                value = unit[2]
                if type("") == type(value):
                    value = "'%s'" % value
                sql = sql + "binary %s %s %s " % (unit[0], unit[1], value) + "  and "
            sql = sql[:-4]
            if len(or_cond) > 0:
                sql += ' or '

        if len(or_cond) > 0:
            for unit in or_cond:
                value = unit[2]
                if type("") == type(value):
                    value = "'%s'" % value
                sql = sql + "binary %s %s %s " % (unit[0], unit[1], value) + ' or '
            sql = sql[:-3]

        return sql

    # 查找数据（单条）
    def find(self, table, conditions, or_cond, fields=('*',), order=None, show_sql=False):
        sql = self.find_info(table, conditions, or_cond, fields, None, order)
        if sql is None:
            return

        sql += " limit 1"
        # 
        res = self.query(sql, show_sql)
        if res is None:
            return None

        if 0 == len(res):
            return None

        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]

        else:
            fieldList = fields

        # self.db.commit()
        return dict(zip(fieldList, res[0]))

    # 查找数据
    def select(self, table, conditions, or_cond, fields=('*',), group=None, order=None, limit=None, show_sql=False):
        sql = self.find_info(table, conditions, or_cond, fields, group, order, limit)
        if sql is None:
            return
        # 
        res = self.query(sql, show_sql)
        if res is None:
            return None

        if 0 == len(res):
            return None

        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]
        else:
            fieldList = fields

        result = []
        for data in res:
            data = dict(zip(fieldList, data))
            result.append(data)

        # self.db.commit()
        return result

    def insert(self, table, content, isCommit=True, show_sql=False):
        params = content
        keys = str(tuple(params.keys()))
        keys = keys.replace("'", "")
        values = str(tuple(params.values()))
        if 1 == len(params):
            keys = keys[:-2] + ")"
            values = values[:-2] + ")"

        sql = 'insert into %s%s values %s ;' % (table, keys, values)
        #
        self.query(sql, show_sql)
        if isCommit is True:
            self.db.commit()
        return

    def update(self, table, conditions, or_cond, params, isCommit=True, show_sql=False):
        if params == {} or params is None:
            return
        sql = 'update %s set ' % table
        for param, value in params.items():
            if type("") == type(value):
                value = "'%s'" % value
            sql = sql + " %s = %s," % (param, value)

        sql = sql[:-1] + ' where  '
        sql = Base.bind_conditions(sql, conditions, or_cond)

        self.query(sql, show_sql)
        if isCommit is True:
            self.db.commit()
        return

    def delete(self, table, condition, or_cond, is_commit, show_sql=False):
        sql = 'delete from %s where  ' % table
        sql = Base.bind_conditions(sql, condition, or_cond)
        #  #
        self.query(sql, show_sql)
        if is_commit is True:
            self.db.commit()
        return

    def query(self, sql, show_sql=False):
        if self._engine == sqlite:
            sql = sql.replace('binary','')
        self.executing_query = sql
        if sql == 'select 1':
            show_sql = False
        if show_sql is True:
            print(sql)
        if self._engine != sqlite:
            try:
                self.db.ping(reconnect=True)
            except Exception as e:
                print(sql, 'db error', str(e), 'reconnecting...')
                self.connect()
        cursor = self.db.cursor()
        try:
            # res = time.time()
            cursor.execute(sql)
            results = cursor.fetchall()
            # print(sql, '执行时间', time.time() - res)
            self.update_last_execute_time()
            # dbg_db(sql,'数据库查询成功')
        except Exception as e:
            print('<--------DBERROR-------->')
            print(sql)
            print('execute fail!', str(e))
            print('<--------DBERROR-------->')
            results = None

        self.update_last_connect_time()
        self.executing_query = ''
        # print(results)
        # print(sql,'执行时间',time.time()-res)
        return results
        # results = cursor.fetchall()

    def update_last_connect_time(self):
        self.last_connect_time = int(time.time())
        return

    def update_last_execute_time(self):
        self.last_execute_time = int(time.time())
        return

    def get_last_connect_time(self):
        return self.last_connect_time

    def query_one(self, sql):
        if self._engine != sqlite:
            try:
                self.db.ping(reconnect=True)
            except Exception as e:
                print(sql, 'db error', str(e), 'reconnecting...')
                self.connect()
        # cur.execute(sql)
        # db.commit()
        cursor = self.db.cursor()
        cursor.execute(sql)
        results = cursor.fetchone()
        return results

    def truncate(self, table, show_sql=False):
        sql = 'TRUNCATE TABLE %s' % table
        self.query(sql, show_sql)
        self.db.commit()
        return

    @property
    def tables(self):
        return self._tables
