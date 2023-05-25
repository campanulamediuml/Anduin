# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import traceback
from typing import Iterable, Union, List, Any, Tuple, Dict, Optional

# from MySQLdb import Connection
# from MySQLdb.cursors import DictCursor

# import pymysql
# from pymysql import Connection
# from pymysql.cursors import DictCursor

from anduin.common import ENGINE_MYSQL, dbg, get_obj_name
from anduin.frames.client_base import ClientBase
from anduin.parser.sql_parser import Parser
# import MySQLdb
import pymysql
from pymysql.cursors import DictCursor as pymysql_dict_cursor
# from MySQLdb.cursors import DictCursor as mysqldb_dict_cursor
# import MySQLdb


class MySQLClient(ClientBase):
    def __init__(self, *args):
        super().__init__(*args)
        # print(self.db_engine)
        self.dict_cursor = None
        self.engine = None
        # if self.db_engine == 'pymysql':
        self.dict_cursor = pymysql_dict_cursor
        self.engine = pymysql
        # else:
        #     self.dict_cursor = mysqldb_dict_cursor
        #     self.engine = MySQLdb
        # print(self.engine.__name__)
        self.db = self.connect_db()
        self._tables = {}
        self._load_tables()

    def connect_db(self):
        '''
        连接数据库，不需要手动调用
        '''
        try:
            res = self.engine.connect(host=self._host, user=self._user, password=self._psw, database=self._dbname,
                                  charset=self._charset, port=self._port,
                                  connect_timeout=self.time_out)
            dbg('连接创建成功', get_obj_name(self))
            return res
        except Exception as e:
            dbg('连接创建失败', e, get_obj_name(self))
            return

    def load_an_table(self, tablename: str) -> Union[Dict, Any]:
        '''
        加载数据库表结构，返回一个
        {
            字段1:(字段1属性1,字段1属性2,字段1属性3....)，
            字段2:(....)
        }
        '''
        sql = 'show fields from ' + tablename + ''
        res = self.query(sql, show_sql=False)
        if isinstance(res, Iterable):
            self._tables[tablename] = dict(zip(list(map(lambda x: x[0], res)), res))
        return res

    def _load_tables(self, show_sql=False):
        '''
        内部方法，加载全部数据表，使self._tables获得当前数据表结构
        '''
        sql = 'show tables'
        res = self.query(sql, show_sql)
        tables = list(map(lambda x: x[0], res))
        for table in tables:
            table_name = table
            self.load_an_table(table_name)
            # self._tables[table_name] = r

    def commit(self):
        '''
        commit本次事务
        '''
        # print('commit')
        self.db.commit()

    def query(self, sql: str, show_sql=False, sql_params=None, return_dict=False):
        '''
        直接执行sql语句
        :param
            sql:数据库sql语句
            show_sql:是否展示本次语句
            sql_params:当sql为带参数语句的时候，是否使用参数
            return_dict:是否使用Dict类型
        '''
        if sql_params is not None:
            tmp = []
            for i in sql_params:
                tmp.append('"' + str(i) + '"')
            dummy_sql = sql % tuple(tmp)
        else:
            dummy_sql = sql
        if return_dict is True:
            cursor = self.db.cursor(self.dict_cursor)
        else:
            cursor = self.db.cursor()
        # print('get_cursor')
        if show_sql is True:
            dbg('sql_id', id(self), dummy_sql)

            # print('xxxx')
        if sql_params is not None:
            cursor.execute(sql, tuple(sql_params))
        else:
            cursor.execute(sql)
        self.update_last_execute_time()
        results = cursor.fetchall()
        return results

    def create(self, table: str, columns: List[Tuple], table_comment: str = '', show_sql=False):
        '''
        创建表
        :params
            tables 表名
            column 每个字段的属性，符合load_an_table属性获取的结果
            table_comment 表注释
            show_sql 是否展示sql语句
        '''
        sql = Parser.create_table_parser(table, columns, table_comment, sql_engine=ENGINE_MYSQL)
        self.query(sql, show_sql)
        # asyncio.get_event_loop()
        self.commit()
        return

    def find(self, table: str, conditions: List[Tuple], or_cond: Optional[List[Tuple]] = None,
             fields: Union[Tuple, List] = ('*',), order: Optional[List[str]] = None, show_sql=False, for_update=False,partition=''):
        """
        传入参数：
            table:表名称
                数据类型为字符串

            conditions:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为and

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]

            fields:查询的字段
                数据类型为列表，默认为select *

                举例：fields = ['id','user']，支持['count(id)']等写法

            or_cond:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为or，与condition通过or连接

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]

            order:排序条件
                数据类型为列表

                举例：order = ['key1','desc','key2','asc']

            for_update:是否对查询加锁
                默认False，当使用for_update参数的时候，强制对本次查询数据加锁

            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志
        """

        if table not in self._tables:
            r = self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        if fields[0] == '*' and len(fields) == 1:
            fieldList = list(self._tables[table].keys())
            fields = fieldList
        # else:
        #     for field_name in fields:
        #         if field_name not in self._tables[table].keys():
        #             dbg(field_name, 'not in ', table)
        #             return

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, None, order, None, for_update,
                                           table_fields=self._tables[table].keys(),partition=partition)
        if sql is None:
            return

        sql += " limit 1"
        # self.db.commit()
        res = self.query(sql, show_sql, sql_params, return_dict=True)
        if isinstance(res, Exception) is True:
            return res

        if 0 == len(res):
            return None

        result = res[0]
        return result

    # 查找数据
    def select(self, table: str, conditions: List[Tuple], or_cond: Optional[List] = None, fields=('*',), group=None,
               order=None, limit=None, show_sql=False,
               for_update=False,partition=''):
        """
        :params
            table:表名称
                数据类型为字符串
            conditions:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为and
                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]
            fields:查询的字段
                数据类型为列表，默认为select *
                举例：fields = ['id','user']，支持['count(id)']等写法
            or_cond:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为or，与condition通过or连接
                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]
            group:分组条件
                数据类型为列表
                举例：group = ['id','user']
            order:排序条件
                数据类型为列表
                举例：order = ['key1','desc','key2','asc']
            limit:查询条数
                数据类型为整数，当未提供limit的时候，返回全部数据（慎用！）
            for_update:是否对查询加锁
                默认False，当使用for_update参数的时候，强制对本次查询数据加锁
            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志
        """

        if table not in self._tables:
            r = self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        if fields[0] == '*' and len(fields) == 1:
            fieldList = list(self._tables[table].keys())
            fields = fieldList
        # else:
        #     for field_name in fields:
        #         if field_name not in self._tables[table].keys():
        #             dbg(field_name, 'not in ', table)
        #             return

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, group, order, limit, for_update,
                                           table_fields=self._tables[table].keys(),partition=partition)
        if sql is None:
            return
        #
        res = self.query(sql, show_sql, sql_params, return_dict=True)
        if isinstance(res, Exception) is True:
            return res

        if 0 == len(res):
            return None

        return res

    def insert(self, table, content, show_sql=False):
        '''
            传入参数：
            table:表名称
                数据类型为字符串

            content:插入字段
                数据类型为字典
                {'key1':'value1','key2':'value2'}
                支持使用列表内嵌套字典进行批量插入

           show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志
        '''
        if table not in self._tables:
            r = self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        sql, sql_params = Parser.insert_parser(table, content, table_fields=self._tables[table].keys())
        if sql is None:
            return
        r = self.query(sql, show_sql, sql_params)
        return r

    def update(self, table, conditions, or_cond=None, params=None, show_sql=False):
        """
        传入参数：
            table:表名称
                数据类型为字符串

            conditions:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为and

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]

            or_cond:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为or，与condition通过or连接

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]

            params:更新字段
                数据类型为字典

                {'key1':'value1','key2':'value2'}

            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志
        """

        # print(self._tables)
        if table not in self._tables:
            r = self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        # print(self._tables)
        # print(self._tables[table])
        # print(self._tables[table].keys())

        sql, sql_params = Parser.update_parser(table, conditions, or_cond, params, table_fields=self._tables[table].keys())
        r = self.query(sql, show_sql, sql_params)
        # dbg('自动提交完毕')
        return r

    def delete(self, table: str, conditions: List[Tuple], or_cond: Union[List[Tuple], None] = None,
               show_sql: bool = False):
        '''
        删除数据
        :params
            conditions: 通过and连接的条件
            [
                 ('id', '=', 1)
                 ('status', '!=', 1)
            ]
            or_cond: 通过or连接的条件
            [
                ('id', '=', 1)
                ('status', '!=', 1)
            ]
            show_sql: 是否展示本次sql
        '''

        sql = 'delete from %s where  ' % table
        sql, sql_params = Parser.bind_conditions(sql, conditions, or_cond, table_fields=self._tables[table].keys())
        #  #
        r = self.query(sql, show_sql, sql_params)
        return r

    def drop_table(self, tablename: str, show_sql=False):
        '''
        删除数据表
        '''
        sql = 'drop table if exists %s' % tablename
        r = self.query(sql, show_sql=show_sql)
        return r

    def show_database(self):
        return self._tables
