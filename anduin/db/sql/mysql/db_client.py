# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from typing import Iterable, Union, List, Any

import pymysql
from pymysql import Connection
from pymysql.cursors import DictCursor

from anduin.common import ENGINE_MYSQL,dbg,get_obj_name
from anduin.db.sql.parser.sql_parser import Parser
from anduin.frames.client_base import ClientBase


class MySQLClient(ClientBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.db = self.connect_db()
        self._tables = {}
        self._load_tables()

    def connect_db(self)->Union[Connection,None]:
        try:
            res = pymysql.connect(host=self._host, user=self._user, password=self._psw, database=self._dbname,
                                  charset=self._charset, port=self._port,
                                  connect_timeout=60)
            dbg('连接创建成功', get_obj_name(self))
            return res
        except Exception as e:
            dbg('连接创建失败',e,get_obj_name(self))
            return

    def load_an_table(self, tablename)->Union[List,Any]:
        sql = 'show fields from ' + tablename
        res = self.query(sql, show_sql=False)
        if isinstance(res,Iterable):
            self._tables[tablename] = list(map(lambda x: x[0],res))
        return res

    def _load_tables(self, show_sql=False):
        sql = 'show tables'
        res = self.query(sql, show_sql)
        tables = list(map(lambda x: x[0], res))
        for table in tables:
            table_name = self._dbname + '.' + table
            r = self.load_an_table(table_name)
            # self._tables[table_name] = r


    def commit(self):
        self.db.commit()

    def query(self, sql: str, show_sql=None, sql_params=None, return_dict=False):
        dummy_sql = sql % tuple(sql_params) if sql_params is not None else sql
        if return_dict is True:
            cursor = self.db.cursor(DictCursor)
        else:
            cursor = self.db.cursor()
        if show_sql is True:
            dbg('sql_id', id(self), dummy_sql)
        try:
            if sql_params is not None:
                cursor.execute(sql , tuple(sql_params))
            else:
                cursor.execute(sql)
            self.update_last_execute_time()
            results = cursor.fetchall()
        except Exception as e:
            dbg('<--------DBERROR-------->')
            dbg(dummy_sql)
            dbg('execute fail!', str(e))
            dbg('<--------DBERROR-------->')
            results = e
        return results

    def create(self, table, columns, table_comment='', show_sql=False):
        sql = Parser.create_table_parser(table, columns, table_comment, sql_engine=ENGINE_MYSQL)
        self.query(sql, show_sql)
        # asyncio.get_event_loop()
        self.commit()
        return

    def find(self, table, conditions, or_cond=None, fields=('*',), order=None, show_sql=False, for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]
            fields = fieldList

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, None, order, None, for_update)
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
    def select(self, table, conditions, or_cond=None, fields=('*',), group=None, order=None, limit=None, show_sql=False,
               for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, group, order, limit, for_update)
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
        sql, sql_params = Parser.insert_parser(table, content)
        r = self.query(sql, show_sql, sql_params)
        return r

    def update(self, table, conditions, or_cond=None, params=None, show_sql=False):
        # dbg('开始执行')
        sql, sql_params = Parser.update_parser(table, conditions, or_cond, params)
        r = self.query(sql, show_sql, sql_params)
        # dbg('自动提交完毕')
        return r

    def delete(self, table, condition, or_cond=None, show_sql=False):
        sql = 'delete from %s where  ' % table
        sql, sql_params = Parser.bind_conditions(sql, condition, or_cond)
        #  #
        r = self.query(sql, show_sql, sql_params)
        return r
