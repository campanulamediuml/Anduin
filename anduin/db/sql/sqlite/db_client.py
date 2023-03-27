# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import sqlite3
from typing import Iterable, Dict, List, Tuple, Any, Union

from anduin.common import ENGINE_SQLITE, dbg, get_obj_name
from anduin.frames.client_base import ClientBase
from anduin.parser.sql_parser import Parser


class SQLiteClient(ClientBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.db = self.connect_db()
        self._tables: Dict[str, Iterable] = {}
        self.load_tables()

    def connect_db(self):
        try:
            res = sqlite3.connect(self._dbname)
            dbg('连接创建成功', get_obj_name(self))
            return res
        except Exception as e:
            dbg('连接创建失败', e, get_obj_name(self))
            return

    def load_an_table(self, tablename):
        sql = 'PRAGMA table_info(%s)' % tablename
        res = self.query(sql, show_sql=False)
        if res is None:
            return
        if isinstance(res, Iterable):
            column_list = list(map(lambda x: x[1], res))
            self._tables[tablename] = column_list
            return column_list

    def load_tables(self):
        # table_keys = self.load_an_table('sqlite_master')
        table_list = self.select('sqlite_master', [], fields=['name', 'tbl_name'])
        for table in table_list:
            self._tables[table['tbl_name']] = self.load_an_table(table['tbl_name'])
        return self._tables

    def query(self, sql, show_sql=False, sql_params=None):
        sql = sql.replace('binary', '')
        if sql_params is not None:
            tmp = []
            for i in sql_params:
                tmp.append('"' + str(i) + '"')
            dummy_sql = sql % tuple(tmp)
        else:
            dummy_sql = sql
        sql = sql.replace('%s', '?')
        if show_sql is True:
            dbg('sql_id', id(self), dummy_sql)
        cursor = self.db.cursor()

        try:
            if sql_params is not None:
                cursor.execute(sql, tuple(sql_params))
            else:
                cursor.execute(sql)
            results = cursor.fetchall()
        except Exception as e:
            dbg('<--------DBERROR-------->')
            dbg(dummy_sql)
            dbg('execute fail!', str(e))
            dbg('<--------DBERROR-------->')
            results = e
        return results

    def create(self, table, columns, table_comment='', show_sql=False):
        sql = Parser.create_table_parser(table, columns, table_comment, sql_engine=ENGINE_SQLITE)
        self.query(sql, show_sql)
        self.commit()
        return

    # 查找数据（单条）
    def find(self, table, conditions, or_cond=None, fields=('*',), order=None, show_sql=False, for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        if fields[0] == '*' and len(fields) == 1:
            fieldList = self._tables[table]
            fields = fieldList

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, None, order, None, for_update,
                                           self._tables[table])
        if sql is None:
            return

        sql += " limit 1"
        res = self.query(sql, show_sql, sql_params)
        if isinstance(res, Exception) is True:
            return res
        if 0 == len(res):
            return None
        result = dict(zip(fields, res[0]))
        return result

    # 查找数据
    def select(self, table, conditions, or_cond=None, fields=('*',), group=None, order=None, limit=None, show_sql=False,
               for_update=False):
        if table not in self._tables:
            self.load_an_table(table)

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, group, order, limit, for_update,
                                           self._tables[table])
        if sql is None:
            return
        #
        res = self.query(sql, show_sql, sql_params)
        if isinstance(res, Exception) is True:
            return res
        if 0 == len(res):
            return None
        result = []
        for data in res:
            data = dict(zip(fields, data))
            result.append(data)

        return result

    def insert(self, table, content, show_sql=False):
        sql, sql_params = Parser.insert_parser(table, content, table_fields=self._tables[table])
        if sql is None:
            return
        r = self.query(sql, show_sql, sql_params)
        return r

    def update(self, table, conditions, or_cond=None, params=None, show_sql=False):
        # dbg('开始执行')
        sql, sql_params = Parser.update_parser(table, conditions, or_cond, params, table_fields=self._tables[table])
        r = self.query(sql, show_sql, sql_params)
        # dbg('自动提交完毕')
        return r

    def delete(self, table: str, condition: List[Tuple[Union[str, Any]]], or_cond: Union[List[Tuple[
        Union[str, Any]]], None] = None, show_sql=False):
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
        sql, sql_params = Parser.bind_conditions(sql, condition, or_cond, table_fields=self._tables[table])
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
