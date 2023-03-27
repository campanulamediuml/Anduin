# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import asyncio
from typing import Iterable, List, Union, Tuple

import aiomysql
from aiomysql import DictCursor

from anduin.common import ENGINE_MYSQL
from anduin.common import dbg, get_obj_name
from anduin.frames.client_base import ClientBase
from anduin.parser.sql_parser import Parser


# from pymysql.cursors import DictCursor


class AsyncMySQLClient(ClientBase):
    def __init__(self, *args):
        super().__init__(*args)
        self.db = None
        self._tables = {}

    async def connect_db(self):
        try:
            res = await aiomysql.connect(host=self._host, user=self._user, password=self._psw, db=self._dbname,
                                         charset=self._charset, port=self._port, connect_timeout=self.time_out)
            # dbg('连接成功',type(res))
            dbg('连接创建成功', get_obj_name(self))
            self.db = res
            return res
        except Exception as e:
            dbg('连接创建失败', e, get_obj_name(self))
            return

    async def load_an_table(self, tablename):
        sql = 'show fields from ' + tablename + ''
        res = await self.query(sql, show_sql=False)
        if isinstance(res, Iterable):
            self._tables[tablename] = dict(zip(list(map(lambda x: x[0], res)), res))
        return res

    async def _load_tables(self, show_sql=False):
        sql = 'show tables'
        res = await self.query(sql, show_sql)
        tables = list(map(lambda x: x[0], res))
        for table in tables:
            table_name = table
            await self.load_an_table(table_name)

    async def release_lock(self):
        await self.commit()
        self.is_lock = False

    async def commit(self):
        await self.db.commit()

    async def query(self, sql: str, show_sql=None, sql_params=None, return_dict=False):
        if sql_params is not None:
            tmp = []
            for i in sql_params:
                tmp.append('"' + str(i) + '"')
            dummy_sql = sql % tuple(tmp)
        else:
            dummy_sql = sql

        if return_dict is True:
            cursor = await self.db.cursor(DictCursor)
        else:
            cursor = await self.db.cursor()
        if show_sql is True:
            dbg('sql_id', id(self), dummy_sql)
        try:
            if sql_params is not None:
                await cursor.execute(sql, tuple(sql_params))
            else:
                await cursor.execute(sql)
            self.update_last_execute_time()
            results = await cursor.fetchall()
        except Exception as e:
            dbg('<--------DBERROR-------->')
            dbg(dummy_sql)
            dbg('execute fail!', str(e))
            dbg('<--------DBERROR-------->')
            results = e
        return results

    async def create(self, table, columns, table_comment='', show_sql=False):
        sql = Parser.create_table_parser(table, columns, table_comment, sql_engine=ENGINE_MYSQL)
        await self.query(sql, show_sql)
        # asyncio.get_event_loop()
        await self.commit()
        return

    async def find(self, table, conditions, or_cond=None, fields=('*',), order=None, show_sql=False, for_update=False):
        if table not in self._tables:
            r = await self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        if fields[0] == '*' and len(fields) == 1:
            fieldList = list(self._tables[table].keys())
            fields = fieldList
        else:
            for field_name in fields:
                if field_name not in self._tables[table].keys():
                    return

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, None, order, None, for_update,
                                           table_fields=self._tables[table].keys())
        if sql is None:
            return

        sql += " limit 1"
        # self.db.commit()
        res = await self.query(sql, show_sql, sql_params, return_dict=True)
        if isinstance(res, Exception) is True:
            return res

        if 0 == len(res):
            return None

        result = res[0]
        return result

    # 查找数据
    async def select(self, table, conditions, or_cond=None, fields=('*',), group=None, order=None, limit=None,
                     show_sql=False,
                     for_update=False):
        if table not in self._tables:
            r = await self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        if fields[0] == '*' and len(fields) == 1:
            fieldList = list(self._tables[table].keys())
            fields = fieldList
        else:
            for field_name in fields:
                if field_name not in self._tables[table].keys():
                    return

        sql, sql_params = Parser.find_info(table, conditions, or_cond, fields, group, order, limit, for_update,
                                           self._tables[table].keys())
        if sql is None:
            return
        #
        res = await self.query(sql, show_sql, sql_params, return_dict=True)
        if isinstance(res, Exception) is True:
            return res

        if 0 == len(res):
            return None

        return res

    async def insert(self, table, content, show_sql=False):
        if table not in self._tables:
            r = await self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r
        sql, sql_params = Parser.insert_parser(table, content, self._tables[table].keys())
        if sql is None:
            return
        r = await self.query(sql, show_sql, sql_params)
        return r

    async def update(self, table, conditions, or_cond=None, params=None, show_sql=False):
        # dbg('开始执行')
        if table not in self._tables:
            r = await self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        sql, sql_params = Parser.update_parser(table, conditions, or_cond, params,
                                               table_fields=self._tables[table].keys())
        r = await self.query(sql, show_sql, sql_params)
        # dbg('自动提交完毕')
        return r

    async def delete(self, table: str, conditions: List[Tuple], or_cond: Union[List[Tuple], None] = None,
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
        if table not in self._tables:
            r = await self.load_an_table(table)
            if isinstance(r, Exception):
                dbg('ERROR', r)
                return r

        sql = 'delete from %s where  ' % table
        sql, sql_params = Parser.bind_conditions(sql, conditions, or_cond, table_fields=self._tables[table].keys())
        if sql is None:
            return
        #  #
        r = await self.query(sql, show_sql, sql_params)
        return r

    def show_database(self):
        return self._tables

    async def drop_table(self, tablename: str, show_sql=False):
        '''
        删除数据表
        '''

        sql = 'drop table if exists %s' % tablename
        r = await self.query(sql, show_sql=show_sql)
        return r


if __name__ == '__main__':
    async def get_data():
        return 'xxx'


    r = asyncio.run(get_data())
    print(r)
