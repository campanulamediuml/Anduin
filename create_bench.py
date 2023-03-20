# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
from anduin import Data


def create_bench():
    free_sql = Data.get_free_client()
    table_name = "mysql_bench.service_data_raw"
    free_sql.query("drop table %s" % table_name)
    column = [
        ("id", "int", "AUTO_INCREMENT", "primary key", "comment ''",),
        #
        ("service_name", "VARCHAR(1024)", "default ''", "comment '服务名称'",),
        #
        ("data_content", "TEXT", " ", "comment '数据内容'",),
        #
        ("u_time", "BIGINT", "default '0'", "comment '入库时间戳'",),
        #
    ]
    free_sql.create(table_name, column, table_comment='')

    table_name = "mysql_bench.service_data_time"
    free_sql.query("drop table %s" % table_name)
    column = [
        ("id", "int", "AUTO_INCREMENT", "primary key", "comment ''",),
        #
        ("c_time", "BIGINT", "default '0'", "comment '入库时间戳'",),
        ("service_name", "VARCHAR(1024)", "default ''", "comment '服务名称'",),
        #
    ]
    free_sql.create(table_name, column, table_comment='')


if __name__ == '__main__':
    create_bench()
