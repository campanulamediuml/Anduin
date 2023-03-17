# !/usr/bin/env python
# -*-coding:utf-8 -*-
# Author     ：Campanula 梦芸 何
import abc
import time


class ClientBase(abc.ABC):
    def __init__(self, host='127.0.0.1', user='', psw='', port=0, dbname='', engine='mysql', charset='utf8mb4'):
        self.id = id(self)
        self._host = host
        self._user = user
        self._psw = psw
        self._dbname = dbname
        self._port = port
        self._engine = engine
        self._charset = charset
        self.db_engine = engine
        self.is_lock = False
        self.last_connect_time = int(time.time())
        self.db = None
        self._tables = None

    @abc.abstractmethod
    def connect_db(self):
        pass

    def lock(self):
        self.is_lock = True

    def release_lock(self):
        self.commit()
        self.is_lock = False

    def commit(self):
        self.db.commit()

    def update_last_execute_time(self):
        self.last_connect_time = int(time.time())
