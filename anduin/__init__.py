"""Anduin: A light python mysql / sqlite / redis connector.

Copyright (c) 2020-2024 Campanula<campanulamediuml@gmail.com> & Ithil Dawnstar & Isil Tindomiel

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE."""
info = """Anduin: A light python mysql / sqlite / redis connector.

Copyright (c) 2020-2024 Campanula<campanulamediuml@gmail.com> & Ithil Dawnstar & Isil Tindomiel

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE."""
# <=========>

from .common.tools import get_filename, clean_old_log, dbg, get_obj_name
from .db.no_sql.async_redis.redis_client_manager import AsyncRedisManager
from .db.no_sql.redis.redis_client_manager import RedisManager
from .db.sql.async_mysql.db_client_manager import AsyncMySQLManager
from .db.sql.mysql.db_client_manager import MySQLManager
from .db.sql.sqlite.db_client_manager import SQLiteManager

SQLite = SQLiteManager

SMySQL = MySQL = MySQLManager
SRedis = Redis = RedisManager
# 同步

AMySQL = AsyncMySQLManager
ARedis = AsyncRedisManager


# 异步

def auto_init():
    dbg('自动初始化同步mysql')
    try:
        from config.db_config import db_config
        mysql_pool = MySQL(db_config)
        dbg('自动初始化同步mysql成功')
        return mysql_pool
    except Exception as e:
        dbg(e)


Data = auto_init()
if Data is None:
    dbg('自动初始化同步mysql失败，如果不需要自动初始化，请忽略本通知')

__version__ = "8.4.15"
