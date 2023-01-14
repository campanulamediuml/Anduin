一个轻量化的sqlite/mysql数据序列化操作引擎
名称来自《指环王》

<=========使用指南=========>

配置

    自动配置：

        配置文件为项目根目录创建文件config/db_config.py

        文件内填写db_config为配置项目

            db_config ={

            'host': mysqlhost,#sql服务器地址，sqlite则不需要填写

            'user': username,#sql用户名

            'password': password,#sql密码

            'database': databasename,#数据库名称，如果是sqlite，则填写文件路径

            'port': port,#数据库端口

            'engine':'mysql'#采用引擎，可选择mysql或者sqlite

            }


    手动配置：

        from anduin import Data
        from anduin.common.tools import get_db_index

        根据db_config字典填写需要初始化的数据库配置，在项目全局内调用Data.init(db_config)，
        即可将数据库配置写入Data目录中，通过get_db_index(db_config)获得唯一字符串，在调用数据库操作方法时，以base_id参数传入即可


使用

    from anduin import Data

    创建表
        Data.create(table, colums, comment='', show_sql=False, base_id='default', show_manager_id=False) -> None

        返回值为None

        传入参数：
            table:表名称
                数据类型为字符串

            columns:表字段
                数据类型为包含列表的列表，每个元素格式如下：

                ["user_id", "int(11)", "default '0'", "comment ''",]

                [字段名, 数据类型, 默认值, 备注,]

                主键为["id", "int", "AUTO_INCREMENT", "primary key", "comment ''",]

            comment:表注释
                数据类型为字符串

            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志

    查询表
        查询多条数据：Data.select(table, conditions, fields=('*',), or_cond=None, group=None, order=None, limit=None, show_sql=False,base_id='default', show_manager_id=False, from_cache=False, for_update=False) -> list

        返回值为列表
            [{字段：值，字段：值...}]

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

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志

        查询单条数据：Data.find(table, conditions, fields=('*',), or_cond=None, order=None, show_sql=False, base_id='default',
             show_manager_id=False, from_cache=False, for_update=False)

        返回值为字典
            {字段：值，字段：值...}

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

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志

    更新表
        Data.update(table, conditions, params=None, or_cond=None, show_sql=False, base_id='default', show_manager_id=False)

        无需返回值

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

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志


    删除表
        Data.delete(table, conditions, or_cond=None, show_sql=False, base_id='default', show_manager_id=False)

        无需返回值

        传入参数：
            table:表名称
                数据类型为字符串

            conditions:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为and

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]

            or_cond:查询条件
                数据类型为列表内包含可迭代对象（元组，列表，集合），逻辑为or，与condition通过or连接

                举例：[('id','=',1),('user_id','<=',2),('status','!=',0)]


            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志


    直接执行sql语句
        注意：本方法将会无条件提交sql，所以无法保证数据注入安全，如果采用本方法，请务必保证sql注入安全

        Data.query(sql, show_sql=False, base_id='default', show_manager_id=False, return_dict=False)

        传入参数:
            sql:要执行的sql字符串
                数据类型为字符串

            show_sql:是否展示sql内容
                默认False，开启后将会显示此次执行的sql内容并写入日志

            base_id:通过get_db_index(db_config)获取的数据库index
                数据类型字符串，默认为config/db_config内的配置

            show_manager_id:显示执行本次sql任务的数据库链接id
                默认False，开启后将会显示此次执行的链接id并写入日志

            return_dict:是否以字典形式返回数据
                默认False，select返回值将会以元组形式返回


