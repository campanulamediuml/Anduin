a lite mysql & sqlite3 connect engine...
Name from the Lord of Ring

<=========how to use=========>

from anduin.river import Ring

configuration setting:
    db_config = {'host': mysql host,
        'user': username,

        'password': password,

        'database': database name,

        'port': port,

        'engine':'mysql'}

then use Ring.init(db_config) to init the connection

now enjoy it!!

or you can put a config file in config/db_config.py , write the dict in it


cond = [
    (col1,'=',val1),
    (col2,'=',val2),
    ....]

fields = [
    col1,
    col2
    ...]

params = {
    key1:val1,
    key2:val2,
    ...}

find one line of data:

    Ring.find(__TableName__,conditions=cond,fields=fields)

    return a python dict like { col1:value1,col2:value2...}

find datas

    Ring.select(__TableName__,conditions=cond,fields=fields)

    return a python list like [{ col1:value1,col2:value2...}]

update data:

    Ring.update(__TableName__,conditions=cond,params=params)

    return None

insert data:

    Ring.insert(__TableName__,params=params)

    return None

delete data:

    Ring.delete(__TableName__,conditions=cond)

    return None

using Ring.query() to execute sql directly like Ring.query('select * from table')

all of these executions will have auto commit

or you can do like

    from anduin.river import Ring

    db_manager = Ring.Base_pool['default']
    sql = db_manager.find_free_sql()

    sql.update('user',cond,or_cond=None,params=params )
    sql.insert('user',params=params )
    sql.delete('user',cond,or_cond=None)

    sql.commit()

to choose a certain sql connection to execute

