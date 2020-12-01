a lite mysql connector...
Name from the Lord of Ring

<=========how to use=========>

from anduin.river import Stream

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

    Stream.find(__TableName__,conditions=cond,fields=fields)

    return a python dict like { col1:value1,col2:value2...}

find datas

    Stream.select(__TableName__,conditions=cond,fields=fields)

    return a python list like [{ col1:value1,col2:value2...}]

update data:

    Stream.update(__TableName__,conditions=cond,params=params)

    return None

insert data:

    Stream.insert(__TableName__,params=params)

    return None

delete data:

    Stream.delete(__TableName__,conditions=cond)

    return None

using Stream.query() to execute sql directly like Stream.query('select * from table')

