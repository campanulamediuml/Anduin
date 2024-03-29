import asyncio
import time

from anduin import AMySQL, MySQL
from anduin.common import func_time, ENGINE_MYSQL
from anduin.db.sql.async_mysql.db_client import AsyncMySQLClient

# session = Data.add_new_sql()

cnf1 = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '033248hyzh',
    'database': 'eyewave_sec',
    'port': 3306,
    'engine': 'pymysql'
}


cnf2 = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '033248hyzh',
    'database': 'eyewave_sec',
    'port': 3306,
    'engine': 'mysqldb'
}

# def find_data(r):
#     Data = r.get_free_client()
#     r = Data.find('user',[('username','=','youtube1')])
#     print(r)

async def async_find_data():
    r = AMySQL(cnf1)
    Data = await r.get_free_client()
    r = await Data.find('user', [('username', '=', 'youtube1')])
    print(r)

# r1 = MySQL(cnf1)
# r1.get_free_client().release_lock()
# r2 = MySQL(cnf2)
# r2.get_free_client().release_lock()



@func_time
def read_data_1():
    session = r1.get_free_client()
    # session.commit()
    # print('...')
    res = []
    for i in range(0,10000):
        t = session.find('take_rec_his',[('id','=',i)])
        res.append(t)
    session.release_lock()
    return res

@func_time
def read_data_2():
    session = r1.get_free_client()
    # session.commit()
    # print('...')
    res = []
    for i in range(0,10000):
        t = session.find('data_example.take_rec_his',[('id','=',i)])
        res.append(t)
    session.release_lock()

def insert_data():
    session = r2.get_free_client()
    insert_params = []
    for i in range(0, 20):
        r = {'user_id':1,'upper_id':2}
        insert_params.append(r)
    session.insert('distr_relation',insert_params,show_sql=True)
    session.insert('distr_relation', {'user_id':9999,'upper_id':99999} , show_sql=True)
    session.release_lock()

def delete_data():
    session = r2.get_free_client()
    r = session.find('distr_relation',[('user_id','in',tuple((1,2,3,4)))],fields=['count(id)'])
    print(r['count(id)'])
    # session.release_lock()


    # session.query()
    # pprint(r)
r1 = AMySQL(cnf1)
async def check_release():
    # while 1:
    # r1 = AMySQL(cnf1)
    # print(r1)
    # res = await r1.get_free_client()
    # print(id(res),hasattr(res,"__aexit__"))
    async with AsyncMySQLClient(r1.host, r1.user, r1.password, r1.port, r1.database, ENGINE_MYSQL,
                                     r1.charset, r1.get_time_out()) as session:
        res = await session.find('invite_relation',[('id','=',3)])
        print(res)
    return







if __name__ == '__main__':
    # check_release()
    loop = asyncio.get_event_loop()
    # loop.create_task(check_release())
    loop.run_until_complete(check_release())
    print('------')
    time.sleep(10)    # read_data_1()
    # read_data_2()
    # insert_data()
    # delete_data()
    # Data = r.get_free_client()
    # r = Data.find('user', [('username', '=', 'youtube1')])
    # print(r)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(async_find_data())
