from anduin import AMySQL, MySQL
from anduin.common import func_time

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

r1 = MySQL(cnf1)
r1.get_free_client().release_lock()
r2 = MySQL(cnf2)
r2.get_free_client().release_lock()



@func_time
def read_data_1():
    print('r_1')
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
    print('r_2')
    session = r2.get_free_client()
    res = []
    for i in range(0, 10000):
        t = session.find('take_rec_his', [('id', '=', i)])
        res.append(t)
    session.release_lock()
    return res

    # session.query()
    # pprint(r)


if __name__ == '__main__':
    read_data_1()
    read_data_2()
    # Data = r.get_free_client()
    # r = Data.find('user', [('username', '=', 'youtube1')])
    # print(r)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(async_find_data())
