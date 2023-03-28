from anduin import AMySQL, MySQL

# session = Data.add_new_sql()

cnf = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '033248hyzh',
    'database': 'eyewave_sec',
    'port': 3306,
    'engine': 'mysql'
}


# def find_data(r):
#     Data = r.get_free_client()
#     r = Data.find('user',[('username','=','youtube1')])
#     print(r)

async def async_find_data():
    r = AMySQL(cnf)
    Data = await r.get_free_client()
    r = await Data.find('user', [('username', '=', 'youtube1')])
    print(r)


def read_data():
    r = MySQL(cnf)
    session = r.get_free_client()
    r = session.find('data_example.distr_relation', [('id = 1 --', '=', 'youtube1')])
    # p = session._tables
    print(r)
    r = session.update('data_example.user',[('id','=',1)],params={'username':'xxxx'})
    print(r)
    r = session.find('data_example.user',[('id','=',1)])
    print(r)
    # session.query()
    # pprint(r)


if __name__ == '__main__':
    read_data()
    # Data = r.get_free_client()
    # r = Data.find('user', [('username', '=', 'youtube1')])
    # print(r)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(async_find_data())
