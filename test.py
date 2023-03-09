

from anduin import MySQL

# session = Data.add_new_sql()

cnf =  {
        'host': '127.0.0.1',
        'user': 'root',
        'password': '033248hyzh',
        'database': 'eyewave_sec',
        'port': 3306,
        'engine':'mysql'
    }



def find_data(r):
    Data = r.get_free_client()
    r = Data.find('user',[('username','=','youtube1')])
    print(r)

# async def async_find_data():
#     r = AsyncMySQL(cnf)
#     Data = await r.get_free_client()
#     r = await Data.find('user',[('username','=','youtube1')])
#     print(r)

if __name__ == '__main__':
    r = MySQL(cnf)
    cur_tid = MySQL.get_cur_thread_id()
    c_pool = r.get_cur_client_pool_by_thread_id()
    print(len(c_pool))
    find_data(r)
    # Data = r.get_free_client()
    # r = Data.find('user', [('username', '=', 'youtube1')])
    # print(r)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(async_find_data())

