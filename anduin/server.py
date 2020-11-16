from .dbserver.data_manager import data_manager

class Data(object):
    Base = None

    @staticmethod
    def init(db_config):
        Data.Base = data_manager(db_config)

    @staticmethod
    def check_connections():
        sql_pool = Data.Base.sql_pool
        busy_list = []
        all_list = []
        free_list = []
        sql_executing = {}
        for sql_id in sql_pool:
            sql = sql_pool[sql_id]
            if sql.is_busy() == True:
                busy_list.append(sql_id)
                sql_executing[sql_id] = sql.executing_query

            if sql.is_busy() == False:
                free_list.append(sql_id)
            all_list.append(sql_id)

        result = {
            'busy_base': {
                'count': len(busy_list),
                'lists': busy_list,
                'executing': sql_executing,
            },
            'free_base': {
                'count': len(free_list),
                'lists': free_list
            },
            'all_base': {
                'count': len(all_list),
                'lists': all_list
            }
        }
        return result

    @staticmethod
    def add_new_sql():
        return Data.Base.new()

    @staticmethod
    def create(table, colums, show_sql=False):
        return Data.Base.create(table, colums, show_sql)

    @staticmethod
    def insert(table, params, is_commit=True, show_sql=False):
        data = Data.Base.insert(table, params, is_commit, show_sql)
        return data

    @staticmethod
    def find(table, conditions, or_cond=None,fields=('*',), order=None, show_sql=False):
        return Data.Base.find(table, conditions, or_cond,fields, order, show_sql)

    @staticmethod
    def select(table, conditions, or_cond=None,fields=('*',), group=None,order=None, limit=None,show_sql=False):
        return Data.Base.select(table, conditions,or_cond, fields,group, order, limit,show_sql)

    @staticmethod
    def update(table, conditions, or_cond=None,params=None, is_commit=True, show_sql=False):
        Data.Base.update(table, conditions, params, or_cond,is_commit, show_sql)
        return

    @staticmethod
    def delete(table, conditions, or_cond=None,is_commit=True, show_sql=False):
        data = Data.Base.delete(table, conditions, or_cond,is_commit, show_sql)
        return data

    @staticmethod
    def find_last(table, conditions, info, limit, fields="*", show_sql=False):
        return Data.Base.find_last(table, conditions, info, limit, fields, show_sql)

    @staticmethod
    def query(sql, show_sql=False):
        return Data.Base.query(sql, show_sql)




if __name__ == '__main__':
    res = Data.select('withdraw_record',[('user_id','in',tuple(range(1,10)))],group=['user_id'],fields=['user_id','sum(share)'],show_sql=False)
    for i in res:
        print(i)
        print(i['sum(share)'])