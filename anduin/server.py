from pprint import pprint

from .construct_file import frame_constructor
from .dbserver.data_manager import data_manager
db_config = None


def get_db_index(db_config):
    return db_config['user'] + '@' + db_config['host'] + ':' + db_config['database']

class Data(object):
    Base_pool = {}
    try:
        exec('from config import db_config')
        db_config = getattr(db_config, "db_config")
        # print(db_config)
        if isinstance(db_config,dict):
            Base = data_manager(db_config)
            Base_pool['default'] = Base
            db_index = get_db_index(db_config)
            Base_pool[db_index] = Base
        print('Auto init success!')
    except:
        print('Did not find a db config file, need run Data.init(db_config) manually...')
        # Base = None


    @staticmethod
    def init(db_config):
        Base = data_manager(db_config)
        if len(Data.Base_pool.keys()) == 0:
            Data.Base_pool['default'] = Base
        db_index = get_db_index(db_config)
        if db_index not in Data.Base_pool:
            Data.Base_pool[db_index] = Base

    @staticmethod
    def check_connections(base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')
        if base_id not in Data.Base_pool:
            return
        sql_pool = Data.Base_pool[base_id].sql_pool
        busy_list = []
        all_list = []
        free_list = []
        sql_executing = {}
        for sql_id in sql_pool:
            sql = sql_pool[sql_id]
            if sql.is_busy() is True:
                busy_list.append(sql_id)
                sql_executing[sql_id] = sql.executing_query

            if sql.is_busy() is False:
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
    def add_new_sql(base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        return Data.Base_pool[base_id].new() if base_id in Data.Base_pool else None

    @staticmethod
    def create(table, colums, comment='',show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        return Data.Base_pool[base_id].create(table, colums, comment,show_sql) if base_id in Data.Base_pool else None

    @staticmethod
    def insert(table, params, is_commit=True, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        data = Data.Base_pool[base_id].insert(table, params, is_commit, show_sql) if base_id in Data.Base_pool else None
        return data

    @staticmethod
    def find(table, conditions, or_cond=None, fields=('*',), order=None, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        return Data.Base_pool[base_id].find(table, conditions, or_cond, fields, order, show_sql) if base_id in Data.Base_pool else None

    @staticmethod
    def select(table, conditions, or_cond=None, fields=('*',), group=None, order=None, limit=None, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        return Data.Base_pool[base_id].select(table, conditions, or_cond, fields, group, order, limit, show_sql) if base_id in Data.Base_pool else None

    @staticmethod
    def update(table, conditions, or_cond=None, params=None, is_commit=True, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        Data.Base_pool[base_id].update(table, conditions, params, or_cond, is_commit, show_sql) if base_id in Data.Base_pool else None
        return

    @staticmethod
    def delete(table, conditions, or_cond=None, is_commit=True, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        data = Data.Base_pool[base_id].delete(table, conditions, or_cond, is_commit, show_sql) if base_id in Data.Base_pool else None
        return data

    @staticmethod
    def find_last(table, conditions, info, limit, fields="*", show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')

        return Data.Base_pool[base_id].find_last(table, conditions, info, limit, fields, show_sql) if base_id in Data.Base_pool else None

    @staticmethod
    def truncate(table, show_sql=False, base_id='default', show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过', base_id, '执行')
        return Data.Base_pool[base_id].truncate(table, show_sql) if base_id in Data.Base_pool else None


    @staticmethod
    def query(sql, show_sql=False,base_id='default',show_manager_id=False):
        if show_manager_id is True:
            print('本次任务通过',base_id,'执行')
        if base_id not in Data.Base_pool:
            return
        return Data.Base_pool[base_id].query(sql, show_sql) if base_id in Data.Base_pool else None

    @staticmethod
    def map_all_db(base_id='default',show_manager_id=False,file_path='',file_name='fb_frame.py'):
        if show_manager_id is True:
            print('本次任务通过', base_id, '执行')
        if base_id not in Data.Base_pool:
            print('base id not exist.. construct fail')
            return
        base_manager = Data.Base_pool[base_id]
        db_name = base_manager.t_data['database']
        all_table = Data.query('show tables',base_id=base_id)
        table_index = {}
        for i in all_table:
            table_name = i[0]
            table_info = Data.find('information_schema.tables',[('table_schema','=',db_name),('table_name','=',table_name)],fields=('table_name','table_comment'))
            res = Data.query('show full columns from '+str(table_name),base_id=base_id)
            table_index[table_name] = list(res)
            table_index[table_name].append(table_info)
        pprint(table_index)

        constructor = frame_constructor(db_name,table_index,file_path,file_name)
        constructor.dump()
        return table_index









