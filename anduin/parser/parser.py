from anduin.common.tools import ENGINE_SQLITE, ENGINE_MYSQL


class sql_parser(object):
    @staticmethod
    def create_table_parser(table,columns,table_comment='',sql_engine=ENGINE_MYSQL):
        sql = 'create table %s(' % table
        tail = ''
        for item in columns:
            col = ''
            if sql_engine == ENGINE_SQLITE:
                item = item[:-1]
            for i in item:
                col += str(i)
                col += ' '
            col += ','
            tail += col

        tail = tail[:-1] + ')'
        sql += tail
        sql += 'charset=utf8mb4,engine=innodb,comment="%s"' % table_comment
        if sql_engine == ENGINE_MYSQL:
            sql += 'charset=utf8mb4,engine=innodb,comment="%s"' % table_comment
        if sql_engine ==ENGINE_SQLITE:
            sql = sql.replace('int AUTO_INCREMENT primary key', 'INTEGER PRIMARY KEY AUTOINCREMENT')
        return sql
    # 拼接create sql

    @staticmethod
    def insert_parser(table,content):
        params = content
        keys = str(tuple(params.keys()))
        keys = keys.replace("'", "")
        if 1 == len(params):
            keys = keys[:-2] + ")"
        sql_params = []
        sql = 'insert into %s%s values (' % (table, keys)
        for i in params.values():
            sql_params.append(i)
        for _ in params.values():
            sql += '%s,'
        sql = sql[:-1]
        sql += ')'
        return sql,sql_params
    # insert规则拼接

    @staticmethod
    def find_info(table, conditions, or_cond, fields=None, group=None, order=None, limit=None, for_update=False):
        if fields is None:
            return
        sql = 'select %s from %s where  ' % (','.join(fields), table)
        # dbg(sql)
        sql, sql_params = sql_parser.bind_conditions(sql, conditions, or_cond)

        if group is not None:
            sql += 'group by '
            for i in group:
                sql += '%s ,' % i
            sql = sql[:-1]

        if order is not None:
            sql += 'order by '
            count = 0
            for i in order:
                if count % 2 == 0:
                    sql += '%s ' % i
                else:
                    sql += '%s ,' % i
                count += 1
            sql = sql[:-1]

        if limit is not None:
            sql += 'limit '
            for i in limit:
                sql += '%s, ' % i
            sql = sql[:-2]

        if for_update is True:
            sql += ' for update'
        return sql, sql_params
    # select 规则拼接

    @staticmethod
    def bind_conditions(sql, conditions, or_cond):
        sql_params = []
        if conditions is None:
            conditions = []
        if or_cond is None:
            or_cond = []
        if len(or_cond) + len(conditions) == 0:
            return sql[:-7], None

        if len(conditions) > 0:
            for unit in conditions:
                value = unit[2]
                sql = sql + " %s %s binary " % (unit[0], unit[1]) + '%s'
                sql_params += [value]
                sql += "  and "
                if 'in' in unit[1]:
                    sql = sql.replace('binary', '')
            sql = sql[:-4]
            if len(or_cond) > 0:
                sql += ' or '

        if len(or_cond) > 0:
            for unit in or_cond:
                value = unit[2]
                sql = sql + " %s %s binary " % (unit[0], unit[1]) + '%s'
                sql_params += [value]
                sql += "  or "
                if 'in' in unit[1]:
                    sql = sql.replace('binary', '')
            sql = sql[:-3]

        return sql, sql_params

    @staticmethod
    def update_parser(table,conditions,or_cond,params):
        if params == {} or params is None:
            # dbg('没有params，结束执行')
            return
        sql = 'update %s set ' % table
        sql_params_header = []
        for param, value in params.items():
            sql = sql + " %s = " % param + '%s,'
            sql_params_header += [value]

        sql = sql[:-1] + ' where  '
        sql, sql_params = sql_parser.bind_conditions(sql, conditions, or_cond)
        # dbg(sql)
        if sql_params is None:
            sql_params = []
        sql_params = sql_params_header + sql_params
        return sql,sql_params

if __name__ == '__main__':
    sql = sql_parser.update_parser('test',[('id','=',1)],[],{'username':'user','k_2':22})
    print(sql)