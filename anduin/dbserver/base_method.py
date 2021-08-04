class base_method(object):
    @staticmethod
    def find_info(table, conditions, or_cond, fields=None, group=None, order=None, limit=None, for_update=False):

        if fields is None:
            return

        sql = 'select %s from %s where  ' % (','.join(fields), table)
        # dbg(sql)
        sql, sql_params = base_method.bind_conditions(sql, conditions, or_cond)

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
