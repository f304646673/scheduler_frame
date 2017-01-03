import re
import MySQLdb
import type_check
from loggingex import LOG_WARNING
from loggingex import LOG_INFO

class mysql_conn():
    _conn = None
    def __init__(self, host_name, port_num, user_name, password, db_name, charset_name = "utf8"):
        self._conn = MySQLdb.connect(host = host_name, port = port_num, user = user_name, passwd = password, db = db_name, charset = charset_name)
        if None == self._conn:
            LOG_WARNING("connect mysql %s:%d error" % (host_name, port_num))
            return
        self._get_tables_info()

    def __del__(self):
        if self._conn:
            self._conn.close()

    
    def _get_tables_info(self):
        tables_info = {}
        cursor = self._conn.cursor()
        tables_sql = "show tables"
        cursor.execute(tables_sql)
        tables_name = cursor.fetchall()
        for table_name_item in tables_name:
            table_name = table_name_item[0]
            if 0 == len(table_name):
                continue
            columns_sql = "show columns from " + table_name 
            cursor.execute(columns_sql)
            table_info = cursor.fetchall()
            columns_info = self._get_table_info(table_info)
            if len(columns_info):
                tables_info[table_name] = columns_info
        cursor.close()
        print tables_info

    def _get_table_info(self, table_info):
        columns_info = {}
        for item in table_info:
            column_name = item[0]
            column_type_info = item[1]
            (type, len) = self._get_column_type_info(column_type_info)
            columns_info[column_name] = {"type":type, "length":len}
        return columns_info

    def _get_column_type_info(self, type_info):
        re_str = '(\w*)\((\d*)\)'
        kw = re.findall(re_str, type_info)
        if len(kw):
            if len(kw[0]) > 1:
                return (kw[0][0], kw[0][1])
        return (None, None)

    def _implode(self, array_data, escape = False):
        array_str = ""
        for item in array_data:
            if 0 != len(array_str):
                array_str += ","
            if escape:
                if type_check.IsNumber(item) or type_check.IsFloat(item):
                    array_str += str(item)
                elif type_check.IsString(item):
                    array_str += "'"
                    array_str += item.encode("string_escape")
                    array_str += "'"
            else:
                array_str += str(item)
        array_str = "(" + array_str + ")"
        return array_str

    def insert_data(self, table_name, columns_name, data_array):
        cursor = self._conn.cursor()
        columns = self._implode(columns_name)
        value_list = []
        for item in data_array:
            value_str = self._implode(item, True)
            value_list.append(value_str)
        values_sql = ",".join(value_list)

        sql = "insert into " + table_name + columns + " values " + values_sql
        LOG_INFO(sql)
        try:
            cursor.execute(sql)
            self._conn.commit()
        except:
            LOG_WARNING("%s execute error" % (sql))
            self._conn.rollback()
        cursor.close()


if __name__ == "__main__":
    a = mysql_manager("127.0.0.1", 3306, "root", "fangliang", "stock")
    a.insert_data("share_base_info", ["share_id", "share_name"], [[1,"2'xxx"], [3,"'4yyy"]])
