import re
import sys
import json
import ConfigParser

from mysql_manager import mysql_manager
from loggingex import LOG_WARNING

class regualer_into_db:
    def __init__(self, conf_path):
        self._path = conf_path
        self._cp = ConfigParser.SafeConfigParser()
        self._cp.read(conf_path)
    
    def walks(self, data):
        section_name = "strategy"
        if False == self._cp.has_section(section_name):
            LOG_WARNING("%s has no %s" % (self._path, section_name))
            return
        if len(self._get_regular(0)) == 0 :
            LOG_WARNING("%s has no regular" % (self._path))
            return
        data_array = []
        self._recursion_regular(data, 0, data_array)   
        if len(data_array):
            self._insert_db(data_array)

    def _get_regular(self, deep):
        section_name = "strategy"
        regular_name_pre = "regular_expression_"
        regular_name = regular_name_pre + str(deep)
        if False == self._cp.has_option(section_name, regular_name):
            return ""
        regular_str = self._cp.get(section_name, regular_name)
        return regular_str

    def _recursion_regular(self, data, deep, data_array):
        regular_str = self._get_regular(deep)
        split_data = re.findall(regular_str, data)
        regualer_next_str = self._get_regular(deep + 1)
        split_array = []
        if len(regualer_next_str) > 0:
            for item in split_data:
                self._recursion_regular(item, deep + 1, data_array)
        else:
            for item in split_data:
                split_array.append(item)
            if len(split_array) > 0:
                data_array.append(split_array)

    def _insert_db(self, data_array):
        section_name = "strategy"
        conn_name_name = "conn_name"
        if False == self._cp.has_option(section_name, conn_name_name):
            LOG_WARNING("%s has no %s %s" % (self._path, section_name, conn_name_name))
            return False
        conn_name = self._cp.get(section_name, conn_name_name)
        table_name_name = "table_name"
        if False == self._cp.has_option(section_name, table_name_name):
            LOG_WARNING("%s has no %s %s" % (self._path, section_name, table_name_name))
            return False
        table_name = self._cp.get(section_name, table_name_name)
        keys_info_name = "keys_info"
        if False == self._cp.has_option(section_name, keys_info_name):
            LOG_WARNING("%s has no %s %s" % (self._path, section_name, keys_info_name))
            return False
        keys_info = self._cp.get(section_name, keys_info_name)

        db_columns_info = json.loads(keys_info)
        into_db_columns = db_columns_info.keys()
        into_db_index = db_columns_info.values()

        into_db_values_array = []
        for data in data_array:
            values = []
            for index in into_db_index:
                values.append(data[index])
            into_db_values_array.append(values)

        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(conn_name)
        if None == conn:
            LOG_WARNING("%s get db connect %s error" % (self._path, conn_name))
            return False
        conn.insert_data(table_name, into_db_columns, into_db_values_array)
        return True

if __name__ == "__main__":
    a = mysql_manager()
    test_data_1 = {"stock_db":{"host":"127.0.0.1", "port":3306, "user":"root", "passwd":"fangliang", "db":"stock", "charset":"utf8"}}
    a.add_conns(test_data_1)
    a = regualer_into_db("../../conf/market_maker_strategy.conf")
    data = 'var SXcUovEK={pages:1646,date:"2014-10-22",data:["2,000858,DDDD,35.90,3.58,29113.49,17.77,16218.12,9.90,12895.37,7.87,-11230.50,-6.86,-17882.99,-10.92,2017-01-04 15:00:00","1,603298,ABCD,29.38,10.00,25382.35,29.62,15561.30,18.16,9821.05,11.46,12111.83,14.13,-37494.18,-43.75,2017-01-04 15:00:00"]}'
    a.walks(data)
    pass
