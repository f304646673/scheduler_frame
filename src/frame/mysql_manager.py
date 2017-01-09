import json
import frame_tools
from collections import OrderedDict

import conf_keys
from mysql_conn import mysql_conn
from loggingex import LOG_WARNING
from loggingex import LOG_INFO
from singleton import singleton
from mysql_conn import mysql_conn

class mysql_conn_info:
    
    def __init__(self):
        self.valid = 0
        self.conns_dict = OrderedDict()

@singleton
class mysql_manager():
    def __init__(self):
        self._conns = {}

    def modify_conns(self, conns_info):
        for (conn_name, conn_info) in conns_info.items():
            conn_info_hash = frame_tools.hash(json.dumps(conn_info))
            if conn_name in self._conns.keys():
                if conn_info_hash in self._conns[conn_name].conns_dict.keys():
                    continue
            else:
                self._conns[conn_name] = mysql_conn_info()

            for key in conf_keys.mysql_conn_keys:
                if key not in conn_info.keys():
                    continue
            conn_obj = mysql_conn(conn_info["host"], conn_info["port"], conn_info["user"], conn_info["passwd"], conn_info["db"], conn_info["charset"])
            self._conns[conn_name].conns_dict[conn_info_hash] = conn_obj
            self._conns[conn_name].valid = 1
        self._print_conns()

    def add_conns(self, conns_info):
        self.modify_conns(conns_info)

    def remove_conns(self, conns_info):
        for (conn_name, conn_info) in conns_info.items():
            conn_info_hash = frame_tools.hash(json.dumps(conn_info))
            if conn_name in  self._conns.keys():
                if conn_info_hash in self._conns[conn_name].conns_dict.keys():
                    self._conns[conn_name].valid = 0
        self._print_conns()
 
    def get_mysql_conn(self, conn_name):
        if conn_name not in self._conns.keys():
            return None
        conn_info = self._conns[conn_name]
        valid = self._conns[conn_name].valid
        if 0 == valid:
            return None
        conns_dict_keys = self._conns[conn_name].conns_dict.keys()
        if len(conns_dict_keys) == 0:
            return None
        key = conns_dict_keys[-1]
        conn = self._conns[conn_name].conns_dict[key]
        return conn

    def _print_conns(self):
        for (conn_name, conn_info) in self._conns.items():
            out_str = "conn name: " + conn_name + "\n"
            out_str = out_str +  "conn info valid: " + str(conn_info.valid) + "\n"
            for (key, value) in conn_info.conns_dict.items():
                out_str = out_str + key + str(value) + "\n"
            LOG_INFO(out_str)
    
    def refresh_all_conns_tables_info(self):
        for (conn_name, conn_info) in self._conns.items():
            conn = self.get_mysql_conn(conn_name)
            if None != conn:
                conn.refresh_tables_info()

if __name__ == "__main__":
    a = mysql_manager()
    test_data_1 = {"a1":{"host":"127.0.0.1", "port":123, "user":"fangliang", "passwd":"fl_pwd", "db":"db1", "charset":"utf8"}}
    a.add_conns(test_data_1)
    
    print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    a.add_conns(test_data_1)
    
    print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    test_data_2 = {"a2":{"host":"127.0.0.2", "port":123, "user":"fangliang", "passwd":"fl_pwd", "db":"db1", "charset":"utf8"}}
    a.add_conns(test_data_2)
    
    print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    test_data_3 = {"a2":{"host":"127.0.0.3", "port":123, "user":"fangliang", "passwd":"fl_pwd", "db":"db1", "charset":"utf8"}}
    a.modify_conns(test_data_3)
    
    print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    test_data_4 = {"a2":{"host":"127.0.0.3", "port":123, "user":"fangliang", "passwd":"fl_pwd", "db":"db1", "charset":"utf8"}}
    a.remove_conns(test_data_4)
    pass
