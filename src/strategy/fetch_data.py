#coding=utf-8
#-*- coding: utf-8 -*-

import os
import re
import sys
import time
import urllib2
sys.path.append("../frame/")

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from mysql_manager import mysql_manager
from regular_split_manager import regular_split_manager

class select_db:
    def __init__(self, conn_name, table_name, select_columns, conditions, pre=""):
        self._conn_name = conn_name
        self._table_name = table_name
        self._select_columns = select_columns
        self._conditions = conditions
        self._pre = pre

    def get_data(self):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        result = conn.select(self._table_name, self._select_columns, self._conditions, self._pre)
        return result

class query_http:
    def __init__(self, url):
        self._url = url

    def get_data(self):
        res = ""
        tried = False
        while True:
            try:
                req = urllib2.Request(self._url)
                res_data = urllib2.urlopen(req)
                res = res_data.read()
                break
            except Exception as e:
                LOG_ERROR("request error: %s"  % e)
                if tried:
                    break
                else:
                    tried = True
        return res

class regular_split:
    def __init__(self, regular_name, data):
        self._regular_name = regular_name
        self._data = data

    def get_data(self):
        regular_split_mgr = regular_split_manager()
        ret_array = regular_split_mgr.get_split_data(self._data, self._regular_name)
        return ret_array

def get_data(queries):
    results = []
    for item in queries:
        if False == hasattr(item, "get_data"):
            continue
        result = item.get_data()
        results.append(result)
    return results

if __name__ == "__main__":
    import os
    os.chdir("../../")
    sys.path.append("./src/frame/")
    
    from j_load_mysql_conf import j_load_mysql_conf
    from j_load_regular_conf import j_load_regular_conf
    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
    j_load_regular_conf_obj = j_load_regular_conf()
    j_load_regular_conf_obj.run()

    j_load_mysql_conf_obj = j_load_mysql_conf()
    j_load_mysql_conf_obj.run()

    qh1 = query_http("http://hq.sinajs.cn/?list=sz000001")
    data_list = get_data([qh1])
    for item in data_list:
        rs1 = regular_split("hq_sinajs_cn_list", item)
        print get_data([rs1])


    #sd1 = select_db("stock_db","share_base_info", ["share_id"],{})
    #print get_data([sd1])

