#coding=utf-8
#-*- coding: utf-8 -*-

import os
import re
import sys
sys.path.append("../frame/")

import fetch_data

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from job_base import job_base
from prepare_table import prepare_table
from mysql_manager import mysql_manager
from regular_split_manager import regular_split_manager

class update_stock_base_info(job_base):
    def __init__(self):
        self._regular_split_manager = regular_split_manager()
        self._db_manager = mysql_manager()
        self._conn_name = "stock_db"
        self._prepare_table = prepare_table(self._conn_name, "share_base")
        self._table_name = "share_base_info"

    def run(self):
        data = self._get_data()
        self._prepare_table.prepare(self._table_name)
        self._save_data(data)
        LOG_INFO("run update_stock_base_info")

    def _get_data(self):
        url = r"http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(FFRank)&sr=1&p=1&ps=3500&js=var%20mozselQI={pages:(pc),data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITAM&rt=49461817"
        res = fetch_data.get_data(fetch_data.query_http(url))
        return res

    def _save_data(self, data):
        data_array = self._regular_split_manager.get_split_data(data, "string_comma_regular")
        for item in data_array:
            share_market_type = item[0]
            share_id = item[1]
            share_name = item[2]
            if len(share_id) > 0 and len(share_name) > 0:
                share_info = {"share_id":share_id, "share_name":share_name, "market_type":share_market_type}
                conn = self._db_manager.get_mysql_conn(self._conn_name)
                conn.insert_onduplicate(self._table_name, share_info, ["share_id"])


if __name__ == "__main__":
    import os
    os.chdir("../../")
    sys.path.append("./src/frame/")
    
    import sys
    reload(sys)
    sys.setdefaultencoding("utf8")
    
    from j_load_mysql_conf import j_load_mysql_conf
    from j_load_regular_conf import j_load_regular_conf
    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
    j_load_regular_conf_obj = j_load_regular_conf()
    j_load_regular_conf_obj.run()

    j_load_mysql_conf_obj = j_load_mysql_conf()
    j_load_mysql_conf_obj.run()

    a = update_stock_base_info()
    a.run()
