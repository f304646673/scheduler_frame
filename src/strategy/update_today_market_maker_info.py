#coding=utf-8
#-*- coding: utf-8 -*-
import os
import re
import sys
import time
import fetch_data
import urllib2
sys.path.append("../frame/")

import fetch_data

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from job_base import job_base
from prepare_table import prepare_table
from mysql_manager import mysql_manager

class update_today_market_maker_info(job_base):
    def __init__(self):
        self._conn_name = "daily_temp"
        self._prepare_table = prepare_table(self._conn_name, "today_market_maker")
    
    def run(self):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "market_maker_%s" % (date_info)
        self._prepare_table.prepare(table_name)

        data = self._get_data()
        data = data.replace("-,", "0,")
        self._parse_data_and_insert_db(table_name, data)
        LOG_INFO("run update_today_market_maker_info")

    def _get_data(self):
        date_info = time.strftime('%Y-%m-%d')
        url_fomart = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(BalFlowMain)&sr=-1&p=3&ps=%d&js=var%%20vDUaFOen={pages:(pc),date:%%22%s%%22,data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITA&rt=49430148"
        url = format(url_fomart % (3500, date_info))
        res = fetch_data.get_data(fetch_data.query_http(url))
        return res

    def _parse_data_and_insert_db(self, table_name, data):
        data_array = fetch_data.get_data(fetch_data.regular_split("string_comma_regular", data))
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        into_db_columns = ["market_type", "share_id", "share_name", "price", "up_percent", "market_maker_net_inflow", "market_maker_net_inflow_per", "huge_inflow", "huge_inflow_per", "large_inflow", "large_inflow_per", "medium_inflow", "medium_inflow_per", "small_inflow", "small_inflow_per", "time_str"]
        conn.insert_data(table_name ,into_db_columns, data_array)


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

    a = update_today_market_maker_info()
    a.run()
