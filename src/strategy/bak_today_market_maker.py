#coding=utf-8
#-*- coding: utf-8 -*-

import os
import re
import sys
import time
import urllib2
sys.path.append("../frame/")

import fetch_data

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from job_base import job_base
from prepare_table import prepare_table
from mysql_manager import mysql_manager

from stock_conn_manager import stock_conn_manager

class bak_today_market_maker(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._daily_temp_conn_name = "daily_temp"
    
    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id  in share_ids:
            self._bak_market_maker_info(share_id[0])
        LOG_INFO("run bak_today_market_maker")
    
    def _bak_market_maker_info(self, share_id):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "market_maker_%s" % (date_info)
        
        fields_array =["time_str", "price", "up_percent", "market_maker_net_inflow", "market_maker_net_inflow_per",
                "huge_inflow", "huge_inflow_per", "large_inflow", "large_inflow_per", "medium_inflow", "medium_inflow_per", "small_inflow", "small_inflow_per"]
        
        daily_data = fetch_data.get_data(fetch_data.select_db(self._daily_temp_conn_name, table_name, fields_array, {"share_id":share_id}))
        self._bak_single_market_maker_info(share_id, daily_data)

    def _bak_single_market_maker_info(self, share_id, daily_data):
        daily_data_list = []
        has_between_11_30_and_13_00 = False
        after_15_00 = False
        keys_list = []
        for item in daily_data:
            item_list = list(item)
            date_str = item[0]
            
            today_11_30 = date_str[:date_str.find(" ")] + " 11:30:00" 
            today_13_00 = date_str[:date_str.find(" ")] + " 13:00:00"
            today_15_00 = date_str[:date_str.find(" ")] + " 15:00:00"
            today_11_30_int = time.mktime(time.strptime(today_11_30,'%Y-%m-%d %H:%M:%S'))
            today_13_00_int = time.mktime(time.strptime(today_13_00,'%Y-%m-%d %H:%M:%S'))
            today_15_00_int = time.mktime(time.strptime(today_15_00,'%Y-%m-%d %H:%M:%S'))

            date_int = time.mktime(time.strptime(date_str,'%Y-%m-%d %H:%M:%S'))
            if date_int >= today_11_30_int and date_int < today_13_00_int:
                if has_between_11_30_and_13_00:
                    continue
                else:
                    has_between_11_30_and_13_00 = True

            if date_int >= today_15_00_int:
                if after_15_00:
                    continue
                else:
                    after_15_00 = True

            if date_int in keys_list:
                continue
            else:
                keys_list.append(date_int)

            item_list[0] = date_int
            daily_data_list.append(item_list)
        keys_array =["time", "price", "up_percent", "market_maker_net_inflow", "market_maker_net_inflow_per",
                "huge_inflow", "huge_inflow_per", "large_inflow", "large_inflow_per", "medium_inflow", "medium_inflow_per", "small_inflow", "small_inflow_per"]

        share_market_maker_table_name = "market_maker_detail_" + share_id
        self._create_table_if_not_exist(share_id, share_market_maker_table_name)

        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        conn.insert_data(share_market_maker_table_name, keys_array, daily_data_list)

    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        share_ids = fetch_data.get_data(fetch_data.select_db(self._daily_temp_conn_name, trade_table_name, ["share_id"],{}, pre = "distinct"))
        return share_ids

    def _create_table_if_not_exist(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        prepare_table_obj = prepare_table(conn_name, "market_maker")
        prepare_table_obj.prepare(table_name)
        
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

    a = bak_today_market_maker()
    a.run()
