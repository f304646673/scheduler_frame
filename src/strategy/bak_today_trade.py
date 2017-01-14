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

class bak_today_trade(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._daily_temp_conn_name = "daily_temp"
    
    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id  in share_ids:
            self._bak_trade_info(share_id[0])
        LOG_INFO("run bak_today_trade")
    
    def _bak_trade_info(self, share_id):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "trade_info_%s" % (date_info)
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._daily_temp_conn_name)
        
        fields_array = ["today_open","yesteday_close","cur","today_high","today_low","compete_buy_price","compete_sale_price",
                "trade_num","trade_price","buy_1_num","buy_1_price","buy_2_num","buy_2_price","buy_3_num","buy_3_price","buy_4_num","buy_4_price",
                "buy_5_num","buy_5_price","sale_1_num","sale_1_price","sale_2_num","sale_2_price","sale_3_num","sale_3_price","sale_4_num","sale_4_price",
                "sale_5_num","sale_5_price","time_date_str","time_str"]
        daily_data = conn.select(table_name, fields_array, {"share_id":share_id})
        self._bak_single_market_maker_info(share_id, daily_data)

    def _bak_single_market_maker_info(self, share_id, daily_data):
        daily_data_list = []
        has_between_11_30_and_13_00 = False
        after_15_00 = False
        keys_list = []
        for item in daily_data:
            item_list = list(item)
            date_str = item[-2] + " " + item[-1]
            
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

            item_list.insert(0, date_int)
            del item_list[-1]
            del item_list[-1]
            daily_data_list.append(item_list)

        keys_array = ["time","today_open","yesteday_close","cur","today_high","today_low","compete_buy_price","compete_sale_price",
                "trade_num","trade_price","buy_1_num","buy_1_price","buy_2_num","buy_2_price","buy_3_num","buy_3_price","buy_4_num","buy_4_price",
                "buy_5_num","buy_5_price","sale_1_num","sale_1_price","sale_2_num","sale_2_price","sale_3_num","sale_3_price","sale_4_num","sale_4_price",
                "sale_5_num","sale_5_price"]

        share_trade_info_table_name = "trade_info_detail_" +share_id
        self._create_table_if_not_exist(share_id, share_trade_info_table_name)

        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        print daily_data_list
        conn.insert_data(share_trade_info_table_name, keys_array, daily_data_list)

    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        share_ids = fetch_data.get_data(fetch_data.select_db(self._daily_temp_conn_name, trade_table_name, ["share_id"],{}, pre = "distinct"))
        return share_ids

    def _create_table_if_not_exist(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        prepare_table_obj = prepare_table(conn_name, "trade_info")
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

    a = bak_today_trade()
    a.run()
