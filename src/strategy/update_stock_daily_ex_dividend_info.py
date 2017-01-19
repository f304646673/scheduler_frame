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

class update_stock_daily_ex_dividend_info(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._table_keys = ["time", "time_str", "today_close", "today_high", "today_low", "today_open", "yesterday_close", "pchg", "turnover_rate", "volume", "turnover"]

    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id  in share_ids:
            self._update_stock_ex_dividend_info(share_id[0])
        LOG_INFO("run update_stock_daily_ex_dividend_info")
    
    def _update_stock_ex_dividend_info(self, share_id):
        ex_dividend_table_name = "daily_info_ex_dividend_%s" % (share_id)
        daily_info_table_name = "daily_info_%s" % (share_id)
        self._create_table_if_not_exist(share_id, ex_dividend_table_name)
        ex_dividend_last_time = self._get_ex_dividend_last_time(share_id, ex_dividend_table_name)

        ex_dividend_table_name = "daily_info_ex_dividend_%s" % (share_id)
        daily_info_table_name = "daily_info_%s" % (share_id)
        last_yesterday_close = self._dividend_ori_data(share_id, daily_info_table_name, ex_dividend_table_name, ex_dividend_last_time)
        if 0 == last_yesterday_close:
            return
        
        self._dividend_ori_data(share_id, ex_dividend_table_name, ex_dividend_table_name, ex_dividend_last_time, "<=", last_yesterday_close)

    def _dividend_ori_data(self, share_id, from_table, to_table, start_time, compare = ">", yesterday_close = 0):
        ori_data = self._get_daily_info(share_id, from_table, start_time, compare)
        if 0 == len(ori_data):
            return 0
        if ori_data[0][6] == yesterday_close:
            return 0

        ex_dividend_ori = []
        pre_div_value = 1
        for item in ori_data:
            if 0 == yesterday_close:
                ex_dividend_ori.append(item)
                yesterday_close = item[6]
                continue
            
            if len(ex_dividend_ori) > 0:
                yesterday_close = ex_dividend_ori[-1][6]
            
            ori_close = item[2]
            if ori_close == 0 or yesterday_close == 0:
                div_value = pre_div_value
            else:
                if yesterday_close == ori_close:
                    ex_dividend_ori.append(item)
                    continue
                div_value = yesterday_close/ori_close
            pre_div_value = div_value
            ex_dividend_ori.append([item[0], item[1], item[2] * div_value, item[3] * div_value,item[4] * div_value,item[5] * div_value,item[6] * div_value,item[7],item[8],item[9] / div_value,item[10]])
        
        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        if from_table != to_table:
            conn.insert_data(to_table, self._table_keys, ex_dividend_ori)
        else:
            for info_value in ex_dividend_ori:
                infos = {}
                for index in range(len(self._table_keys)):
                    infos[self._table_keys[index]] = info_value[index]
                conn.insert_onduplicate(to_table, infos, ["time"])

            conn.insert_onduplicate(to_table, {"close_ma5":0, "time":ex_dividend_ori[-1][0]}, ["time"])
        
        last_yesterday_close = ex_dividend_ori[-1][6]
        return last_yesterday_close 

    def _get_ex_dividend_last_time(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        data = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time"], {}, extend = "order by time desc limit 1"))
        if len(data) > 0:
            last_day = data[0][0]
            return int(last_day)
        else:
            return 0
 
    def _get_daily_info(self, share_id, table_name, start_time, compare):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        data = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, self._table_keys, {"time":[start_time, compare]}, extend = "order by time desc"))
        return data

    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{"share_id":["000001","="]}, pre = "distinct"))
        #share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"], {}, pre = "distinct"))
        return share_ids

    def _create_table_if_not_exist(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        prepare_table_obj = prepare_table(conn_name, "daily_info")
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

    a = update_stock_daily_ex_dividend_info()
    a.run()
