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

class update_today_trade_info(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._conn_name = "daily_temp"
        self._base_conn_name = "stock_db"
        self._prepare_table = prepare_table(self._conn_name, "today_trade_info")
        self._pre_share_count = 0
        self._share_ids_str_list = []
    
    def run(self):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "trade_info_%s" % (date_info)
        self._prepare_table.prepare(table_name)
        
        ids_str_list = self._get_all_share_ids()
        for ids_str in ids_str_list:
            data = self._get_data(ids_str)
            self._parse_data_and_insert_db(table_name, data)
        LOG_INFO("run update_today_trade_info")

    def _get_all_share_ids(self):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._base_conn_name)
        count_info = conn.select("share_base_info", ["count(*)"],{})
        count = int(count_info[0][0])
        if count == self._pre_share_count:
            return self._share_ids_str_list
        #share_ids = conn.select("share_base_info", ["share_id", "market_type"],{"share_id":"601375"})
        share_ids = conn.select("share_base_info", ["share_id", "market_type"],{})
        ids = []
        for item in share_ids:
            market_type = item[1]
            pre = ""
            if 1 == market_type:
                pre = "sh"
            elif 2 == market_type:
                pre = "sz"
            else:
                continue
            new_id = pre + item[0]
            ids.append(new_id)

        id_count = len(ids)
        count_per = 500
        page = id_count/count_per
        ids_str_list = []
        for index in range(page+1):
            ids_list = ids[index*count_per:(index+1)*count_per]
            ids_str = "," . join(ids_list)
            ids_str_list.append(ids_str)
        self._share_ids_str_list = ids_str_list
        self._pre_share_count = count
        return self._share_ids_str_list


    def _get_data(self, ids):
        url_pre = "http://hq.sinajs.cn/list="
        url = url_pre + ids
        res = fetch_data.get_data(fetch_data.query_http(url))
        res = res.decode("gbk").encode("utf-8")
        return res

    def _parse_data_and_insert_db(self, table_name, data):
        ret_array = fetch_data.get_data(fetch_data.regular_split("hq_sinajs_cn_list", data))
        if 0 == len(ret_array):
            LOG_WARNING("hg_sinajs_cn_list regular %s empty data" %(data))
            return

        data_array = []

        into_db_columns = ["share_id","share_name","today_open","yesteday_close","cur","today_high","today_low","compete_buy_price","compete_sale_price",
                "trade_num","trade_price","buy_1_num","buy_1_price","buy_2_num","buy_2_price","buy_3_num","buy_3_price","buy_4_num","buy_4_price",
                "buy_5_num","buy_5_price","sale_1_num","sale_1_price","sale_2_num","sale_2_price","sale_3_num","sale_3_price","sale_4_num","sale_4_price",
                "sale_5_num","sale_5_price","time_date_str","time_str","empty"]
        columns_count = len(into_db_columns)

        for item in ret_array:
            if len(item) != columns_count:
                LOG_INFO("%s length is not match for column length %d" %(str(item), columns_count))
                continue
            data_array.append(item)

        if 0 == len(data_array):
            return

        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        conn.insert_data(table_name ,into_db_columns, data_array)

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

    a = update_today_trade_info()
    a.run()
