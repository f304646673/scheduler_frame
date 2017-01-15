#coding=utf-8
#-*- coding: utf-8 -*-
import os
import re
import sys
import time
import pytz
import datetime
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

class update_stock_daily_info(job_base):
    def __init__(self):
        pass

    def run(self):
        share_id_market_type = self._get_all_share_ids_market_type()
        for id_market_type in share_id_market_type:
            share_id = id_market_type[0]
            market_type = id_market_type[1]
            print share_id
            self._query_save_data(share_id, market_type)           
        LOG_INFO("run update_stock_daily_info")

    def _get_start_time(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        last_time = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time"], {}, extend="order by time desc limit 1"))
        if len(last_time) > 0:
            last_day = last_time[0][0]
            tz = pytz.timezone('Asia/Shanghai')
            last_day_obj = datetime.datetime.fromtimestamp(last_day, tz)
            next_day_obj = last_day_obj + datetime.timedelta(days = 1)
            time_str = next_day_obj.strftime("%Y%m%d")
        else:
            time_str = "19900101"
        return time.mktime(time.strptime(time_str, '%Y%m%d'))

    def _query_save_data(self, share_id, market_type):
        table_name = "daily_info_%s" % (share_id)
        market_type = market_type - 1

        self._create_table_if_not_exist(share_id, table_name)
        
        start_time_int = self._get_start_time(share_id, table_name)
        end_time_str = time.strftime('%Y%m%d')
        end_time_int = time.mktime(time.strptime(end_time_str, '%Y%m%d'))
        if end_time_int <= start_time_int:
            return

        start_time_obj = time.localtime(start_time_int)
        start_time_str = time.strftime("%Y%m%d", start_time_obj)
        data = self._get_data(market_type, share_id, start_time_str, end_time_str)
        filter_data = self._filter_data(data)
        self._save_data(share_id, table_name, filter_data)

    def _get_all_share_ids_market_type(self):
        share_ids = self._get_all_share_ids()
        ids = []
        for share_id in share_ids:
            ids.append(share_id[0])
        share_ids = fetch_data.get_data(fetch_data.select_db("stock_db", "share_base_info", ["share_id", "market_type"],{"share_id":ids}))
        return share_ids
        
    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{}, pre = "distinct"))
        return share_ids
    
    def _get_data(self, market_type, id, start_time, end_time):
        url_format = """http://quotes.money.163.com/service/chddata.html?code=%d%s&start=%s&end=%s&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;PCHG;TURNOVER;VOTURNOVER"""
        url = url_format % (market_type, id, start_time, end_time)
        res = fetch_data.get_data(fetch_data.query_http(url))
        #res = res.decode("gbk").encode("utf-8")
        return res
    
    def _filter_data(self, data):
        data = data.replace("None", "0")
        filter_data = fetch_data.get_data(fetch_data.regular_split("quotes_money_163", data))
        if len(filter_data) > 0:
            del filter_data[0]
        for item in filter_data:
            time_str = item[0]
            time_int = time.mktime(time.strptime(time_str,'%Y-%m-%d'))
            item.insert(0, time_int)
            del item[2]
            del item[2]
            if item[2] == 0:
                del item
        return filter_data

    def _save_data(self, share_id, table_name, data):
        into_db_columns = ["time","time_str","today_close","today_high","today_low","today_open","yesteday_close","pchg","turnover_rate","volume"]
        columns_count = len(into_db_columns)

        for item in data:
            if len(item) != columns_count:
                LOG_INFO("%s length is not match for column length %d" %(str(item), columns_count))
                continue
            del item

        if 0 == len(data):
            return
        
        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        conn.insert_data(table_name, into_db_columns, data)

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

    a = update_stock_daily_info()
    a.run()
