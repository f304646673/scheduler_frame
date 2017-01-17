#coding=utf-8
#-*- coding: utf-8 -*-
import os
import re
import sys
import time
import math
import pytz
import numpy
import talib
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
class update_stock_daily_average_info(job_base):
    def __init__(self):
        pass

    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id_item in share_ids:
            share_id = share_id_item[0]
            self._update_average(share_id)           
        LOG_INFO("run update_stock_daily_average_info")
    

    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        #share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{"share_id":[["000001","000301","000601","000901","002101","002401","002701","300001","300301","600301","600601","601801","603001","603601","603901",],"in"]}, pre = "distinct"))
        share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{}, pre = "distinct"))
        return share_ids

    def _get_ma_empty_start_time(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        last_time = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time"], {"close_ma5":[0, "="]}, extend="order by time asc limit 1"))
        if len(last_time) > 0:
            last_day = last_time[0][0]
            tz = pytz.timezone('Asia/Shanghai')
            last_day_obj = datetime.datetime.fromtimestamp(last_day, tz)
            time_str = last_day_obj.strftime("%Y%m%d")
            return time.mktime(time.strptime(time_str, '%Y%m%d'))
        else:
            return 0

    def _get_start_time(self, share_id, table_name, ma_empty_start_time):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        last_time = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time"], {"time":[ma_empty_start_time, "<="]}, extend="order by time desc limit 180"))
        if len(last_time) > 0:
            last_day = last_time[-1][0]
            tz = pytz.timezone('Asia/Shanghai')
            last_day_obj = datetime.datetime.fromtimestamp(last_day, tz)
            time_str = last_day_obj.strftime("%Y%m%d")
            return time.mktime(time.strptime(time_str, '%Y%m%d'))
        else:
            return ma_empty_start_time

    def _get_close_volume(self, share_id, table_name, start_time):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        data = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time", "today_close", "volume"], {"time":[start_time, ">="]}))
        time_list = []
        close_list = []
        volume_list = []
        for item in data:
            time_int = item[0]
            close = item[1]
            volume = item[2]
            time_list.append(time_int)
            close_list.append(close)
            volume_list.append(volume)
        return {"time":time_list, "close":close_list, "volume":volume_list}

    def _get_ma_data(self, ori_data, periods):
        ret_data = {}
        float_data = [float(x) for x in ori_data]
        for period in periods:
            data = talib.MA(numpy.array(float_data), timeperiod = period)
            data_list = data.tolist()
            data_list = self._filter_data(data_list)
            ret_data["%d" % period] = data_list
        return ret_data
 
    def _update_average(self, share_id):
        table_name = "daily_info_%s" % (share_id)
        infos = self._calc_average_data(share_id, table_name)
        for item in infos:
            self._save_data(share_id, table_name, item)

    def _calc_average_data(self, share_id, table_name):
        ma_empty_start_time_int = self._get_ma_empty_start_time(share_id, table_name)
        if ma_empty_start_time_int == 0:
            return []
        start_time_int = self._get_start_time(share_id, table_name, ma_empty_start_time_int)
        stock_info = self._get_close_volume(share_id, table_name, start_time_int)
        periods = [5, 10, 20, 30, 60, 90, 180]
        #periods = [90, 180]
        close_data = self._get_ma_data(stock_info["close"], periods)
        volume_data = self._get_ma_data(stock_info["volume"], periods)
        if len(stock_info["time"]) == len(close_data["180"]) and len(close_data["180"]) == len(volume_data["180"]):
            pass
        else:
            LOG_WARNING("calc %s daily average error" % share_id)
            return

        infos = []
        data_len = len(stock_info["time"])
        for index in range(data_len):
            info = {}
            time_int = stock_info["time"][index]
            if time_int < ma_empty_start_time_int:
                continue
            info["time"] = time_int
            for period in periods:
                info["close_ma%s" % period] = close_data["%s" % period][index]
                info["volume_ma%s" % period] = volume_data["%s" % period][index]
            infos.append(info)
        return infos

    def _filter_data(self, data):
        for index in range(len(data)):
            if math.isnan(data[index]):
                data[index] = 0.01
            else:
                break
        return data

    def _save_data(self, share_id, table_name, data):
        if len(data) < 2:
            return 
        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        conn.update(table_name, data, ["time"])

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

    a = update_stock_daily_average_info()
    a.run()
