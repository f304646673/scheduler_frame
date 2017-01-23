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
class second_buy_point(job_base):
    def __init__(self):
        pass

    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id_item in share_ids:
            share_id = share_id_item[0]
            self._calc(share_id)           
        LOG_INFO("run second_buy_point")

    def _get_all_share_ids(self):
        date_info = time.strftime('%Y_%m_%d')
        trade_table_name = "trade_info_%s" % (date_info)
        share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{"share_id":[["000001","000010","000301","000601","000901","002101","002401","002701","300001","300301","600301","600601","601801","603001","603601","603901"],"in"]}, pre = "distinct"))
        #share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{}, pre = "distinct"))
        #share_ids = fetch_data.get_data(fetch_data.select_db("daily_temp", trade_table_name, ["share_id"],{"share_id":["000001","="]}, pre = "distinct"))
        return share_ids

    def _calc(self, share_id):
        table_name = "daily_info_ex_dividend_%s" % (share_id)
        all_data = self._get_average_info(share_id, table_name, 360)
        indexs_info = self._get_cmp_indexs(all_data)
        match_indexs = indexs_info[0]
        mismatch_indexs_info = indexs_info[1]
        #print "**********************************************************"
        #print share_id
        #print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
        #print "match indexs:"
        #self._print_result(all_data, match_indexs)
        #print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
        #print "mismatch_indexs:"
        #self._print_result(all_data, mismatch_indexs_info)
        #print "#####################################################"
        filter_results = self._filter_indexs(all_data, match_indexs)
        buy_info = filter_results[0]
        buy_info = self._filter_time(all_data, buy_info)
        #not_buy_info = filter_results[1]
        if len(buy_info) > 0:
            print share_id
            print "buy info:"
            self._print_result(all_data, buy_info)
            print "#####################################################"
        #print "not buy info"
        #self._print_result(all_data, not_buy_info)
        #print "**********************************************************"
    
    def _filter_time(self, data, indexs_info):
        tendays_ago = (datetime.datetime.now() - datetime.timedelta(days = 10))
        timeStamp = int(time.mktime(tendays_ago.timetuple()))
        filter_data = []
        for index_info in indexs_info:
            index = index_info[0]
            info = index_info[1]
            a = data[index]["time"]
            if timeStamp < a:
                filter_data.append(index_info)
        return filter_data

    def _print_result(self, data, indexs_info):
        for index_info in indexs_info:
            index = index_info[0]
            info = index_info[1]
            a = data[index]["time"]
            x = time.localtime(int(a)) 
            time_str = time.strftime('%Y-%m-%d', x)
            print "%s %d %s" % (time_str, index, info)

    def _get_history_data(self, data, index, period, type, pre = True):
        if pre:
            period = -1 * period
        new_index = index + period
        if new_index < 0 or new_index >= len(data):
            return None
        return (new_index, data[new_index][type])

    def _get_history_max_min_info(self, data, index, period, type, include_today = True, pre = True, max=True):
        cmp_result = None
        for iter in range(period):
            if False == include_today and iter == 0:
                continue
            value_info = self._get_history_data(data, index, iter, type, pre)
            if value_info:
                pass
            else:
                continue

            if cmp_result:
                if max:
                    if value_info[1] > cmp_result[1]:
                        cmp_result = value_info
                else:
                    if value_info[1] < cmp_result[1]:
                        cmp_result = value_info
            else:
                cmp_result = value_info
        return cmp_result

    def _check_cmp_cond(self, data, index):
        period = 5
        close_offset_max = 0.4
        index_volume_times_than_max = 1.2
        after_period = 5
        if period > index:
            return (False, "period(%d) bigger than index(%d)" % (period, index))
        
        pchg_max_info = self._get_history_max_min_info(data, index, period, "pchg", include_today = True, pre = True, max=True)
        volume_max_info = self._get_history_max_min_info(data, index, period, "volume", include_today = True, pre = True, max=True)
        high_max_info = self._get_history_max_min_info(data, index, period, "today_high", include_today = True, pre = True, max=True)
        if None == pchg_max_info or None == volume_max_info or None == high_max_info:
            return (False, "get %d pre %d pchg volume today_high error" % (index, period))
        
        cmp_index = volume_max_info[0]
        if cmp_index != high_max_info[0] and cmp_index != pchg_max_info[0]:
            return (False, "vol max index(%d) not equal high max index(%d) or pchg max index(%d)" % (volume_max_info[0], high_max_info[0], pchg_max_info[0]))
        
        if cmp_index != index:
            return (False, "today(%d) is not compare day(%d)" % (index, cmp_index))

        today_high_info = self._get_history_data(data, index, 0, "today_high")
        today_close_info = self._get_history_data(data, index, 0, "today_close")
        today_open_info = self._get_history_data(data, index, 0, "today_open")
        if None == today_high_info or None == today_close_info or None == today_open_info:
            return (False, "get today(%d) today_high today_close today_open error" % index)

        today_close = float(today_close_info[1])
        today_high = float(today_high_info[1])
        today_high_sub_close = today_high - today_close
        today_close_sub_open = today_close - float(today_open_info[1])
        #if 0 == today_close_sub_open:
        #    return False
        #
        #if today_high_sub_close / today_close_sub_open > close_offset_max:
        #    return False
        
        today_volume_info = self._get_history_data(data, index, 0, "volume")
        today_volume_ma5_info = self._get_history_data(data, index, 0, "volume_ma5")
        today_volume_ma10_info = self._get_history_data(data, index, 0, "volume_ma10")
        if None == today_volume_info or None == today_volume_ma5_info or None == today_volume_ma10_info:
            return (False, "get today(%d) volume volume_ma5 volume_ma10 error" % index)

        today_volume = float(today_volume_info[1])
        today_volume_ma5 = float(today_volume_ma5_info[1])
        today_volume_ma10 = float(today_volume_ma10_info[1])
        
        if today_volume < index_volume_times_than_max * today_volume_ma5:
            return (False, "today(%d) vol(%f) is not %f times vol_ma5(%f)" % (index, today_volume, index_volume_times_than_max, today_volume_ma5))
        if today_volume < index_volume_times_than_max * today_volume_ma10:
            return (False, "today(%d) vol(%f) is not %f times vol_ma10(%f)" % (index, today_volume, index_volume_times_than_max, today_volume_ma10))
        
        today_close_ma5_info = self._get_history_data(data, index, 0, "close_ma5")
        today_close_ma10_info = self._get_history_data(data, index, 0, "close_ma10")
        today_close_ma20_info = self._get_history_data(data, index, 0, "close_ma20")
        today_close_ma30_info = self._get_history_data(data, index, 0, "close_ma30")
        today_close_ma60_info = self._get_history_data(data, index, 0, "close_ma60")
        if None == today_close_ma5_info or None == today_close_ma10_info or None == today_close_ma20_info or None == today_close_ma30_info or None == today_close_ma60_info:
            return (False, "get today(%d) close ma5 ma10 ma20 ma30 ma60 error" % index)

        today_close_ma5 = today_close_ma5_info[1]
        today_close_ma10 = today_close_ma10_info[1]
        today_close_ma20 = today_close_ma20_info[1]
        today_close_ma30 = today_close_ma30_info[1]
        today_close_ma60 = today_close_ma60_info[1]

        if today_close_ma30 > today_close_ma20 and today_close_ma30 > today_close_ma10 and today_close_ma30 > today_close_ma5:
            return (False, "today(%d) close ma30 is bigger than ma20 ma10 ma5" % index)

        if today_high < today_close_ma30 or today_high < today_close_ma20 or today_high < today_close_ma10 or today_high < today_close_ma5:
            return (False, "today(%d) high is not bigger than close ma30 ma20 ma10 ma5" % index)

        after_close_max_info = self._get_history_max_min_info(data, index, after_period, "today_close", include_today = False, pre = False, max=True)
        if None == after_close_max_info:
            return (False, "today is last day")

        after_close_max = after_close_max_info[1]
        if today_close > after_close_max:
            return (False, "today is highest in %d days" % (period))

        today_low_info = self._get_history_data(data, index, 0, "today_low")
        if None == today_low_info:
            return (False, "get today(%d) low error" % (index))
        today_low = today_low_info[1]

        yesterday_close_info = self._get_history_data(data, index, 0, "yesterday_close")
        if None == yesterday_close_info:
            yesterday_close = yesterday_close_info[1]
        
        ##############################################################################
        wave_include_ma = 0
        if today_close_ma5 > today_low and today_close_ma5 < today_high:
            wave_include_ma = wave_include_ma + 1
        
        if today_close_ma10 > today_low and today_close_ma10 < today_high:
            wave_include_ma = wave_include_ma + 1

        if today_close_ma30 > today_low and today_close_ma30 < today_high:
            wave_include_ma = wave_include_ma + 1
        
        if today_close_ma60 > today_low and today_close_ma60 < today_high:
            wave_include_ma = wave_include_ma + 1
        
        if wave_include_ma < 1:
            return (False, "waves include %d ma" % (wave_include_ma))
        
        #return (True, "%s %s" % (polyfit_30, polyfit_60))
        return (True, "")

    def _polyfit_close(self, data, index, type, period):
        #volume_max_info = self._get_history_max_min_info(data, index, period, "volume", include_today = False, pre = True, max=True)
        #if None == volume_max_info:
        #    return "get today(%d) pre %d volume error" % (index, period)
        #period = index - volume_max_info[0]

        all_pre_datas = []
        all_pre_indexs = []
        first = 0
        for pre_index in range(period):
            new_index = index - pre_index
            if new_index < 0:
                break
            pre_data_info = self._get_history_data(data, new_index, 0, type)
            if None == pre_data_info:
                break
            pre_data = pre_data_info[1]
            if first == 0:
                first = pre_data
            pre_data = pre_data/first
            all_pre_datas.insert(0, pre_data)
            all_pre_indexs.append(pre_index + 1)
        all_pre_datas_np = numpy.array(all_pre_datas)
        all_pre_indexs_np = numpy.array(all_pre_indexs)
        result = numpy.polyfit(all_pre_datas_np, all_pre_indexs_np, 1)
        return result

    def _filter_indexs(self, data, indexs):
        filter_indexs = []
        buy_info = []
        not_buy_info = []
        for index_info in indexs:
            index = index_info[0]
            today_close_ma5_info = self._get_history_data(data, index, 0, "close_ma5")
            today_close_ma10_info = self._get_history_data(data, index, 0, "close_ma10")
            today_close_ma20_info = self._get_history_data(data, index, 0, "close_ma20")
            today_close_ma30_info = self._get_history_data(data, index, 0, "close_ma30")
            today_close_ma60_info = self._get_history_data(data, index, 0, "close_ma60")
            today_close_ma120_info = self._get_history_data(data, index, 0, "close_ma120")
            if None == today_close_ma5_info or None == today_close_ma10_info or None == today_close_ma20_info or None == today_close_ma30_info or None == today_close_ma60_info or None == today_close_ma120_info:
                not_buy_info.append((index, "get day(%d) close ma5 ma10 ma20 ma30 ma60 ma120 error" % (index)))
                continue

            today_close_ma5 = today_close_ma5_info[1]
            today_close_ma10 = today_close_ma10_info[1]
            today_close_ma20 = today_close_ma20_info[1]
            today_close_ma30 = today_close_ma30_info[1]
            today_close_ma60 = today_close_ma60_info[1]
            today_close_ma120 = today_close_ma120_info[1]
            
            tomorrow_high_info = self._get_history_data(data, index, 1, "today_high", pre=False)
            tomorrow_low_info = self._get_history_data(data, index, 1, "today_low", pre=False)
            tomorrow_close_info = self._get_history_data(data, index, 1, "today_close", pre=False)
            tomorrow_pchg_info = self._get_history_data(data, index, 1, "pchg", pre=False)
            if None == tomorrow_high_info or None == tomorrow_low_info or None == tomorrow_close_info or None == tomorrow_pchg_info:
                not_buy_info.append((index, "get day(%d) high low close pchg error" % (index+1)))
                continue
            
            tomorrow_close = float(tomorrow_close_info[1])
            tomorrow_high = float(tomorrow_high_info[1])
            tomorrow_low = float(tomorrow_low_info[1])
            tomorrow_pchg = float(tomorrow_pchg_info[1])
            
            today_close_info = self._get_history_data(data, index, 0, "today_close")
            today_high_info = self._get_history_data(data, index, 0, "today_high")
            today_low_info = self._get_history_data(data, index, 0, "today_low")
            if None == today_close_info or None == today_high_info or None == today_low_info:
                not_buy_info.append((index, "get day(%d) close high low error" % (index)))
                continue

            today_close = today_close_info[1]
            today_high = today_high_info[1]
            today_low = today_low_info[1]
            
            #up_waves = 0

            #if today_close_ma5 < today_close:
            #    up_waves = up_waves + 1

            #if today_close_ma10 < today_close:
            #    up_waves = up_waves + 1
            
            #if today_close_ma20 < today_close:
            #    up_waves = up_waves + 1
            
            #if today_close_ma30 < today_close:
            #    up_waves = up_waves + 1
            
            #if today_close_ma60 < today_close:
            #    up_waves = up_waves + 1
            
            #if today_close_ma120 < today_close:
            #    up_waves = up_waves + 1

            #if up_waves > 4:
            #    if tomorrow_close > today_close:
            #        polyfit_5 = self._polyfit_close(data, index, "close_ma5", 5)
            #        polyfit_10 = self._polyfit_close(data, index, "close_ma10", 5)
            #        polyfit_20 = self._polyfit_close(data, index, "close_ma20", 5)
            #        buy_info.append((index, "buy pointer from index(%d) %s %s %s" %(index, polyfit_5, polyfit_10, polyfit_20)))
            #        continue

            low_day_index = 0
            for after_index in range(1, 11):
                after_close_info = self._get_history_data(data, index, after_index, "today_close", pre=False)
                if after_close_info != None:
                    after_close = after_close_info[1]
                    if after_close < today_close:
                        low_day_index = after_close_info[0]
                        break
            
            if low_day_index == 0:
                not_buy_info.append((index, "get after day(%d) low day index error" % (index)))
                continue

            reback_day_index = 0
            for after_index in range(1,11):
                after_close_info = self._get_history_data(data, low_day_index, after_index, "today_close", pre=False)
                if after_close_info != None:
                    after_close = after_close_info[1]
                    if after_close >= today_close:
                        reback_day_index = after_close_info[0]
                        break
            
            if reback_day_index == 0:
                not_buy_info.append((index, "get after day(%d) reback day index error" % (index)))
                continue

            #polyfit_5 = self._polyfit_close(data, reback_day_index, "close_ma5", 5)
            #polyfit_10 = self._polyfit_close(data, reback_day_index, "close_ma10", 5)
            #polyfit_20 = self._polyfit_close(data, reback_day_index, "close_ma20", 5)
            
            cmp_index_volume_info = self._get_history_data(data, index, 0, "volume")
            reback_index_volume_info = self._get_history_data(data, reback_day_index, 0, "volume")
            if None == cmp_index_volume_info or None == reback_index_volume_info:
                not_buy_info.append((index, "get %d %d volume info error" % (index, reback_day_index)))
                continue

            if cmp_index_volume_info[1] > reback_index_volume_info[1]:
                not_buy_info.append((index, "%d day volume(%f) is bigger than %d day volume(%f)" % (index, cmp_index_volume_info[1], reback_day_index, reback_index_volume_info[1])))
                continue

            #buy_info.append((reback_day_index, "buy pointer from index(%d) low(%d) %s %s %s" % (index, low_day_index, polyfit_5, polyfit_10, polyfit_20)))
            buy_info.append((reback_day_index, "buy pointer from index(%d) low(%d)" % (index, low_day_index)))
            continue

        return (buy_info, not_buy_info)
#            yesterday_high_info = self._get_history_data(data, index, 1, "today_high", pre=True)
#            yesterday_low_info = self._get_history_data(data, index, 1, "today_low", pre=True)
#            yesterday_close_info = self._get_history_data(data, index, 1, "today_close", pre=True)
#            if None == yesterday_high_info or None == yesterday_low_info or None == yesterday_close_info:
#                continue
#
#            yesterday_close = float(yesterday_close_info[1])
#            yesterday_high = float(yesterday_high_info[1])
#            yesterday_low = float(yesterday_low_info[1])
#            if yesterday_close == yesterday_high and yesterday_close == yesterday_low:
#                continue
#
#            tomorrow_high_info = self._get_history_data(data, index, 1, "today_high", pre=False)
#            tomorrow_low_info = self._get_history_data(data, index, 1, "today_low", pre=False)
#            tomorrow_close_info = self._get_history_data(data, index, 1, "today_close", pre=False)
#            tomorrow_pchg_info = self._get_history_data(data, index, 1, "pchg", pre=False)
#            if None == tomorrow_high_info or None == tomorrow_low_info or None == tomorrow_close_info or None == tomorrow_pchg_info:
#                continue
#
#            today_close_info = self._get_history_data(data, index, 0, "today_close")
#            today_high_info = self._get_history_data(data, index, 0, "today_high")
#            today_low_info = self._get_history_data(data, index, 0, "today_low")
#            if None == today_close_info or None == today_high_info or None == today_low_info:
#                continue
#
#            today_close = float(today_close_info[1])
#            today_high = float(today_high_info[1])
#            today_low = float(today_low_info[1])
#            if today_high > yesterday_high and today_low < yesterday_low:
#                continue
#            
#            up_down_per = 0.5
#
#            tomorrow_close = float(tomorrow_close_info[1])
#            tomorrow_high = float(tomorrow_high_info[1])
#            tomorrow_low = float(tomorrow_low_info[1])
#            tomorrow_pchg = float(tomorrow_pchg_info[1])
#            if tomorrow_pchg > 0:
#                tomorrow_high_sub_close = tomorrow_high - tomorrow_close
#                tomorrow_close_sub_today_close = tomorrow_close - today_close
#                if tomorrow_close_sub_today_close != 0:
#                    if tomorrow_high_sub_close/tomorrow_close_sub_today_close > up_down_per:
#                        continue
#            
#            filter_indexs.append(index)
#
#        return filter_indexs

    def _get_cmp_indexs(self, data):
        mismatch_indexs_info = []
        match_indexs = []
        for index in range(len(data)):
            index_info = self._check_cmp_cond(data, index)
            index_new_info = (index, index_info[1])
            if index_info[0]:
                match_indexs.append(index_new_info)
            else:
                mismatch_indexs_info.append(index_new_info)
        
        return (match_indexs, mismatch_indexs_info)

    def _get_average_info(self, share_id, table_name, period = 0):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        periods = [5, 10, 20, 30, 60, 120]
        types = ["close_ma", "volume_ma"]
        columns = ["time", "today_close", "today_high", "today_low", "today_open", "yesterday_close", "pchg", "turnover_rate", "volume", "turnover"]
        for type_item in types:
            for period_item in periods:
                column_name = "%s%d" % (type_item, period_item)
                columns.append(column_name)

        extend_str = "order by time desc"
        if period > 0:
            extend_str = "%s limit %d" % (extend_str, period)

        data= fetch_data.get_data(fetch_data.select_db(conn_name, table_name, columns, {}, extend=extend_str))
        infos = []
        for data_item in data:
            info = {}
            for index in range(len(columns)):
                    info[columns[index]] = data_item[index]
            infos.insert(0, info)
        return infos

    def _get_start_time(self, share_id, table_name, ma_empty_start_time):
        stock_conn_manager_obj = stock_conn_manager()
        conn_name = stock_conn_manager_obj.get_conn_name(share_id)
        last_time = fetch_data.get_data(fetch_data.select_db(conn_name, table_name, ["time"], {"time":[ma_empty_start_time, "<="]}, extend="order by time desc limit 120"))
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
        table_name = "daily_info_ex_dividend_%s" % (share_id)
        infos = self._calc_average_data(share_id, table_name)
        for item in infos:
            self._save_data(share_id, table_name, item)

    def _calc_average_data(self, share_id, table_name):
        ma_empty_start_time_int = self._get_ma_empty_start_time(share_id, table_name)
        if ma_empty_start_time_int == 0:
            return []
        start_time_int = self._get_start_time(share_id, table_name, ma_empty_start_time_int)
        stock_info = self._get_close_volume(share_id, table_name, start_time_int)
        periods = [5, 10, 20, 30, 60, 90, 120, 180]
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

    a = second_buy_point()
    a.run()
