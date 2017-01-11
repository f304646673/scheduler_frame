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

from job_base import job_base
from mysql_manager import mysql_manager

from stock_conn_manager import stock_conn_manager

class bak_today_trade(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._base_conn_name = "stock_db"
        self._daily_temp_conn_name = "daily_temp"

        self._create_table_format = """
            CREATE TABLE `%s` (
              `time` bigint(64) NOT NULL COMMENT '数据时间',
              `today_open` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '今日开盘价',
              `yesteday_close` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '昨天收盘价',
              `cur` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '当前价格',
              `today_high` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '今日最高',
              `today_low` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '今日最低',
              `compete_buy_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '竞买价格（买一价格）',
              `compete_sale_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '竞卖价格（卖一价格）',
              `trade_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '成交的股票数',
              `trade_price` float(32,2) NOT NULL DEFAULT '0.00' COMMENT '成交的金额',
              `buy_1_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '买一数量',
              `buy_1_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '买一价格',
              `buy_2_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '买二数量',
              `buy_2_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '买二价格',
              `buy_3_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '买三数量',
              `buy_3_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '买三价格',
              `buy_4_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '买四数量',
              `buy_4_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '买四价格',
              `buy_5_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '买五数量',
              `buy_5_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '买五价格',
              `sale_1_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '卖一数量',
              `sale_1_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '卖一价格',
              `sale_2_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '卖二数量',
              `sale_2_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '卖二价格',
              `sale_3_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '卖三数量',
              `sale_3_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '卖三价格',
              `sale_4_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '卖四数量',
              `sale_4_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '卖四价格',
              `sale_5_num` bigint(64) NOT NULL DEFAULT '0' COMMENT '卖五数量',
              `sale_5_price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '卖五价格',
              PRIMARY KEY (`time`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='今日交易数据';
        """
    
    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id  in share_ids:
            self._bak_market_maker_info(share_id[0])
        LOG_INFO("run bak_today_trade")
    
    def _bak_market_maker_info(self, share_id):
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

        share_market_maker_table_name = "trade_info_detail_" +share_id
        self._create_table_if_not_exist(share_id, share_market_maker_table_name)

        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        conn.insert_data(share_market_maker_table_name, keys_array, daily_data_list)

    def _get_all_share_ids(self):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._base_conn_name)
        #share_ids = conn.select("share_base_info", ["share_id"],{"share_id":"000001"})
        share_ids = conn.select("share_base_info", ["share_id"],{})
        return share_ids

    def  _create_table_if_not_exist(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        if False == conn.has_table(table_name):
            sql = self._create_table_format % (table_name)
            conn.execute(sql)
            conn.refresh_tables_info()

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
