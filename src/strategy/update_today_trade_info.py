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
from regular_split_manager import regular_split_manager

class update_today_trade_info(job_base):
    def __init__(self):
        self._regular_split_manager = regular_split_manager()
        self._db_manager = mysql_manager()
        self._conn_name = "daily_temp"
        self._base_conn_name = "stock_db"
        self._pre_share_count = 0
        self._share_ids_str_list = []
        self._create_table_format = """
            CREATE TABLE `%s` (
              `seq_id` bigint(64) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
              `share_id` char(6) CHARACTER SET latin1 NOT NULL DEFAULT '000000',
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
              `time_date_str` varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT '更新时间（日期）',
              `time_str` varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT '更新时间（时间）',
              `empty` int(16) NOT NULL DEFAULT '0' COMMENT '空占位',
              PRIMARY KEY (`seq_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='今日交易数据';
        """
    
    def run(self):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "trade_info_%s" % (date_info)
        self._create_table_if_not_exist(table_name)
        
        ids_str_list = self._get_all_share_ids()
        for ids_str in ids_str_list:
            data = self._get_data(ids_str)
            self._parse_data_and_insert_db(table_name, data)
        LOG_INFO("run update_today_trade_info")

    def  _create_table_if_not_exist(self, table_name):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        if False == conn.has_table(table_name):
            sql = self._create_table_format % (table_name)
            conn.excute(sql)
            conn.refresh_tables_info()

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
        count_per = 1 #500
        page = id_count/count_per
        ids_str_list = []
        for index in range(page):
            ids_list = ids[index*count_per:(index+1)*count_per]
            ids_str = "," . join(ids_list)
            ids_str_list.append(ids_str)
        self._share_ids_str_list = ids_str_list
        self._pre_share_count = count
        return self._share_ids_str_list


    def _get_data(self, ids):
        date_info = time.strftime('%Y-%m-%d')
        url_pre = "http://hq.sinajs.cn/list="
        url = url_pre + ids
        res = ""
        tried = False
        while True:
            try:
                req = urllib2.Request(url)
                res_data = urllib2.urlopen(req)
                res = res_data.read()
                break
            except Exception as e:
                LOG_ERROR("request error: %s"  % e)
                if tried:
                    break
                else:
                    tried = True
        return res

    def _parse_data_and_insert_db(self, table_name, data):
        ret_array = self._regular_split_manager.get_split_data(data, "hq_sinajs_cn_list")
        if 0 == len(ret_array):
            LOG_WARNING("hg_sinajs_cn_list regular %s empty data" %(data))
            return

        data_array = []

        into_db_columns = ["share_id","today_open","yesteday_close","cur","today_high","today_low","compete_buy_price","compete_sale_price",
                "trade_num","trade_price","buy_1_num","buy_1_price","buy_2_num","buy_2_price","buy_3_num","buy_3_price","buy_4_num","buy_4_price",
                "buy_5_num","buy_5_price","sale_1_num","sale_1_price","sale_2_num","sale_2_price","sale_3_num","sale_3_price","sale_4_num","sale_4_price",
                "sale_5_num","sale_5_price","time_date_str","time_str","empty"]
        columns_count = len(into_db_columns)

        for item in ret_array:
            if len(item) < columns_count:
                continue
            del item[1]
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
