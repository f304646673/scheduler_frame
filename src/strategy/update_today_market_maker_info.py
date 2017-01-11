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

class update_today_market_maker_info(job_base):
    def __init__(self):
        self._regular_split_manager = regular_split_manager()
        self._db_manager = mysql_manager()
        self._conn_name = "daily_temp"

        self._create_table_format = """
            CREATE TABLE `%s` (
              `seq_id` bigint(64) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
              `market_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1 沪市 2 深市',
              `share_id` char(6) CHARACTER SET latin1 NOT NULL DEFAULT '' COMMENT '股票代码',
              `share_name` varchar(36) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT '股票名称',
              `price` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '股票价格',
              `up_percent` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '涨幅',
              `market_maker_net_inflow` float(64,2) NOT NULL DEFAULT '0.00' COMMENT '主力净流入',
              `market_maker_net_inflow_per` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '主力净流入占比',
              `huge_inflow` float(64,2) NOT NULL DEFAULT '0.00' COMMENT '超大单净流入',
              `huge_inflow_per` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '超大单净流入占比',
              `large_inflow` float(64,2) NOT NULL DEFAULT '0.00' COMMENT '大单净流入',
              `large_inflow_per` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '大单净流入占比',
              `medium_inflow` float(64,2) NOT NULL DEFAULT '0.00' COMMENT '中单净流入',
              `medium_inflow_per` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '中单净流入占比',
              `small_inflow` float(64,2) NOT NULL DEFAULT '0.00' COMMENT '小单净流入',
              `small_inflow_per` float(16,2) NOT NULL DEFAULT '0.00' COMMENT '小单净流入占比',
              `time_str` varchar(64) CHARACTER SET latin1 NOT NULL DEFAULT '' COMMENT '时间',
              PRIMARY KEY (`seq_id`)
            ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='主力行为统计';
        """
    
    def run(self):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "market_maker_%s" % (date_info)
        self._create_table_if_not_exist(table_name)

        data = self._get_data()
        data =data.replace("-,", "0,")
        self._parse_data_and_insert_db(table_name, data)
        LOG_INFO("run update_today_market_maker_info")

    def  _create_table_if_not_exist(self, table_name):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        if False == conn.has_table(table_name):
            sql = self._create_table_format % (table_name)
            conn.execute(sql)
            conn.refresh_tables_info()

    def _get_data(self):
        date_info = time.strftime('%Y-%m-%d')
        url_fomart = "http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(BalFlowMain)&sr=-1&p=3&ps=%d&js=var%%20vDUaFOen={pages:(pc),date:%%22%s%%22,data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITA&rt=49430148"
        url = format(url_fomart % (3500, date_info))
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
        data_array = self._regular_split_manager.get_split_data(data, "string_comma_regular")
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
