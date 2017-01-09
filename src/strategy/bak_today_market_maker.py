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

class bak_today_market_maker(job_base):
    def __init__(self):
        self._db_manager = mysql_manager()
        self._base_conn_name = "stock_db"
        self._daily_temp_conn_name = "daily_temp"
        self._market_maker_conn_name = "market_maker"

        self._create_table_format = """
            CREATE TABLE `%s` (
              `time` bigint(64) NOT NULL COMMENT '数据时间',
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
              PRIMARY KEY (`time`)
            ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='主力行为统计';
        """
    
    def run(self):
        share_ids = self._get_all_share_ids()
        for share_id  in share_ids:
            self._bak_market_maker_info(share_id[0])
        LOG_INFO("run bak_today_market_maker")
    
    def _bak_market_maker_info(self, share_id):
        date_info = time.strftime('%Y_%m_%d')
        table_name = "market_maker_%s" % (date_info)
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._daily_temp_conn_name)
        
        fields_array =["time_str", "price", "up_percent", "market_maker_net_inflow", "market_maker_net_inflow_per",
                "huge_inflow", "huge_inflow_per", "large_inflow", "large_inflow_per", "medium_inflow", "medium_inflow_per", "small_inflow", "small_inflow_per"]
        daily_data = conn.select(table_name, fields_array, {"share_id":share_id})
        self._bak_single_market_maker_info(share_id, daily_data)

    def _bak_single_market_maker_info(self, share_id, daily_data):
        daily_data_list = []
        for item in daily_data:
            item_list = list(item)
            date_str = item[0]
            date_int = time.mktime(time.strptime(date_str,'%Y-%m-%d %H:%M:%S'))
            item_list[0] = date_int
            daily_data_list.append(item_list)
        keys_array =["time", "price", "up_percent", "market_maker_net_inflow", "market_maker_net_inflow_per",
                "huge_inflow", "huge_inflow_per", "large_inflow", "large_inflow_per", "medium_inflow", "medium_inflow_per", "small_inflow", "small_inflow_per"]

        share_market_maker_table_name = "stock_" +share_id
        self._create_table_if_not_exist(share_id, share_market_maker_table_name)

        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        conn.insert_data(share_market_maker_table_name, keys_array, daily_data_list)

    def _get_all_share_ids(self):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._base_conn_name)
        #share_ids = conn.select("share_base_info", ["share_id"],{})
        share_ids = conn.select("share_base_info", ["share_id"],{"share_id":"600435"})
        return share_ids

    def  _create_table_if_not_exist(self, share_id, table_name):
        stock_conn_manager_obj = stock_conn_manager()
        conn = stock_conn_manager_obj.get_conn(share_id)
        print share_id, table_name, conn
        if False == conn.has_table(table_name):
            sql = self._create_table_format % (table_name)
            conn.excute(sql)
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
