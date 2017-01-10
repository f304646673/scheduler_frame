#coding=utf-8
#-*- coding: utf-8 -*-

import os
import re
import sys
import urllib2
sys.path.append("../frame/")

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from job_base import job_base
from mysql_manager import mysql_manager
from regular_split_manager import regular_split_manager

class update_stock_base_info(job_base):
    def __init__(self):
        self._regular_split_manager = regular_split_manager()
        self._db_manager = mysql_manager()
        self._conn_name = "stock_db"
        self._table_name = "share_base_info"
        self._create_table_format = """
            CREATE TABLE `%s` (
              `share_id` char(6) COLLATE utf8_unicode_ci NOT NULL DEFAULT '000000' COMMENT '股票代码',
              `share_name` varchar(36) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT '股票名称',
              `market_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '市场 1 沪市 2 深市',
              PRIMARY KEY (`share_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='股票基本信息';
        """

    def run(self):
        data = self._get_data()
        self._create_table_if_not_exist()
        self._save_data(data)
        LOG_INFO("run update_stock_base_info")

    def  _create_table_if_not_exist(self):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        if False == conn.has_table(self._table_name):
            sql = self._create_table_format % (self._table_name)
            conn.excute(sql)
            conn.refresh_tables_info()

    def _get_data(self):
        url = r"http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx/JS.aspx?type=ct&st=(FFRank)&sr=1&p=1&ps=3500&js=var%20mozselQI={pages:(pc),data:[(x)]}&token=894050c76af8597a853f5b408b759f5d&cmd=C._AB&sty=DCFFITAM&rt=49461817"
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

    def _save_data(self, data):
        data_array = self._regular_split_manager.get_split_data(data, "string_comma_regular")
        for item in data_array:
            share_market_type = item[0]
            share_id = item[1]
            share_name = item[2]
            if len(share_id) > 0 and len(share_name) > 0:
                share_info = {"share_id":share_id, "share_name":share_name, "market_type":share_market_type}
                conn = self._db_manager.get_mysql_conn(self._conn_name)
                conn.insert_onduplicate(self._table_name, share_info, ["share_id"])


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

    a = update_stock_base_info()
    a.run()
