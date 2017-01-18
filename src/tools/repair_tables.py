#coding=utf-8
#-*- coding: utf-8 -*-

import os
import re
import sys
import time
import MySQLdb
import urllib2
sys.path.append("../frame/")

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from mysql_conf_parser import mysql_conf_parser
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

def repair_all_tables(table_name, sql_tpl):
    section_name = "mysql_manager"
    option_name = "conf_path"
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
    if False == frame_conf_inst.has_option(section_name, option_name):
        LOG_WARNING("no %s %s" % (section_name, option_name))
        return
    conf_path = frame_conf_inst.get(section_name, option_name)
    
    mysql_conf_parser_obj = mysql_conf_parser()
    mysql_conf_info = mysql_conf_parser_obj.parse(conf_path)
    for conn_name, conn_info in mysql_conf_info.items():
        repair_table(conn_info["host"],int(conn_info["port"]),conn_info["user"],conn_info["passwd"],conn_info["db"], table_name, sql_tpl)

def repair_table(host_name, port_num, user_name, password, db_name, table_name ,sql_tpl):
    conn = None
    cursor = None
    try:
        conn = MySQLdb.connect(host=host_name, port=port_num, user=user_name, passwd=password, db = db_name)
        cursor = conn.cursor()
        sql = """show tables"""
        cursor.execute(sql)
        tables_name = cursor.fetchall()
        for table_name_item in tables_name:
            table_name_in_db = table_name_item[0]
            if table_name not in table_name_in_db:
                continue
            new_sql = sql_tpl % table_name_in_db
            print new_sql
            data = cursor.execute(new_sql)
            conn.commit()
            print data
    except MySQLdb.Error, e :
        LOG_WARNING("%s execute error %s" % (sql, str(e)))
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except:
            pass

if __name__ == "__main__":
    import os
    os.chdir("../../")
    sys.path.append("./src/frame/")
    
    import sys
    reload(sys)
    sys.setdefaultencoding("utf8")
    #repair_all_tables("daily_info_" , "delete from %s where volume = 0")
    #repair_all_tables("daily_info_" , "DROP TABLE IF EXISTS %s")

#    modify = """
#        ALTER TABLE `%s`
#        ADD COLUMN `close_ma5`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价5日均值' AFTER `turnover`,
#        ADD COLUMN `close_ma10`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价10日均值' AFTER `close_ma5`,
#        ADD COLUMN `close_ma20`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价20日均值' AFTER `close_ma10`,
#        ADD COLUMN `close_ma30`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价30日均值' AFTER `close_ma20`,
#        ADD COLUMN `close_ma60`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价60日均值' AFTER `close_ma30`,
#        ADD COLUMN `close_ma90`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价90日均值' AFTER `close_ma60`,
#        ADD COLUMN `close_ma180`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价180日均值' AFTER `close_ma90`,
#        ADD COLUMN `volume_ma5`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量5日均值' AFTER `close_ma180`,
#        ADD COLUMN `volume_ma10`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量10日均值' AFTER `volume_ma5`,
#        ADD COLUMN `volume_ma20`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量20日均值' AFTER `volume_ma10`,
#        ADD COLUMN `volume_ma30`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量30日均值' AFTER `volume_ma20`,
#        ADD COLUMN `volume_ma60`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量60日均值' AFTER `volume_ma30`,
#        ADD COLUMN `volume_ma90`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量90日均值' AFTER `volume_ma60`,
#        ADD COLUMN `volume_ma180`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量180日均值' AFTER `volume_ma90`;
#    """

#    modify = """
#        ALTER TABLE `%s`
#        ADD COLUMN `close_ma120`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价120日均值' AFTER `close_ma90`,
#        ADD COLUMN `close_ma150`  float(16,2) NOT NULL DEFAULT 0 COMMENT '收盘价150日均值' AFTER `close_ma120`,
#        ADD COLUMN `volume_ma120`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量120日均值' AFTER `volume_ma90`,
#        ADD COLUMN `volume_ma150`  float(16,2) NOT NULL DEFAULT 0 COMMENT '成交量150日均值' AFTER `volume_ma120`;
#    """

    modify = """
        ALTER TABLE `%s`
        CHANGE COLUMN `yesteday_close` `yesterday_close`  float(16,2) NOT NULL DEFAULT 0.00 COMMENT '×ÌÊÅ' AFTER `today_open`;
    """
    #repair_all_tables("daily_info_" , modify)
    repair_all_tables("trade_info_" , modify)


