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

def drop_all_tables():
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
        drop_table(conn_info["host"],int(conn_info["port"]),conn_info["user"],conn_info["passwd"],conn_info["db"])

def drop_table(host_name, port_num, user_name, password, db_name):
    conn = None
    cursor = None
    try:
        conn = MySQLdb.connect(host=host_name, port=port_num, user=user_name, passwd=password)
        cursor = conn.cursor()
        sql = """drop database %s""" %(db_name)
        cursor.execute(sql)
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
    drop_all_tables()
