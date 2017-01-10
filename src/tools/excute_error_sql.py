#coding=utf-8
#-*- coding: utf-8 -*-
import os
os.chdir("../../")
import sys
sys.path.append("./src/frame/")

import re
import MySQLdb
    
import sys
reload(sys)
sys.setdefaultencoding("utf8")

from mysql_manager import mysql_manager
from j_load_mysql_conf import j_load_mysql_conf
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

def run(file_path, regular, conn_name):
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
    frame_conf_inst = scheduler_frame_conf_inst()
    j_load_mysql_conf_obj = j_load_mysql_conf()
    j_load_mysql_conf_obj.run()
    f = open(file_path)
    while True:
        line = f.readline()
        if not line:
            break
        data = re.findall(regular, line)
        for item in data:
            #print item
            db_manager = mysql_manager()
            conn = db_manager.get_mysql_conn(conn_name)
            conn.excute(item)

if __name__ == "__main__":
    run("./log/nomal.log.wec_bak", "(insert into market_maker_2017_01_10.*\)) execute error", "daily_temp")
    #run("./src/tools/test.dat", "(insert into market_maker_2017_01_10.*\)) execute error", "daily_temp")
    
