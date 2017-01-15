#coding=utf-8
#-*- coding: utf-8 -*-

import os
import sys
sys.path.append("../frame/")

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from mysql_manager import mysql_manager

class prepare_table():
    def __init__(self, conn_name, table_template_name):
        self._conn_name = conn_name
        self._table_template_name = table_template_name
        self._create_table_format = ""

    def prepare(self, table_name):
        self._create_table_if_not_exist(table_name)
    
    def _get_table_template(self):
        file_path = "./conf/table_template/" + self._table_template_name + ".ttpl"
        if False == os.path.isfile(file_path):
            LOG_WARNING("can't read %s" %(file_path))
            return

        fobj = open(file_path)
        try:
             self._create_table_format = fobj.read()
        except:
            self._create_table_format = ""
            LOG_WARNING("get table_template file error.path: %s" % (file_path))
        finally:
            fobj.close()

    def _create_table_if_not_exist(self, table_name):
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(self._conn_name)
        if False == conn.has_table(table_name):
            if len(self._create_table_format) == 0:
                self._get_table_template()
            if len(self._create_table_format) == 0:
                return
            sql = self._create_table_format % (table_name)
            data = conn.execute(sql)
            conn.refresh_tables_info()

if __name__ == "__main__":
    import os
    os.chdir("../../")
    sys.path.append("./src/frame/")
    
    import sys
    reload(sys)
    sys.setdefaultencoding("utf8")

    from j_load_mysql_conf import j_load_mysql_conf
    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")

    j_load_mysql_conf_obj = j_load_mysql_conf()
    j_load_mysql_conf_obj.run()

    a = prepare_table("daily_temp", "today_market_maker")
    a.prepare("test")
