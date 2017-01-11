import os
import re
import sys
import time
import urllib2
sys.path.append("../frame/")

from loggingex import LOG_INFO
from loggingex import LOG_ERROR
from loggingex import LOG_WARNING

from mysql_manager import mysql_manager
from singleton import singleton

@singleton
class stock_conn_manager():
    def __init__(self):
        pass

    def get_conn(self, share_id):
        share_id_int = int(share_id)
        share_id_part_no = share_id_int % 300
        conn_name = "stock_part_%d" % (share_id_part_no)
        db_manager = mysql_manager()
        conn = db_manager.get_mysql_conn(conn_name)
        return conn

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

    a = stock_conn_manager()
    print a.get_conn("600435")
