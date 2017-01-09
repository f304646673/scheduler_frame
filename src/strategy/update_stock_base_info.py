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
    
    def run(self):
        data = self._get_data()
        self._parse_data(data)
        LOG_INFO("run update_stock_base_info")

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

    def _parse_data(self, data):
        data_array = self._regular_split_manager.get_split_data(data, "string_comma_regular")
        for item in data_array:
            share_id = item[1]
            share_name = item[2]
            if len(share_id) > 0 and len(share_name) > 0:
                share_info = {"share_id":share_id, "share_name":share_name}
                conn = self._db_manager.get_mysql_conn("stock_db")
                conn.insert_onduplicate("share_base_info", share_info, ["share_id"])


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

    a = update_stock_base_info()
    a.run()
