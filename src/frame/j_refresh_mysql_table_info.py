import json

import frame_tools

from job_base import job_base
from singleton import singleton
from mysql_manager import mysql_manager
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

from loggingex import LOG_INFO
from loggingex import LOG_DEBUG

@singleton
class j_refresh_mysql_table_info(job_base):
    def __init__(self):
        self._mysql_manager = mysql_manager()

    def run(self):
        self._mysql_manager.refresh_all_conns_tables_info()

if __name__ == "__main__":
    a = mysql_manager()
    test_data_1 = {"a1":{"host":"127.0.0.1", "port":123, "user":"fangliang", "passwd":"fl_pwd", "db":"db1", "charset":"utf8"}}
    a.add_conns(test_data_1)
    b = j_refresh_mysql_table_info()
    b.run()
