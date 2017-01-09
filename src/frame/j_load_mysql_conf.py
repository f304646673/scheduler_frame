import json

import frame_tools

from job_base import job_base
from singleton import singleton
from mysql_manager import mysql_manager
from mysql_conf_parser import mysql_conf_parser
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

from loggingex import LOG_INFO
from loggingex import LOG_DEBUG
from loggingex import LOG_WARNING

@singleton
class j_load_mysql_conf(job_base):
    def __init__(self):
        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._mysql_manager = mysql_manager()
        self._pre_mysql_conf_info = {}

    def run(self):
        section_name = "mysql_manager"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        
        mysql_conf_parser_obj = mysql_conf_parser()
        mysql_conf_info = mysql_conf_parser_obj.parse(conf_path)
        self._execute_mysql_conn(mysql_conf_info)

    def _execute_mysql_conn(self, mysql_conf_info):
        add_dict = {}
        remove_dict = {}
        modify_dict = {}

        frame_tools.dict_diff(mysql_conf_info, self._pre_mysql_conf_info, add_dict, remove_dict, modify_dict)

        LOG_INFO("add mysql conf %s" % (json.dumps(add_dict)))
        LOG_INFO("modify mysql conf %s" % (json.dumps(modify_dict)))
        LOG_INFO("remove mysql conf %s" % (json.dumps(remove_dict)))
    
        if 0 == len(add_dict) and 0 == len(modify_dict) and 0 == len(remove_dict):
            return

        self._pre_mysql_conf_info = mysql_conf_info
        print self._pre_mysql_conf_info
        
        self._mysql_manager.remove_conns(remove_dict)
        self._mysql_manager.add_conns(add_dict)
        self._mysql_manager.modify_conns(modify_dict)

if __name__ == "__main__":
    a = j_load_mysql_conf()
    test_data_1 = {"a":"b", "c":"d"}
    a._execute_mysql_conn(test_data_1)
    print "******************************"
    
    test_data_2 = {"a":"b", "c":"d"}
    a._execute_mysql_conn(test_data_2)
    print "******************************"
    
    test_data_3 = {"c":"d"}
    a._execute_mysql_conn(test_data_3)
    print "******************************"
    
    test_data_4 = {"a":"b", "c":"d"}
    a._execute_mysql_conn(test_data_4)
    print "******************************"
    
    test_data_5 = {"a":"b", "c":"d", "e":"f"}
    a._execute_mysql_conn(test_data_5)
    print "******************************"
    
    test_data_6 = {"a":"g", "c":"d", "e":"f"}
    a._execute_mysql_conn(test_data_6)
    print "******************************"
