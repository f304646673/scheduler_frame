import json

import frame_tools
from job_base import job_base
from singleton import singleton
from regular_split_manager import  regular_split_manager
from scheduler_frame_conf_inst import scheduler_frame_conf_inst
from regulars_manager_conf_parser import regulars_manager_conf_parser

from loggingex import LOG_INFO
from loggingex import LOG_DEBUG
from loggingex import LOG_WARNING
@singleton
class j_load_regular_conf(job_base):
    def __init__(self):
        self._pre_regulars_info = {}
        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._regulars_manager = regular_split_manager()

    def run(self):
        section_name = "regulars"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        
        regulars_manager_conf_parser_obj = regulars_manager_conf_parser()
        regulars_info = regulars_manager_conf_parser_obj.parse(conf_path)
        self._reload_regulars(regulars_info)

    def _reload_regulars(self, regulars_info):
        add_dict = {}
        remove_dict = {}
        modify_dict = {}

        frame_tools.dict_diff(regulars_info, self._pre_regulars_info, add_dict, remove_dict, modify_dict)
        LOG_INFO("add dict %s" % (json.dumps(add_dict)))
        LOG_INFO("remove dict %s" % (json.dumps(remove_dict)))
        LOG_INFO("modify dict %s" % (json.dumps(modify_dict)))
        
        if 0 == len(add_dict) and 0 == len(remove_dict) and 0 == len(modify_dict):
            return

        self._pre_regulars_info = regulars_info

        self._regulars_manager.remove_regulars(remove_dict)
        self._regulars_manager.add_regulars(add_dict)
        self._regulars_manager.modify_regulars(modify_dict)

if __name__ == "__main__":
    import os
    os.chdir("../../")
    
    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
    
    j_load_regular_conf_obj = j_load_regular_conf()
    j_load_regular_conf_obj.run()

    #a = j_load_regular_conf()
    #test_data_1 = {"a":"b", "c":"d"}
    #a._reload_regulars(test_data_1)
    #print "******************************"
    
    #test_data_2 = {"a":"b", "c":"d"}
    #a._reload_regulars(test_data_2)
    #print "******************************"
    
    #test_data_3 = {"c":"d"}
    #a._reload_regulars(test_data_3)
    #print "******************************"
    
    #test_data_4 = {"a":"b", "c":"d"}
    #a._reload_regulars(test_data_4)
    #print "******************************"
    
    #test_data_5 = {"a":"b", "c":"d", "e":"f"}
    #a._reload_regulars(test_data_5)
    #print "******************************"
    
    #test_data_6 = {"a":"g", "c":"d", "e":"f"}
    #a._reload_regulars(test_data_6)
    #print "******************************"
