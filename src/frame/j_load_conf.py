import json

import frame_tools

from job_base import job_base
from singleton import singleton
from job_center import job_center
from job_conf_parser import job_conf_parser
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

from loggingex import LOG_INFO
from loggingex import LOG_DEBUG

class j_load_conf(job_base, singleton):

    def __init__(self):
        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._job_center = job_center()
        self._pre_jobs_info = {}

    def run(self):
        section_name = "strategy_job"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        
        job_conf_parser_obj = job_conf_parser()
        jobs_info = job_conf_parser_obj.parse(conf_path)
        self._excute_jobs(jobs_info)

    def _excute_jobs(self, jobs_info):
        add_dict = {}
        remove_dict = {}
        modify_dict = {}

        frame_tools.dict_diff(jobs_info, self._pre_jobs_info, add_dict, remove_dict, modify_dict)

        add_jobs_info = dict(add_dict, **modify_dict)

        remove_jobs_info = {}
        for item in modify_dict.keys():
            remove_jobs_info[item] = self._pre_jobs_info[item]

        LOG_INFO("add jobs %s" % (json.dumps(add_jobs_info)))
        LOG_INFO("remove jobs %s" % (json.dumps(remove_jobs_info)))

        self._pre_jobs_info = jobs_info

        self._job_center.remove_jobs(remove_jobs_info)
        self._job_center.add_jobs(add_jobs_info)

if __name__ == "__main__":
    a = j_load_conf()
    test_data_1 = {"a":"b", "c":"d"}
    a._excute_jobs(test_data_1)
    print "******************************"
    
    test_data_2 = {"a":"b", "c":"d"}
    a._excute_jobs(test_data_2)
    print "******************************"
    
    test_data_3 = {"c":"d"}
    a._excute_jobs(test_data_3)
    print "******************************"
    
    test_data_4 = {"a":"b", "c":"d"}
    a._excute_jobs(test_data_4)
    print "******************************"
    
    test_data_5 = {"a":"b", "c":"d", "e":"f"}
    a._excute_jobs(test_data_5)
    print "******************************"
    
    test_data_6 = {"a":"g", "c":"d", "e":"f"}
    a._excute_jobs(test_data_6)
    print "******************************"
