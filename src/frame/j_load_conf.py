import md5
import json

from job_base import job_base
from singleton import singleton
from job_center import job_center
from job_parser import job_parser
from scheduler_frame_conf_inst import scheduler_frame_conf_inst

from loggingex import LOG_INFO
from loggingex import LOG_DEBUG

class j_load_conf(job_base, singleton):
    _frame_conf_inst = None
    _job_center = None
    _pre_jobs_info = {}
    _pre_jobs_info_hash = ""

    def __init__(self):
        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._job_center = job_center()

    def _hash(self, data):
        m = md5.new()   
        m.update(data)   
        return m.hexdigest()  

    def run(self):
        section_name = "strategy_job"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        
        job_parser_obj = job_parser()
        jobs_info = job_parser_obj.parse(conf_path)
        self._excute_jobs(jobs_info)

    def _excute_jobs(self, jobs_info):
        json_data = json.dumps(jobs_info)
        json_info_hash = self._hash(json_data)
        
        if self._pre_jobs_info_hash == json_info_hash:
            return

        same_jobs_name = []

        for (item_name, item_info) in jobs_info.items():
            if item_name not in self._pre_jobs_info.keys():
                continue
            cur_hash = self._hash(json.dumps(item_info))
            pre_hash = self._hash(json.dumps(self._pre_jobs_info[item_name]))
            if cur_hash != pre_hash:
                continue
            same_jobs_name.append(item_name)
        
        add_jobs_name_list = list(set(jobs_info.keys()).difference(set(same_jobs_name)))
        remove_jobs_name_list = list(set(self._pre_jobs_info.keys()).difference(set(same_jobs_name)))

        add_jobs_info = {}
        for add_jobs_name in add_jobs_name_list:
            add_jobs_info[add_jobs_name] = jobs_info[add_jobs_name]

        remove_jobs_info = {}
        for remove_jobs_name in remove_jobs_name_list:
            remove_jobs_info[remove_jobs_name] = self._pre_jobs_info[remove_jobs_name]

        LOG_INFO("add jobs %s" % (json.dumps(add_jobs_info)))
        LOG_INFO("remove jobs %s" % (json.dumps(remove_jobs_info)))

        self._pre_jobs_info = jobs_info
        self._pre_jobs_info_hash = self._hash(json.dumps(self._pre_jobs_info))

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
    
