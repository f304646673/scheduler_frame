import time

from datetime import date
from apscheduler.schedulers.background import BackgroundScheduler

import conf_keys
from job_conf_parser import job_conf_parser
from singleton import singleton
from loggingex import LOG_WARNING

class job_center(singleton):
    def __init__(self):
        self._sched = None
        self._job_conf_path = ""
        self._job_id_handle = {}
        self._static_job_id_handle = {}
    
    def start(self):
        self._sched = BackgroundScheduler()
        self._sched.start()

    def add_jobs(self, jobs_info, is_static = False):
        if None == self._sched:
            LOG_WARNING("job center must call start() first")
            return

        for (job_name,job_info) in jobs_info.items():
            if is_static and job_name in self._static_job_id_handle.keys():
                continue
            job_type = job_info["type"]
            class_name = job_info["class"]
            job_handle = self._get_obj(class_name)
            if is_static:
                self._static_job_id_handle[job_name] = job_handle
            else:
                self._job_id_handle[job_name] = job_handle
            cmd = "self._sched.add_job(job_handle.run, job_type, id = job_name"
            params = self._join_params(job_info)
            if 0 != len(params):
                cmd += " , "
                cmd += params
            cmd += ")"
            #print cmd
            eval(cmd)

    def remove_jobs(self, jobs_info):
        if None == self._sched:
            LOG_WARNING("job center must call start() first")
            return

        for job_name in jobs_info.keys():
            self._sched.remove_job(job_name)
            self._job_id_handle.pop(job_name)


    def _join_params(self, job_info):
        params = ""
        param = ""
        job_type = job_info["type"]
        for key in job_info.keys():
            if key in conf_keys.job_conf_info_dict[job_type]:
                if  0 != len(params):
                    params += ' , '
                value = job_info[key]
                if value.isdigit():
                    param = key + " = " + value
                else:
                    param = key + " = '" + value + "'"
                if 0 != len(param):
                    params += param
        return params
        

    def _get_obj(self, _cls_name):  
        _packet_name = _cls_name  
        _module_home = __import__(_packet_name,globals(),locals(),[_cls_name])
        obj =  getattr(_module_home,_cls_name)  
        return obj()

if __name__ == "__main__":
    a = job_center("job_sample.conf")
    a.start()
    time.sleep(1000)
