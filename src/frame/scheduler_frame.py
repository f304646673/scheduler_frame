import os
import sys
import time
import ConfigParser

from loggingex import LOG_INIT
from loggingex import LOG_WARNING
from loggingex import LOG_DEBUG

from job_parser import job_parser
from job_center import job_center
from singleton import singleton
from scheduler_frame_conf_inst import scheduler_frame_conf_inst 

class scheduler_frame(singleton):
    _frame_conf_inst = None
    _job_center = None

    def __init__(self, conf_path):
        os.chdir("../../")
        sys.path.append("src/")
        sys.path.append("src/*")
        sys.path.append("src/*/*")
        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._frame_conf_inst.load(conf_path)

        self._job_center = job_center()
        self._job_center.start()

    def _init_log(self):
        section_name = "frame_log"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            print("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        print("Load %s %s %s" % (section_name, option_name, conf_path))
        LOG_INIT(conf_path)

    def _start_jobs(self):
        section_name = "frame_job"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        job_parser_obj = job_parser()
        jobs_info = job_parser_obj.parse(conf_path)
        self._job_center.add_jobs(jobs_info, True)
    
    def _init_db(self):
        section_name = "mysql_conn"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        

    def start(self):
        self._init_log()
        self._init_db()
        self._start_jobs()

if __name__ == "__main__":
    a = scheduler_frame("./conf/frame.conf")
    a.start()
    time.sleep(1000)
