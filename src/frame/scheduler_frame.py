import os
import sys
import time
import ConfigParser

from loggingex import LOG_INIT
from loggingex import LOG_WARNING
from loggingex import LOG_DEBUG

from job_conf_parser import job_conf_parser
from job_center import job_center
from singleton import singleton
from mysql_manager import mysql_manager
from mysql_conf_parser import mysql_conf_parser
from scheduler_frame_conf_inst import scheduler_frame_conf_inst 

@singleton
class scheduler_frame():
    def __init__(self, conf_path):
        os.chdir("../../")
        sys.path.append("src/")
        self._append_src_path("src/")

        self._frame_conf_inst = scheduler_frame_conf_inst()
        self._frame_conf_inst.load(conf_path)
        
        self._mysql_manager = mysql_manager()

        self._job_center = job_center()
        self._job_center.start()

    def _append_src_path(self, path):
        filelist =  os.listdir(path)  
        for filename in filelist:  
            filepath = os.path.join(path, filename)  
            if os.path.isdir(filepath):
                sys.path.append(filepath)
                self._append_src_path(filepath)  


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
        job_conf_parser_obj = job_conf_parser()
        jobs_info = job_conf_parser_obj.parse(conf_path)
        self._job_center.add_jobs(jobs_info, True)
    
    def _init_db(self):
        section_name = "mysql_manager"
        option_name = "conf_path"
        if False == self._frame_conf_inst.has_option(section_name, option_name):
            LOG_WARNING("no %s %s" % (section_name, option_name))
            return
        conf_path = self._frame_conf_inst.get(section_name, option_name)
        LOG_DEBUG("Load %s %s %s" % (section_name, option_name, conf_path))
        mysql_conf_parser_obj = mysql_conf_parser()
        conns_info = mysql_conf_parser_obj.parse(conf_path)
        self._mysql_manager.add_conns(conns_info)
        #print conns_info

    def start(self):
        self._init_log()
        self._init_db()
        self._start_jobs()

if __name__ == "__main__":
    a = scheduler_frame("./conf/frame.conf")
    a.start()
    time.sleep(1000)
