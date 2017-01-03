from job_base import job_base

class interval_job_test(job_base):
    def __init__(self):
        print "interval job init"
    def __del__(self):
        print "interval job del"
    def run(self):
        print "interval job run"
