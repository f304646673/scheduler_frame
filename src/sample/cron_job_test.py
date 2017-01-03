from job_base import job_base

class cron_job_test(job_base):
    def __init__(self):
        print "cron job init"
    def __del__(self):
        print "cron job del"
    def run(self):
        print "cron job run"
