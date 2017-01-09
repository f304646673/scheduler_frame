import ConfigParser
from singleton import singleton

@singleton
class scheduler_frame_conf_inst():
    def __init__(self):
        self._cp = None

    def load(self, conf_path):
        print("load frame conf %s" % conf_path)
        self._cp = ConfigParser.SafeConfigParser()
        self._cp.read(conf_path)

    def has_option(self, section_name, option_name):
        if self._cp:
            return self._cp.has_option(section_name, option_name)
        return False

    def get(self, section_name, option_name):
        if self._cp:
            return self._cp.get(section_name, option_name)
        else:
            print("get conf %s %s" % (section_name, option_name))


if __name__ == "__main__":
    import os
    os.chdir("../../")
    
    a = scheduler_frame_conf_inst()
    a.load("./conf/frame.conf")

    print a.get("frame_job", "conf_path")
    print a.get("frame_log", "conf_path")
    print a.get("strategy_job", "conf_path")
    print a.get("mysql_manager", "conf_path")
    print a.get("regulars", "conf_path")
