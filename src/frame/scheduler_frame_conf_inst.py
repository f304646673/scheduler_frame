import ConfigParser
from singleton import singleton

@singleton
class scheduler_frame_conf_inst():
    def load(self, conf_path):
        print("load frame conf %s" % conf_path)
        self._cp = ConfigParser.SafeConfigParser()
        self._cp.read(conf_path)

    def has_option(self, section_name, option_name):
        if self._cp:
            return self._cp.has_option(section_name, option_name)
        else:
            print("no conf path load")

    def get(self, section_name, option_name):
        if self._cp:
            return self._cp.get(section_name, option_name)
        else:
            print("no conf path load")


if __name__ == "__main__":
    a = scheduler_frame_conf_inst()
    print a.get("frame_job", "conf_path")
