import ConfigParser
import conf_keys
from loggingex import LOG_WARNING

class mysql_conf_parser:

    def parse(self, job_conf_path):

        cp = ConfigParser.SafeConfigParser()
        cp.read(job_conf_path)
        sections = cp.sections()
        conns_info = {}
        for section in sections:
            conn_info = {}
            for key in conf_keys.mysql_conn_keys:
                if False == cp.has_option(section, key):
                    LOG_WARNING()
                    continue
                conn_info[key] = cp.get(section, key)
            conns_info[section] = conn_info
        return conns_info

if __name__ == "__main__":
    a = mysql_conf_parser()
    print a.parse("../../conf/mysql_manager.conf")
    pass
