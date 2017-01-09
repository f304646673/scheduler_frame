import copy
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
            if cp.has_option(section, "range_max"):
                range_max = int(cp.get(section, "range_max"))
                db_name_base = conn_info["db"] 
                for index in range(0, range_max):
                    conn_info["db"] = db_name_base + "_" + str(index)
                    section_index_name = section + "_" + str(index)
                    conns_info[section_index_name] = copy.deepcopy(conn_info)
            else:
                conns_info[section] = conn_info
        return conns_info

if __name__ == "__main__":
    a = mysql_conf_parser()
    print a.parse("../../conf/mysql_manager.conf")
    pass
