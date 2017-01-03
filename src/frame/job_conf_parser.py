import ConfigParser
import job_conf

class job_conf_parser:
    
    def parse(self, job_conf_path):

        cp = ConfigParser.SafeConfigParser()
        cp.read(job_conf_path)
        sections = cp.sections()
        jobs_info = {}
        for section in sections:
            if False == cp.has_option(section, "type") or False == cp.has_option(section, "class"):
                continue
            job_info = {}
            job_type = cp.get(section, "type")
            job_class = cp.get(section, "class")
            if job_type not in job_conf.job_conf_info_dict.keys():
                continue
            job_info["type"] = job_type
            job_info["class"] = job_class
            for key in job_conf.job_conf_info_dict[job_type]:
                if False == cp.has_option(section, key):
                    continue
                job_info[key] = cp.get(section, key)
            jobs_info[section] = job_info
        return jobs_info

if __name__ == "__main__":
    a = job_conf_parser()
    a.parse("job_sample.conf")
    pass
