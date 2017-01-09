import ConfigParser

class regulars_manager_conf_parser:
    
    def parse(self, regulars_conf_path):
        cp = ConfigParser.SafeConfigParser()
        cp.read(regulars_conf_path)
        sections = cp.sections()
        regulars_info = {}
        for section in sections:
            regular_info = []
            regular_name_pre = "regular_expression_"
            for index in range(0, len(cp.options(section))):
                regular_name = regular_name_pre + str(index)
                if cp.has_option(section, regular_name):
                    regular_info.append(cp.get(section, regular_name))
                else:
                    break
            regulars_info[section] = regular_info
        return regulars_info

if __name__ == "__main__":
    import os
    os.chdir("../../")

    a = regulars_manager_conf_parser()
    print a.parse("./conf/regulars_manager.conf")
    pass
