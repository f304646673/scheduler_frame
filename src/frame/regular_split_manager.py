import re
import sys
import json

from singleton import singleton
from loggingex import LOG_INFO
from loggingex import LOG_WARNING

@singleton
class regular_split_manager():
    def __init__(self):
        self._regulars = {}

    def modify_regulars(self, regulars_info):
        for (regular_name, regular_info) in regulars_info.items():
            self._regulars[regular_name] = regulars_info

    def add_regulars(self, regulars_info):
        for (regular_name, regular_info) in regulars_info.items():
            self._regulars[regular_name] = regular_info
    
    def remove_regulars(self, regulars_info):
        for (regular_name, regular_info) in regulars_info.items():
            if regular_name in self._regulars.keys():
                del self._regulars[regular_name]

    def get_split_data(self, data, regular_name):
        data_array = []
        self._recursion_regular(data, regular_name, 0, data_array)   
        return data_array

    def _get_regular(self, regular_name, deep):
        if regular_name not in self._regulars.keys():
            LOG_WARNING("regular manager has no %s" % (regular_name))
            return ""
        if deep >= len(self._regulars[regular_name]):
            return ""
        return self._regulars[regular_name][deep]

    def _recursion_regular(self, data, regular_name, deep, data_array):
        regular_str = self._get_regular(regular_name, deep)
        split_data = re.findall(regular_str, data)
        regualer_next_str = self._get_regular(regular_name, deep + 1)
        split_array = []
        if len(regualer_next_str) > 0:
            for item in split_data:
                self._recursion_regular(item, regular_name, deep + 1, data_array)
        else:
            for item in split_data:
                split_array.append(item)
            if len(split_array) > 0:
                data_array.append(split_array)

if __name__ == "__main__":
    import os
    os.chdir("../../")
    
    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
   
    from j_load_regular_conf import j_load_regular_conf
    j_load_regular_conf_obj = j_load_regular_conf()
    j_load_regular_conf_obj.run()

    regular_split_manager_obj = regular_split_manager()
    test_data = """data:["a,b,c", "e,f,g"]"""
    print regular_split_manager_obj.get_split_data(test_data, "string_comma_regular")
    
