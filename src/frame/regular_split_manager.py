#coding=utf-8
#-*- coding: utf-8 -*-

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
    
    import sys
    reload(sys)
    sys.setdefaultencoding("utf8")

    from scheduler_frame_conf_inst import scheduler_frame_conf_inst
    frame_conf_inst = scheduler_frame_conf_inst()
    frame_conf_inst.load("./conf/frame.conf")
   
    from j_load_regular_conf import j_load_regular_conf
    j_load_regular_conf_obj = j_load_regular_conf()
    j_load_regular_conf_obj.run()

    regular_split_manager_obj = regular_split_manager()
    test_data = """
    data:["2,000825,太钢不锈,5.12,10.11,17815.31,12.56,19933.61,14.05,-2118.30,-1.49,-10376.81,-7.31,-7438.51,-5.24,2017-01-11 15:00:00","2,300571,平治信息,102.08,-3.77,17568.33,25.39,5748.17,8.31,11820.16,17.09,-13102.20,-18.94,-4466.13,-6.46,2017-01-11 15:00:00"]
    """
    print regular_split_manager_obj.get_split_data(test_data, "string_comma_regular")
    quit()

    test_data = """
    var hq_str_sz000001="ƽ°²ÒÐ,9.150,9.150,9.150,9.160,9.140,9.140,9.150,24105395,220575131.960,2686152,9.140,1061400,9.130,604900,9.120,574700,9.110,572200,9.100,622479,9.150,1839523,9.160,2385519,9.170,1233430,9.180,1339248,9.190,2017-01-10,15:05:03,00";
    var hq_str_sh601003="Á¸ֹɷÝ4.620,4.650,4.810,4.870,4.610,4.810,4.820,63821968,305063734.000,57920,4.810,193700,4.800,149100,4.790,302500,4.780,237475,4.770,485141,4.820,501400,4.830,217200,4.840,533600,4.850,415400,4.860,2017-01-10,15:00:00,00";
    var hq_str_sh601001="´óºҵ,6.270,6.270,6.360,6.410,6.230,6.360,6.370,16896165,106949852.000,106776,6.360,94143,6.350,124600,6.340,108580,6.330,85700,6.320,170400,6.370,119500,6.380,139300,6.390,281248,6.400,139800,6.410,2017-01-10,15:00:00,00";
    var hq_str_sh800001="";
    """
    print regular_split_manager_obj.get_split_data(test_data, "hq_sinajs_cn_list")

    
