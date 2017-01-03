import md5
import json

def hash(data):
    m = md5.new()   
    m.update(data)   
    return m.hexdigest()  

def dict_diff(cur_data_dict, ori_data_dict, add_dict, remove_dict, modify_dict):
    add_dict.clear()
    remove_dict.clear()
    modify_dict.clear()

    cur_json_data = json.dumps(cur_data_dict)
    cur_json_data_hash = hash(cur_json_data)
    
    ori_json_data = json.dumps(ori_data_dict)
    ori_json_data_hash = json.dumps(ori_json_data)

    if ori_json_data_hash == cur_json_data_hash:
        return

    same_name_list = []

    for (item_name, item_info) in cur_data_dict.items():
        if item_name not in ori_data_dict.keys():
            continue
        cur_hash = hash(json.dumps(item_info))
        pre_hash = hash(json.dumps(ori_data_dict[item_name]))
        if cur_hash != pre_hash:
            modify_dict[item_name] = cur_data_dict[item_name]
        same_name_list.append(item_name)

    
    add_name_list = list(set(cur_data_dict.keys()).difference(set(same_name_list)))
    remove_name_list = list(set(ori_data_dict.keys()).difference(set(same_name_list)))

    for add_jobs_name in add_name_list:
        add_dict[add_jobs_name] = cur_data_dict[add_jobs_name]

    for remove_jobs_name in remove_name_list:
        remove_dict[remove_jobs_name] = ori_data_dict[remove_jobs_name]

if __name__ == "__main__":
    cur_dict_1 = {"a":["b", "c"], "d":["e", "f"]}
    ori_dict_1 ={}
    add_dict = {}
    remove_dict = {}
    modify_dict = {}

    dict_diff(cur_dict_1, ori_dict_1, add_dict, remove_dict, modify_dict)
    print "add dict:" 
    print add_dict
    print "remove dict:"
    print remove_dict
    print "modify dict:"
    print modify_dict

    cur_dict_2 = {"a":["b", "c"], "d":["e", "f"]}
    dict_diff(cur_dict_2, cur_dict_1, add_dict, remove_dict, modify_dict)
    print "add dict:" 
    print add_dict
    print "remove dict:"
    print remove_dict
    print "modify dict:"
    print modify_dict


    cur_dict_3 = {"a":["b", "c"]}
    dict_diff(cur_dict_3, cur_dict_2, add_dict, remove_dict, modify_dict)
    print "add dict:" 
    print add_dict
    print "remove dict:"
    print remove_dict
    print "modify dict:"
    print modify_dict

    cur_dict_4 = {"a":["b", "c"], "d":["e", "f"]}
    dict_diff(cur_dict_4, cur_dict_3, add_dict, remove_dict, modify_dict)
    print "add dict:" 
    print add_dict
    print "remove dict:"
    print remove_dict
    print "modify dict:"
    print modify_dict

    cur_dict_5 = {"a":["b", "c"], "d":["e", "g"]}
    dict_diff(cur_dict_5, cur_dict_4, add_dict, remove_dict, modify_dict)
    print "add dict:" 
    print add_dict
    print "remove dict:"
    print remove_dict
    print "modify dict:"
    print modify_dict
    pass
