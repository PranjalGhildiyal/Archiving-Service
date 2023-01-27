'''
INFO: modular config code to fetch any config file.
By: Hardik Seth, Algo8 AI
Date: 27-June-2022
'''
import configparser
config_value = configparser.ConfigParser()
# function to fetch config values of sections dynamically
def read_config_file(getsections):
    try:
        # read config file from the directory
        config_value.read(r'V2/Config/Config.ini')
        # create list of sections available in the config file
        list_sections  = config_value.sections()
        # check if parsed section is in the list we of section we have
        if getsections not in list_sections:
            return(False,"section not found")
        else:
            print("found section")
        #  create dictionary of all the values and parse it to the code
        dict_new = {}
        for key_config in config_value[getsections]:
            dict_new[key_config] = config_value[getsections][key_config]
        #  return fetched values in the code
        return(True, dict_new)
    except Exception as e:
        return (False,"Reason : {}".format(str(e)))