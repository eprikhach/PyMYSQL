from configparser import ConfigParser
import os


def get_config(dbtype: str):
    config = ConfigParser()
    conf_file = os.path.join(os.path.dirname
                             (os.path.realpath(__file__)),
                             'DB_Conf.ini')
    config.read(conf_file)
    return config[dbtype]
