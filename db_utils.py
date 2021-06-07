"""Database utils."""

from configparser import ConfigParser
import os

from loguru import logger


@logger.catch
def get_config(dbtype: str):
    """Getting db params.

    :param dbtype: section
    :return: ConfigParser
    """

    config = ConfigParser()
    conf_file = os.path.join(os.path.dirname
                             (os.path.realpath(__file__)),
                             'db_conf.ini')
    config.read(conf_file)

    logger.info('Getting db params')

    return config[dbtype]


@logger.catch
def set_config(db_name: str, db_host: str, db_user: str, db_password: str):
    """Setting db params

    :param db_name: str
    :param db_host: str
    :param db_user: str
    :param db_password: str
    :return: None
    """

    config = ConfigParser()
    config.add_section('mysql')
    config.set('mysql', 'name', db_name)
    config.set('mysql', 'host', db_host)
    config.set('mysql', 'user', db_user)
    config.set('mysql', 'password', db_password)

    with open('db_conf.ini', 'w') as configfile:
        config.write(configfile)

    logger.info('Setting db params')
