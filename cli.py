"""CLI for script."""

import argparse

from loguru import logger


@logger.catch
def args_parser():
    """Script arguments parser.

    :return: Namespace
    """
    parser = argparse.ArgumentParser(
        description='Parser for script, which implement serialization')
    parser.add_argument('-s', '--students', type=str,
                        help='Path to students.json')
    parser.add_argument('-r', '--rooms', type=str,
                        help='Path to rooms.json')
    parser.add_argument('-f', '--format', choices=['json', 'xml'],
                        help="Serialization format")
    parser.add_argument('-n', '--db_name', type=str, help="Database name")
    parser.add_argument('-d', '--db_host', type=str, help="Database host")
    parser.add_argument('-u', '--db_user', type=str, help="Database user")
    parser.add_argument('-p', '--db_password', type=str,
                        help="Database password")


    args = parser.parse_args()
    return args
