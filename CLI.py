"""CLI for PyMYSQL."""

import argparse

from loguru import logger


@logger.catch
def args_parser():
    """Script arguments parser.

    :return: class
    """
    parser = argparse.ArgumentParser(
        description='Parser for script, which implement serialization')
    parser.add_argument('-s', '--students', type=str,
                        help='Path to students.json')
    parser.add_argument('-r', '--rooms', type=str,
                        help='Path to rooms.json')
    parser.add_argument('-f', '--format', choices=['json', 'xml'],
                        help="Serialization format")
    return parser.parse_args()
