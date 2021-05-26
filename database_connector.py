"""Connection to db"""

import pymysql
from pymysql.cursors import DictCursor
from configparser import ConfigParser


class DBConnector:
    """DB context manager."""

    def __init__(self, config: ConfigParser):
        self.config = config

    def __enter__(self):
        """Open a connection to the database.

        :return: connection object
        """
        self.conn = pymysql.connect(
            db=self.config["db"],
            charset='utf8mb4',
            cursorclass=DictCursor,
            host=self.config["host"],
            user=self.config["user"],
            password=self.config["password"]
        )
        return self.conn

    def __exit__(self, exc_type: Exception, exc_val: Exception,
                 exc_tb: Exception):
        """Closing the database connection.

        :param exc_type: The type of the caught exception, or None.
        :param exc_val: The caught exception object, or None.
        :param exc_tb:  traceback: The stack trace for the caught
        exception, or None.
        :return: None
        """
        self.conn.close()
