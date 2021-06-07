"""Database deserialization."""

import json

from loguru import logger

from database_connector import DbConnector


class Deserializer:
    """Base Deserialization class."""

    def __init__(self, json_data: list):
        self.json_data = json_data

    def to_table(self):
        raise NotImplementedError


class StudentDeserializer(Deserializer):
    """Class that implement deserialization to Students table."""

    def __init__(self, json_data: list):
        super().__init__(json_data)

    @logger.catch
    def to_table(self):
        """Inserting data into table.

        :return: None
        """

        with DbConnector() as conn:
            with conn.cursor() as cursor:
                for row in self.json_data:
                    query = """insert into students(id, name, birthday, 
                    room, sex) values(%s, %s, %s, %s, %s)"""
                    cursor.execute(query, (row["id"], row["name"],
                                   row["birthday"], row["room"],
                                   row["sex"]))

                    conn.commit()

        logger.info("Deserialization students into table.")


class RoomsDeserializer(Deserializer):
    """Class that implement deserialization to Rooms table."""

    def __init__(self, json_data: list):
        super().__init__(json_data)

    @logger.catch
    def to_table(self):
        """Inserting data into table.

        :return: None
        """

        with DbConnector() as conn:
            for row in self.json_data:
                with conn.cursor() as cursor:
                    query = """insert into rooms(id, number) values(
                    %s, %s)"""

                    cursor.execute(query, (row["id"], row["name"]))

                    conn.commit()
        logger.info("Deserialization rooms into table.")


@logger.catch
def get_students_from_json(students_json: str):
    """Getting students information from JSON object.

    :param students_json: Path to students JSON
    :return: list
    """

    with open(students_json, 'r') as json_file:
        students_list = json.load(json_file)

    logger.info('Students.json was deserialized to list of dicts.')

    return students_list


@logger.catch
def get_rooms_from_json(rooms_json: str):
    """Getting rooms information from JSON object.

        :param rooms_json: Path to rooms JSON
        :return: list
        """

    with open(rooms_json, 'r') as json_file:
        rooms_list = json.load(json_file)

    logger.info('Rooms.json was deserialized to list of dicts.')

    return rooms_list
