"""In this task we will work with OOP and MYSQL.

To solve this task we need:
1. Deserialize JSON files.
2. Insert json structure into db.
3. Create 4 queries to db, which return indeed result.
"""

from xml.dom.minidom import parseString
import json

from dicttoxml import dicttoxml

# Logging third-party tools is mainly used here for debugging. I
# thought about catching critical situations of different levels,
# but this would greatly inflate the program.
from loguru import logger

import Mysql_queries

from CLI import args_parser

# Removing basic logging handler, which is used for debugging.
# Users don't need to know what's going on in the program.
logger.remove(0)

logger.add('logs.log', level='INFO',
           format="{time} {level} {message}",
           rotation='1 MB', compression='zip')


class Serializer:
    """Base class, that implements serialization Python object"""

    def __init__(self, json_file):
        self.json_file = json_file

    def to_format(self):
        raise NotImplementedError


class XMLSerializer(Serializer):
    """Class that implements serialization to XML."""

    def __init__(self, json_file):
        super().__init__(json_file)

    def to_format(self):
        """Converts a lists of dicts to XML string.

        :return: XML in string representation
        """

        xml_string = dicttoxml(self.json_file,
                               custom_root='students_in_room',
                               # dict_to_xml(item_func) need function,
                               # that generate the element name for
                               # items in a list. Default is 'item'.
                               item_func=lambda source_name: 'field',
                               attr_type=False)

        xml = parseString(xml_string).toprettyxml()

        logger.info('Data was serialized into XML format.')

        return xml


class JSONSerializer(Serializer):
    """Class that implements serialization to XML."""

    def __init__(self, json_file):
        super().__init__(json_file)

    @logger.catch
    def to_format(self):
        """Converts a list of dicts to JSON string.

        :return: JSON in string representation.
        """

        json_string = json.dumps(self.json_file, indent=2)

        logger.info('Data was serialized into JSON format.')

        return json_string


class FileWriter:
    def __init__(self, serialized_structure):
        self.serialized_structure = serialized_structure

    def to_file(self):
        raise NotImplementedError


class JSONWriter(FileWriter):

    def __init__(self, serialized_structure):
        super().__init__(serialized_structure)

    @logger.catch
    def to_file(self, file_name):
        """Write information into json.

        :return: None
        """
        with open(file_name + '.json', 'w') as xml_file:
            xml_file.write(self.serialized_structure)

        logger.info('JSON structure was recorded.')


class XMLWriter(FileWriter):
    def __init__(self, serialized_structure):
        super().__init__(serialized_structure)

    @logger.catch
    def to_file(self, file_name):
        """Write information into XML"""
        with open(file_name + '.xml', 'w') as xml_file:
            xml_file.write(self.serialized_structure)

        logger.info('XML structure was recorded.')


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
    """Getting students information from JSON object.

        :param rooms_json: Path to rooms JSON
        :return: list
        """

    with open(rooms_json, 'r') as json_file:
        rooms_list = json.load(json_file)

    logger.info('Rooms.json was deserialized to list of dicts.')

    return rooms_list


@logger.catch
def main(room_json: str, student_json: str, serialize_format: str):
    students = get_students_from_json(student_json)
    rooms = get_rooms_from_json(room_json)

    Mysql_queries.create_rooms_table()
    Mysql_queries.create_students_table()

    Mysql_queries.create_queries_timer_table()

    Mysql_queries.RoomsDeserializer(rooms).to_table()
    Mysql_queries.StudentDeserializer(students).to_table()

    Mysql_queries.set_indexes()

    queries_result = [
        {"Students in room count": Mysql_queries.
            students_in_room_count()},
        {"Lowest avg age": Mysql_queries.get_top5_lowest_avg_ages()},
        {"Higher ages diff": Mysql_queries.get_top5_higher_ages_diff()},
        {"Rooms with diff gender": Mysql_queries.
            get_rooms_with_diff_genders()}
    ]

    for dicts in queries_result:
        for file_name, result in dicts.items():
            if serialize_format.lower() == 'xml':
                serialized_xml = XMLSerializer(result).to_format()
                XMLWriter(serialized_xml).to_file(file_name)
            if serialize_format.lower() == 'json':
                serialized_json = JSONSerializer(result).to_format()
                JSONWriter(serialized_json).to_file(file_name)


if __name__ == '__main__':
    args_namespace = args_parser()
    main(args_namespace.rooms, args_namespace.students,
         args_namespace.format)
