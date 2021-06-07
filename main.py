"""In this task we will work with OOP and MYSQL.

To solve this task we need:
1. Deserialize JSON files.
2. Insert json structure into db.
3. Create 4 queries to db, which return indeed result.
"""

# Logging third-party tools is mainly used here for debugging. I
# thought about catching critical situations of different levels,
# but this would greatly inflate the program.
from loguru import logger


from cli import args_parser
from db_utils import set_config
from serializers import JSONSerializer, XMLSerializer
from recorders import XMLWriter, JSONWriter
from deserializers import StudentDeserializer, RoomsDeserializer, \
    get_students_from_json, get_rooms_from_json


import mysql_queries

# Removing basic logging handler, which is used for debugging.
# Users don't need to know what's going on in the program.
logger.remove(0)

logger.add('logs.log', level='INFO',
           format="{time} {level} {message}",
           rotation='1 MB', compression='zip')


@logger.catch
def main(room_json: str, student_json: str, serialize_format: str):
    students = get_students_from_json(student_json)
    rooms = get_rooms_from_json(room_json)

    mysql_queries.create_rooms_table()
    mysql_queries.create_students_table()
    mysql_queries.create_queries_timer_table()

    RoomsDeserializer(rooms).to_table()
    StudentDeserializer(students).to_table()

    mysql_queries.set_indexes()

    queries_result = [
        {"Students in room count": mysql_queries.
            students_in_room_count()},
        {"Lowest avg age": mysql_queries.get_top5_lowest_avg_ages()},
        {"Higher ages diff": mysql_queries.get_top5_higher_ages_diff()},
        {"Rooms with diff gender": mysql_queries.
            get_rooms_with_diff_genders()}
    ]

    for dicts in queries_result:
        for file_name, result in dicts.items():
            if serialize_format.lower() == 'xml':
                serialized_xml = XMLSerializer(result()).to_format()
                XMLWriter(serialized_xml).to_file(file_name)
            if serialize_format.lower() == 'json':
                serialized_json = JSONSerializer(result()).to_format()
                JSONWriter(serialized_json).to_file(file_name)


if __name__ == '__main__':
    args_namespace = args_parser()

    set_config(args_namespace.db_name, args_namespace.db_host,
               args_namespace.db_user, args_namespace.db_password)

    main(args_namespace.rooms, args_namespace.students,
         args_namespace.format)
