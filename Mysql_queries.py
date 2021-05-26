"""Queries to db"""

import time

from database_connector import DBConnector
from DB_Utils import get_config

db_config = get_config("mysql")


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

    def to_table(self):
        """Inserting data into table.

        :return: None
        """

        with DBConnector(db_config) as conn:
            with conn.cursor() as cursor:
                for row in self.json_data:
                    query = """insert into students(Id, Name, Birthday, 
                    Room, Sex) values(%s, %s, %s, %s, %s)"""
                    cursor.execute(query, (row["id"], row["name"],
                                   row["birthday"], row["room"],
                                   row["sex"]))

                    conn.commit()


class RoomsDeserializer(Deserializer):
    """Class that implement deserialization to Rooms table."""

    def __init__(self, json_data: list):
        super().__init__(json_data)

    def to_table(self):
        """Inserting data into table.

        :return: None
        """

        with DBConnector(db_config) as conn:
            for row in self.json_data:
                with conn.cursor() as cursor:
                    query = """insert into rooms(Id, Number) values(
                    %s, %s)"""

                    cursor.execute(query, (row["id"], row["name"]))

                    conn.commit()


def create_rooms_table():
    """Creating rooms table.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """CREATE TABLE IF NOT EXISTS rooms(
            Id INT PRIMARY KEY, 
            Number VARCHAR(20)
            )"""

            cursor.execute(query)


def create_students_table():
    """Creating student table.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """CREATE TABLE IF NOT EXISTS students(
            Id INT PRIMARY KEY, 
            Name VARCHAR(40),
            Sex ENUM('M','F'),
            Birthday DATETIME,
            Room INT,
            FOREIGN KEY (Room) REFERENCES rooms(Id)
            )"""

            cursor.execute(query)


def create_queries_timer_table():
    """Creating student table.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """CREATE TABLE IF NOT EXISTS queries_time(
            Id INT AUTO_INCREMENT PRIMARY KEY,
            query_name varchar(100),
            spent_time varchar(10)
            )"""

            cursor.execute(query)


def drop_students_table():
    """Deleting students table.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """DROP TABLE students"""

            cursor.execute(query)


def drop_rooms_table():
    """Deleting rooms table.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """DROP TABLE rooms"""

            cursor.execute(query)


def set_indexes():
    """Adding indexes to tables.

    :return: None
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """ALTER TABLE students ADD INDEX idx_students
             (Name, Sex)"""

            cursor.execute(query)
            query = """ALTER TABLE rooms ADD INDEX idx_rooms
                         (Number)"""
            cursor.execute(query)


def time_tracker(values: list):
    """

    :return:
    """
    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """Insert into queries_time(query_name, 
            spent_time) VALUES(%s, %s)"""
            cursor.execute(query, (values[0], values[1]))

            conn.commit()


def queries_timer(func):
    def wrapper():
        start_time = time.time()
        func()
        end_time = time.time() - start_time
        time_tracker([func.__name__, end_time])

        return func
    return wrapper


def drop_indexes():
    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """drop index idx_students on students; """
            cursor.execute(query)
            query = """drop index idx_rooms on rooms; """
            cursor.execute(query)


@queries_timer
def students_in_room_count():
    """Getting count of students living in a room.

    :return: return list
    """
    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """
            SELECT r.Id as Room_Id, COUNT(s.room) AS Students_Count
            FROM rooms r
            LEFT JOIN students s
            ON 
            r.Id = s.room
            GROUP BY r.Id ;
            """
            cursor.execute(query)

        return cursor.fetchall()


@queries_timer
def get_top5_lowest_avg_ages():
    """Getting top 5 rooms with a lowest average ages.

    :return: list
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """
            SELECT Room_name, Avg_age
            FROM
            (
            SELECT rooms.Number as Room_name,
            cast(AVG(DATEDIFF(CURRENT_DATE(), students.Birthday)/365) AS
            FLOAT) as Avg_age
            FROM rooms LEFT JOIN students
            ON rooms.Id = students.room
            Where students.Birthday > 1
            GROUP BY rooms.Number
            ) AS test
            GROUP BY Room_name
            ORDER BY avg_age LIMIT 5;
            """
            cursor.execute(query)

            return cursor.fetchall()


@queries_timer
def get_top5_higher_ages_diff():
    """Getting top 5 rooms with a highest ages diff.

    :return: list
    """

    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """
            SELECT Room_name, Age_diff
            FROM
            (
            SELECT rooms.Number as Room_name,
            cast((DATEDIFF(max(students.Birthday),
            min(students.Birthday))/365) AS
            FLOAT) as Age_diff
            FROM rooms LEFT JOIN students
            ON rooms.Id = students.room
            GROUP BY rooms.Number
            ) AS test
            GROUP BY Room_name
            ORDER BY Age_diff desc LIMIT 5;
            """

            cursor.execute(query)

            return cursor.fetchall()


@queries_timer
def get_rooms_with_diff_genders():
    """Getting list of rooms with different gender students living here.

    :return: list
    """
    with DBConnector(db_config) as conn:
        with conn.cursor() as cursor:
            query = """
            SELECT r.Number as Room_Name
            from rooms r join students s 
            ON r.Id = s.Room
            GROUP BY r.Id HAVING COUNT(DISTINCT sex) = 2
            ORDER BY r.Id
            """

            cursor.execute(query)

        return cursor.fetchall()
