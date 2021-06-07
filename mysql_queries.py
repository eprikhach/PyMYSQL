"""Queries to db"""

import time

from database_connector import DbConnector


def create_rooms_table():
    """Creating rooms table.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """create table if not exists rooms(
            id int primary key, 
            number varchar(20)
            )"""

            cursor.execute(query)


def create_students_table():
    """Creating student table.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """create table if not exists students(
            id int primary key, 
            name varchar(40),
            sex enum('M','F'),
            birthday datetime,
            room int,
            foreign key (room) references rooms(id)
            )"""

            cursor.execute(query)


def create_queries_timer_table():
    """Creating student table.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """create table if not exists queries_time(
            id int auto_increment primary key,
            query_name varchar(100),
            spent_time varchar(10)
            )"""

            cursor.execute(query)


def drop_students_table():
    """Deleting students table.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """drop table students"""

            cursor.execute(query)


def drop_rooms_table():
    """Deleting rooms table.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """drop table rooms"""

            cursor.execute(query)


def set_indexes():
    """Adding indexes to tables.

    :return: None
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """alter table students add index idx_students
             (Name, Sex)"""

            cursor.execute(query)
            query = """alter table rooms add index idx_rooms
                         (Number)"""
            cursor.execute(query)


def time_tracker(values: list):
    """Inserting time spent on query into table.

    :return: None
    """
    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """insert into queries_time(query_name, 
            spent_time) values(%s, %s)"""
            cursor.execute(query, (values[0], values[1]))

            conn.commit()


def queries_timer(func):
    """Getting time spent on query.

    :param func: function
    :return: wrapped function
    """
    def wrapper():
        start_time = time.time()
        func()
        end_time = time.time() - start_time
        time_tracker([func.__name__, end_time])

        return func
    return wrapper


def drop_indexes():
    """Deleting indexes from the tables.

    :return: None
    """
    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """drop index idx_students on students; """
            cursor.execute(query)
            query = """drop index idx_rooms on rooms; """
            cursor.execute(query)


@queries_timer
def students_in_room_count():
    """Getting count of students living in a room.

    :return: list
    """
    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """
            select r.id as room_id, count(s.room) as students_count
            from rooms r
            left join students s
            on 
            r.id = s.room
            group by r.id ;
            """
            cursor.execute(query)

        return cursor.fetchall()


@queries_timer
def get_top5_lowest_avg_ages():
    """Getting top 5 rooms with a lowest average ages.

    :return: list
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """
            select rooms.id as room_name, 
            cast(avg(datediff(current_date(), students.Birthday)/365) as float)
            as avg_age
            from rooms left join students
            on rooms.id = students.room
            group by rooms.id
            order by avg_age limit 5
            """
            cursor.execute(query)

            return cursor.fetchall()


@queries_timer
def get_top5_higher_ages_diff():
    """Getting top 5 rooms with a highest ages diff.

    :return: list
    """

    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """
            select rooms.number as room_name,
            cast((datediff(max(students.birthday),min(students.birthday))/365) 
            as float) as age_diff
            from rooms left join students
            on rooms.id = students.room
            group by rooms.number
            order by age_diff desc limit 5;
            """

            cursor.execute(query)

            return cursor.fetchall()


@queries_timer
def get_rooms_with_diff_genders():
    """Getting list of rooms with different gender students living here.

    :return: list
    """
    with DbConnector() as conn:
        with conn.cursor() as cursor:
            query = """
            select r.number as room_Name
            from rooms r join students s 
            on r.id = s.room
            group by r.id having count(distinct sex) = 2
            order by r.id
            """

            cursor.execute(query)

        return cursor.fetchall()
