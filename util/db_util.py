import pymysql


def get_db_connection(host, port, user, password, database):
    db = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    return db


def get_node_connection():
    return get_db_connection("", 3306, "", "", "")


def get_btok_connection():
    return get_db_connection("", 13306, "", "", "")

