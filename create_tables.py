import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is used to execute drop instructions defined in sql_queries.py to 'drop' the database.
    It's main purpose is to reset the database for testing.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is used to execute create instructions defined in sql_queries.py to initialize the database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function reads the dwh.cfg file in order to connect to AWS.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()