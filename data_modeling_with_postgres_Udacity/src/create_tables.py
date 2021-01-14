import psycopg2
from sql_queries import create_table_queries
from sql_queries import drop_table_queries


##############################################################################
def create_database():
    """
    Creates and connects to sparkifydb
    :return: cursor and connection to sparkifydb
    """
    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0"
    )

    # close connection to default database
    conn.close()

    # conenct to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Executes all queries to drop tables
    :param cur: cursor to the database
    :param conn: connection to the database
    :return:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes all queries to create tables
    :param cur: cursor to the database
    :param conn: connection to the database
    :return:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Executes all functions: defines cursor and connections, drops existing
    tables, and creates new tables
    :return:
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


##############################################################################
if __name__ == '__main__':
    main()
