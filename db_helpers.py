import psycopg2


def execute_commands(host, port, database, user, password, commands):
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    finally:
        if conn is not None:
            conn.close()


def open_connection(host, port, database, user, password):
    conn_string = "host={} port={} dbname={} user={} password={}".format(host, port, database, user, password)
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print('Connected to DB.\n')
    return conn, cursor


def close_connection(conn):
    conn.close()
    print('Connection to DB closed')
