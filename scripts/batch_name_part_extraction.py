# Add author name parts to existing authorship table
# for results of exploration of these methods see notebook in project 'name_gender_production'

import io
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from config import DB_HOST, DB_PORT, DB_NAME
from config_db_admin import DB_ADMIN_PW, DB_ADMIN_USER
from db_helpers import open_connection, close_connection, execute_commands
from helpers import extract_first_and_middle_name, replace_umlauts

ENGINE = create_engine('postgresql://%s:%s@%s:%s/%s' % (DB_ADMIN_USER, DB_ADMIN_PW, DB_HOST, DB_PORT, DB_NAME))


def fetch_distinct_forenames(cursor):
    """Fetch distinct forenames from the authorship table and return as a pandas DataFrame"""
    cursor.execute("""SELECT DISTINCT forenames FROM arxiv_authorship""")
    df = cursor.fetchall()
    df = pd.DataFrame(df, columns=["forenames"])

    return df


def build_tmp_table(df, conn):
    commands = (
        """
        CREATE TABLE arxiv_forenames_tmp (
            forenames VARCHAR, 
            first_name VARCHAR, 
            middle_name VARCHAR)
        """,
        """CREATE INDEX forenames_idx ON public.arxiv_forenames_tmp (forenames)""",
    )
    execute_commands(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)

    s = datetime.now()

    with conn.cursor() as cursor:
        output = io.StringIO()
        df.to_csv(output, index=False, quoting=1)
        output.seek(0)
        table_name = 'arxiv_forenames_tmp'
        cols = ', '.join([f'{col}' for col in df.columns])
        sql = f'COPY {table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
        cursor.copy_expert(sql, output)
        count = cursor.rowcount
    conn.commit()
    print('  {} elapsed for insertion of {} rows into table {}'.format(datetime.now() - s, count, table_name))


def drop_tmp_table():
    commands = (
        """DROP TABLE IF EXISTS arxiv_forenames_tmp""",
    )

    execute_commands(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)


def update_authorship_table():
    commands = (
        """
          UPDATE arxiv_authorship
            SET first_name = arxiv_forenames_tmp.first_name,
                middle_name = arxiv_forenames_tmp.middle_name
            FROM arxiv_forenames_tmp
            WHERE arxiv_authorship.forenames = arxiv_forenames_tmp.forenames
        """,
    )
    execute_commands(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)


if __name__ == '__main__':
    db_conn, cur = open_connection(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW)

    names_arxiv = fetch_distinct_forenames(cur)
    names_arxiv['first_name'], names_arxiv['middle_name'] = zip(
        *names_arxiv.forenames.map(extract_first_and_middle_name))
    names_arxiv['first_name'] = names_arxiv['first_name'].map(replace_umlauts)
    names_arxiv['middle_name'] = names_arxiv['middle_name'].map(replace_umlauts)
    print(names_arxiv.head())
    print(names_arxiv.shape)
    print("Data prepared for temp table")

    drop_tmp_table()
    build_tmp_table(names_arxiv, db_conn)
    print("Built temp table")

    update_authorship_table()
    print("Updated authorship table")
    # drop_tmp_table()

    close_connection(db_conn)
