#!/usr/bin/python

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

from config import DB_HOST, DB_PORT, DB_USER, DB_PW, DB_NAME
from config_db_admin import DB_ADMIN_DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW


def create_non_admin_user():
    commands = (
        """
        CREATE USER %s WITH
            LOGIN
            NOSUPERUSER
            INHERIT
            NOCREATEDB
            NOCREATEROLE
            NOREPLICATION
        """ % DB_USER,
        """
        ALTER USER %s
            PASSWORD '%s'
        """ % (DB_USER, DB_PW),
    )
    execute_commands(DB_HOST, DB_PORT, DB_ADMIN_DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)


def create_tables_for_arxiv():
    commands = (
        """
        CREATE TABLE arxiv_articles (
            identifier VARCHAR(31) NOT NULL PRIMARY KEY, -- max found 30
            title VARCHAR NOT NULL, -- max found 279
            created DATE NOT NULL, -- date (10)
            categories VARCHAR(255) NOT NULL, -- max found 124
            datestamp DATE NOT NULL, -- date (10)
            set_spec VARCHAR(255) NOT NULL, -- max found 158
            abstract VARCHAR NOT NULL, -- max found 3930
            msc_class VARCHAR(255), -- max found 158 
            acm_class VARCHAR, -- max found 264
            comments VARCHAR, -- max found 1150
            updated DATE, -- date (10)
            journal_ref VARCHAR, -- max found 331
            report_no VARCHAR, -- max found 347 
            doi VARCHAR(255) -- max found 138 
          )
        """,
        """
        CREATE TABLE arxiv_authorship (
            article_id VARCHAR(31) NOT NULL,
            author_pos INTEGER NOT NULL,
            keyname VARCHAR(255) NOT NULL, -- max found 52
            forenames VARCHAR(255), -- max found 64
            suffix VARCHAR(10), -- max found 3
            first_name VARCHAR(255),
            middle_name VARCHAR(255),
            CONSTRAINT authorship_pk PRIMARY KEY (article_id, author_pos),
            FOREIGN KEY (article_id)
                REFERENCES arxiv_articles (identifier)
                ON UPDATE CASCADE 
                ON DELETE CASCADE
        )
        """,
        """
        CREATE INDEX article_id_idx
            ON public.arxiv_authorship (article_id)
        """,
        """
        CREATE TABLE arxiv_affiliations (
            affiliation_id SERIAL PRIMARY KEY,
            article_id VARCHAR(31) NOT NULL,
            author_pos INTEGER NOT NULL,
            affiliation VARCHAR, -- max found 329 
            FOREIGN KEY (article_id, author_pos)
                REFERENCES arxiv_authorship (article_id, author_pos)
                ON UPDATE CASCADE 
                ON DELETE CASCADE
        )
        """
        """
        CREATE INDEX article_id_aff_idx
            ON public.arxiv_affiliations (article_id)
        """
    )
    execute_commands(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)


def grant_to_non_admin_user():
    commands = (
        """
        GRANT ALL ON ALL TABLES IN SCHEMA public TO %s
        """ % DB_USER,
        """
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s
        """ % DB_USER
    )
    execute_commands(DB_HOST, DB_PORT, DB_NAME, DB_ADMIN_USER, DB_ADMIN_PW, commands)


def create_database(database_name):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_ADMIN_DB_NAME, user=DB_ADMIN_USER,
                                password=DB_ADMIN_PW)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("""
            CREATE DATABASE %s
                WITH 
                OWNER = %s
                ENCODING = 'UTF8'
                CONNECTION LIMIT = -1
            """, database_name, DB_ADMIN_USER)
        cur.close()
        conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


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


if __name__ == '__main__':
    create_non_admin_user()

    create_database(DB_NAME)
    create_tables_for_arxiv()

    # run after all tables have been created!
    grant_to_non_admin_user()
