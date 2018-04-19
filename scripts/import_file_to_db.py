import io
import re
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from config import AWS_S3_BUCKET, DB_USER, DB_PW, DB_HOST, DB_PORT, DB_NAME
from db_constants import COLUMNS_ARTICLES, COLUMNS_AUTHORSHIP, COLUMNS_AFFILIATIONS, TABLE_ARTICLE, \
    TABLE_AUTHORSHIP, TABLE_AFFILIATION
from naive_s3_lock import NaiveS3Lock

LOCAL_BUFFER_DIR = "/tmp/"

COLUMN_RENAMING = {'acm-class': 'acm_class', 'msc-class': 'msc_class', 'setSpec': 'set_spec',
                   'journal-ref': 'journal_ref', 'report-no': 'report_no'}

ENGINE = create_engine('postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PW, DB_HOST, DB_PORT, DB_NAME))

LOCK = NaiveS3Lock(AWS_S3_BUCKET, 'importer.lock')

PREFIX_FILE = 'metadata/'
PREFIX_FILE_DELETIONS = 'missing_metadata/'
ARCHIVE_FOLDER = 'finished_metadata/'
ARCHIVE_FOLDER_DELETIONS = 'finished_missing_metadata/'

s3 = boto3.resource('s3')
BUCKET = s3.Bucket(AWS_S3_BUCKET)


def import_json_dump_into_db(filename):
    begin = datetime.now()

    print('{} ##### Begin import of file {}'.format(datetime.now(), filename))

    local_file_path = download_json_file(filename)

    df = df_from_json_file(local_file_path)

    ids = df['identifier'].values.tolist()  # collect all IDs to delete them before insertion

    df_filtered = remove_old_versions(df)

    df_filtered.rename(columns=COLUMN_RENAMING, inplace=True)  # rename columns to DB friendly names

    df_articles = prepare_articles_df(df_filtered)

    df_authors_and_affiliations = prepare_authors_and_affiliations_df(df_filtered)

    df_authors = prepare_authors_df(df_authors_and_affiliations)

    df_affiliations = prepare_affiliations(df_authors_and_affiliations)

    print('  {} elapsed for preparation of data'.format(datetime.now() - begin))

    apply_changes_to_db(ids, df_articles, df_authors, df_affiliations)

    move_file_to_folder(filename, ARCHIVE_FOLDER)

    print('{} ***** End import of file {}'.format(datetime.now(), filename))


def download_json_file(filename):
    local_filename = LOCAL_BUFFER_DIR + filename.split('/')[-1]
    BUCKET.download_file(filename, local_filename)
    return local_filename


def df_from_json_file(local_filename):
    return pd.read_json('file://localhost' + local_filename, orient="columns")


def remove_old_versions(df):
    """Removes older versions from DataFrame.
    This is the case when there are updates on a document in this df."""
    return df.drop_duplicates(['identifier'], keep='last')


def prepare_articles_df(df):
    df_complete = add_expected_columns(df, COLUMNS_ARTICLES)

    df_rel = df_complete[COLUMNS_ARTICLES]  # filter for relevant columns

    df_rel.title = df_rel.title.map(clean_whitespaces_and_line_breaks())
    df_rel.abstract = df_rel.abstract.map(clean_whitespaces_and_line_breaks())
    df_rel.comments = df_rel.comments.map(clean_whitespaces_and_line_breaks())
    return df_rel


def clean_whitespaces_and_line_breaks():
    return lambda x: re.sub(r"\s+", ' ', x) if isinstance(x, str) else x


def add_expected_columns(df, columns):
    data_frame = pd.DataFrame(df)
    for col in columns:
        if col not in data_frame.columns:
            data_frame[col] = None
    return data_frame


def ignore_initials(s):
    if len(s) > 1:
        return s
    else:
        return ''


def get_item_or_filler(l, pos, filler=''):
    """
    Return item of list 'l' at position 'pos' if it exists, otherwise return the 'filler' element.
    Example:
        get_item_or_filler(['a', 'b', 'c'], 1)
        >>>'b'
        get_item_or_filler(['a', 'b', 'c'], 4)
        >>>''
        get_item_or_filler(['a', 'b', 'c'], 4, 'e')
        >>>'e'
    """
    if len(l) > pos:
        return l[pos]
    else:
        return filler


def extract_first_and_middle_name(s):
    if s is not None:
        s = str(s)
        words = re.findall("[\w'-]+", s.lower())  # extract all words; don't remove '-'
        first_name = get_item_or_filler(words, 0)
        first_name = ignore_initials(first_name)
        middle_name = get_item_or_filler(words, 1)
        middle_name = ignore_initials(middle_name)
    else:
        first_name, middle_name = '', ''

    return first_name, middle_name


def prepare_authors_and_affiliations_df(df):
    """takes DF with complex column 'authors' and returns a flattened DF"""
    df.authors = df.authors.map(lambda x: x if isinstance(x, list) else [x])
    authors_dict = df[['identifier', 'authors']].to_dict(orient='records')
    flat = []
    for item in authors_dict:
        authors = item['authors']
        for i in range(len(authors)):
            flat.append({**{'article_id': item['identifier']}, **{'author_pos': i + 1}, **authors[i]})
    df_authors = pd.DataFrame(flat)
    return add_expected_columns(df_authors, set(COLUMNS_AUTHORSHIP + COLUMNS_AFFILIATIONS))


def prepare_authors_df(df_authors_and_affiliations):
    df_authors = df_authors_and_affiliations[COLUMNS_AUTHORSHIP]
    df_authors['first_name'], df_authors['middle_name'] = zip(*df_authors.forenames.map(extract_first_and_middle_name))
    return df_authors


def prepare_affiliations(df):
    df_aff = df[pd.notnull(df.affiliation)]
    df_aff.affiliation = df_aff.affiliation.map(lambda x: x if isinstance(x, list) else [x])

    affs_dict = df_aff[COLUMNS_AFFILIATIONS].to_dict(orient='records')
    flat = []
    for item in affs_dict:
        affs = item['affiliation']
        for aff in affs:
            flat.append(
                {**{'article_id': item['article_id']}, **{'author_pos': item['author_pos']}, **{'affiliation': aff}})
    data_frame = pd.DataFrame(flat)
    return add_expected_columns(data_frame, COLUMNS_AFFILIATIONS)


def apply_changes_to_db(ids, df_articles, df_authors, df_affiliations):
    delete_from_db(ids)
    insert_into_table(df_articles, TABLE_ARTICLE, COLUMNS_ARTICLES)
    insert_into_table(df_authors, TABLE_AUTHORSHIP, COLUMNS_AUTHORSHIP)
    insert_into_table(df_affiliations, TABLE_AFFILIATION, COLUMNS_AFFILIATIONS)


def delete_from_db(ids):
    s = datetime.now()

    id_lst = "'" + "','".join(ids) + "'"

    # data sets in dependent tables will be deleted as well because of cascading
    command = ("""DELETE FROM %s WHERE identifier IN (%s)""" % (TABLE_ARTICLE, id_lst))
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PW)
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
        count = cur.rowcount
        cur.close()
    finally:
        if conn is not None:
            conn.close()
    print('  {} elapsed for deletion of old versions ({} articles)'.format(datetime.now() - s, count))


def move_file_to_folder(file, folder):
    target_key = folder + file.split('/')[-1]
    s3.Object(AWS_S3_BUCKET, target_key).copy_from(CopySource=AWS_S3_BUCKET + '/' + file)
    s3.Object(AWS_S3_BUCKET, file).delete()
    print('  Moved file {} to {}'.format(file, target_key))


def handle_deletions(filename):
    # deletions are stored in separate files ('missing_metadata*') -> delete IDs AFTER handling of other documents

    print('{} ##### Begin import of deletions file {}'.format(datetime.now(), filename))

    local_file_path = download_json_file(filename)
    df = df_from_json_file(local_file_path)
    delete_from_db(df['identifier'].values.tolist())

    move_file_to_folder(filename, ARCHIVE_FOLDER_DELETIONS)

    print('{} ***** End import of deletions file {}'.format(datetime.now(), filename))


def do_import():
    LOCK.lock()

    for obj in BUCKET.objects.filter(Prefix=PREFIX_FILE, Delimiter='/'):
        import_json_dump_into_db(obj.key)

    for obj in BUCKET.objects.filter(Prefix=PREFIX_FILE_DELETIONS, Delimiter='/'):
        handle_deletions(obj.key)

    LOCK.unlock()


def lambda_handler(event, context):
    """lambda handler syntax; requires event and context variables"""
    do_import()


if __name__ == '__main__':
    do_import()


def insert_into_table(df, table_name, columns):
    """Inspired by: https://stackoverflow.com/a/47984180/7740194"""

    df_ordered = df[columns]  # ensure column order
    s = datetime.now()

    conn = ENGINE.raw_connection()
    with conn.cursor() as cur:
        output = io.StringIO()
        df_ordered.to_csv(output, index=False)
        output.seek(0)
        # contents = output.getvalue()

        cols = ', '.join([f'{col}' for col in columns])
        sql = f'COPY {table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
        cur.copy_expert(sql, output)
        count = cur.rowcount
    conn.commit()
    print('  {} elapsed for insertion of {} rows into table {}'.format(datetime.now() - s, count, table_name))