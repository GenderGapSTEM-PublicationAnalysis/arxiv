# README #

Repository for harvesting, processing, storing and analysis of data from <https://arxiv.org> as part of
the 3-year ICSU-project *A Global Approach to the Gender Gap in Mathematical and Natural Sciences:
How to Measure It, How to Reduce It?* (https://icsugendergapinscience.org/)

# Python environment

This code runs under Python 3.6. The packages required for the project are stored in `requirements.txt`.
The recommended way to run the code is in a virtual environment. For details on how to install a
virtual environment from the file `requirements.txt`, see <http://docs.python-guide.org/en/latest/dev/virtualenvs/>.

The scripts for harvesting and importing data can be run as AWS Lambda functions or on any server.

# Harvester Script

The script `etl_update_batches.py` fetches batches of data newly added to the
database of <https://arxiv.org>. It is intended to be executed every day once by an AWS Lambda.

The XML files returned by arXiv harvesting endpoint are flattened and stored as JSON files
in an AWS S3 bucket.


To fetch all metdata currently available in arXiv, or to fetch a large batch,
the script can be started with it's main method.

There are two types of files, `metadata_*` and `missing_metadata_*`.
The files prefixed with `metadata` contain metadata on newly created articles as well as updates of older ones.
Metadata of deleted articles are stored in files prefixed with `missing_metadata`.


# Importer Script

The script `import_file_to_db.py` imports data from the files created by the harvester script
`etl_update_batches.py` into a relational database. It is intended to be executed every day once by an AWS Lambda.

The files prefixed with `metadata` contain metadata of articles newly added to the arXiv
but also updates of older ones.
The database design is such that newer versions replace older ones, hence it is crucial to maintain
the order of updates.

Imported files are moved to another "directory" inside the S3 bucket.

The `missing_metadata*` files are imported after all `metadata*` files.

A "lock" file mechanism is used to prevent subsequent executions of the
script after an error has occurred. Once the cause of the error has been
fixed, the lock file needs to be removed manually. The script will
continue to import the files in the right order when started the next time.

## Creation of AWS Lambda for `import_file_to_db.py`

**Problem:**

Some of the dependencies contain compiled code that currently does not work
out of the box in AWS Lambda.

- **`pandas`:** (dependency `numpy` must be compiled on an AWS machine,
see: https://stackoverflow.com/questions/43877692/pandas-in-aws-lambda-gives-numpy-error?rq=1)
- **`psycopg2`:** see https://stackoverflow.com/questions/36607952/using-psycopg2-with-lambda-to-update-redshift-python#36608956

**Solution:**

Checked out projects from github where someone had already built the libraries.

### Creation of the AWS Lambda zip-file

Cloned the projects `aws-lambda-py3.6-pandas-numpy` and `awslambda-psycopg2` alongside this project:
```
BASE_DIR=<path/to/directory/containing/this/project>
cd ${BASE_DIR}
git clone https://github.com/pbegle/aws-lambda-py3.6-pandas-numpy.git
git clone https://github.com/jkehler/awslambda-psycopg2.git
```


Activated the virtualenv of this project:
```
workon <name_of_virtualenv>
```

Added scripts and dependencies:

```
# add our scripts to the zip from `aws-lambda-py3.6-pandas-numpy`
cd ${BASE_DIR}/arxiv/
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip scripts/import_file_to_db.py
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip naive_s3_lock.py
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip config.py
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip db_constants.py
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip scripts/__init__.py
# add dependency psycopg2
cp -R ${BASE_DIR}/awslambda-psycopg2/with_ssl_support/psycopg2-3.6/ /tmp/psycopg2
cd /tmp/
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip psycopg2*
# add dependency sqlalchemy
cd $VIRTUAL_ENV/lib/python3.6/site-packages/
zip -ur ${BASE_DIR}/aws-lambda-py3.6-pandas-numpy/lambda.zip sqlalchemy*
```


# Preparation of the database

The script `prepare_database.py` connects to a PostgreSQL server and
creates a database, the tables that the import script expects
and a database user with rights on this tables (but no higher privileges).


# Configuration and passwords

Confidential data has been removed from the configuration files
`config.py` and `config_db_admin.py`. Remember to add it prior
deploying and running the scripts.
(One can also set them as environment variables within the AWS Lambda configuration and remove them from code.)


