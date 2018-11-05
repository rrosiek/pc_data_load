import psycopg2
from psycopg2.extras import DictCursor
from urlparse import urlparse

# DATABASE_URL = 'postgresql://username:password@endpoint/database_name'

from data_load.DATA_LOAD_CONFIG import *

LEGACY_DATABASE_URL = 'postgresql://' + POSTGRES_USERNAME + ':' + POSTGRES_PASSWORD + '@' + POSTGRES_SERVER_IP + ':' + POSTGRES_SERVER_PORT + '/' + POSTGRES_DATABASE_NAME
# PRODUCTION_DATABASE_URL = 'postgresql://mira_postgres:yA7mU374ykrB3FZp@miraprod3.cpmzbizljm3l.us-east-1.rds.amazonaws.com:5432/mira_prod'

# DATABASE_URL = os.getenv('DATABASE_URL')
assert LEGACY_DATABASE_URL
# assert PRODUCTION_DATABASE_URL


def database_connection(database_url):
    parsed = urlparse(database_url)
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    database = parsed.path.strip('/')

    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        cursor_factory=DictCursor)
    connection.set_session(autocommit=True)

    return connection

# production_database = database_connection(database_url=PRODUCTION_DATABASE_URL)
pardi_database = database_connection(database_url=LEGACY_DATABASE_URL)


