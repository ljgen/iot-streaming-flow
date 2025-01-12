import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra Configuration
CASSANDRA_HOST_LOCAL = 'localhost'

def create_keyspace(c_conn):
    keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS sensor_data
        WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
            };
    """
    c_conn.execute(keyspace_query)

    print("Keyspace created successfully!")


def create_table(c_conn):
    table_query = """
        CREATE TABLE IF NOT EXISTS sensor_data.iot (
        device_id TEXT,
        timestamp DOUBLE,
        temperature DOUBLE,
        humidity DOUBLE,
        PRIMARY KEY (device_id, timestamp)
        );
    """
    c_conn.execute(table_query)

    print("Table created successfully!")

def cassandra_conn():
    conn = None
    try:
        # Connecting to the cassandra cluster
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster([CASSANDRA_HOST_LOCAL], auth_provider=auth_provider)

        conn = cluster.connect()

        print("Cassandra connection created successfully!")

    except Exception as e:
        logging.error(f"Could not create cassandra connection: {e}")

    return conn