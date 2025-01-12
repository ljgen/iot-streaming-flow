import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from cassandra_connection import create_keyspace, create_table, cassandra_conn

# Kafka Configuration
KAFKA_BROKER_LOCAL = 'localhost:9092'
KAFKA_TOPIC = 'iot_data'

# Cassandra Configuration
CASSANDRA_KEYSPACE = 'sensor_data'
CASSANDRA_TABLE = 'iot'
CASSANDRA_HOST_LOCAL = 'localhost'
CASSANDRA_PORT = '9042'

def connect_to_kafka(s_conn):
    df = None
    try:
        df = (s_conn.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', KAFKA_BROKER_LOCAL)
              .option('subscribe', KAFKA_TOPIC)
              .option('startingOffsets', 'earliest')
            #   .option('failOnDataLoss', 'true')
            #   .option("startingOffsets", "latest")  # Start reading from the latest offsets
              .option("failOnDataLoss", "false")     # Fail if data loss is detected              
              .load())
        df.printSchema()

        print("Kafka dataframe created successfully")

    except Exception as e:
        logging.error(f"Kafka dataframe could not be created because: {e}")

    return df


def spark_connection():
    conn = None
    try:
        conn = (SparkSession.builder
                .appName('RealTimePipeline')
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,'
                                               'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')
                .config('spark.cassandra.connection.host', CASSANDRA_HOST_LOCAL)
                # .config('spark.cassandra.connection.port', CASSANDRA_PORT)
                .getOrCreate())

        conn.sparkContext.setLogLevel("INFO")

        print("Spark connection created successfully!")

    except Exception as e:
        logging.error(f"Could not create spark connection due to {e}")

    return conn


def transform_kafka_data(s_df):
    transformed_df = None

    schema = StructType([
        StructField('device_id', StringType(), True),
        StructField('timestamp', DoubleType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('humidity', DoubleType(), True)
    ])

    try:
        transformed_df = s_df.selectExpr("CAST(value AS STRING)").select(from_json(col('value'), schema).alias('data')).select("data.*")

        print("Kafka data transformed successfully!")
        transformed_df.printSchema()

    except Exception as e:
        logging.error(f"Data transformation failed: {e}")

    return transformed_df


if __name__ == "__main__":
    spark_conn = spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = transform_kafka_data(spark_df)
        cs_conn = cassandra_conn()
        
        if cs_conn is not None:
            create_keyspace(cs_conn)
            create_table(cs_conn)
        
            logging.info("Streaming is being started...")
        
            streaming_query = (selection_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "/tmp/checkpoint")
                               .option("keyspace", CASSANDRA_KEYSPACE)
                               .option("table", CASSANDRA_TABLE)
                               .outputMode("append")
                               .start())
        
            streaming_query.awaitTermination()
        
            logging.info("Streaming query terminated.") 