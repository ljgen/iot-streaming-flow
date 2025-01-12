from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# initialization
default_args = {
    'owner': 'tipakorn',
    'start_date': days_ago(0)
}

def generate_iot_data():
    import logging
    import json
    import sys
    import time
    import random
    from datetime import datetime, timezone
    from confluent_kafka import Producer, KafkaError

    # Kafka Configuration
    KAFKA_BROKER = 'kafka:29092'
    KAFKA_TOPIC = 'iot_data'

    producer = None

    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})

        duration = 1 * 60 # Set the duration to 1 minutes (300 seconds)
        start_time = time.time()
        count = 0

        devices = ['device_1', 'device_2', 'device_3']

        while time.time() - start_time < duration:
            try:
                data = {
                    'device_id': random.choice(devices),                   
                    'timestamp': time.time(),
                    'temperature': round(random.uniform(20.0, 30.0), 2),
                    'humidity': round(random.uniform(40.0,60.0), 2)
                }
                producer.produce(KAFKA_TOPIC, key=data['device_id'], value=json.dumps(data))            
                print(f"Producing IoT data to Kafka topic '{KAFKA_TOPIC}' : {data}")

                count += 1
                time.sleep(1)
            except KafkaError as e:
                logging.error(f"Error sending data to Kafka: {e} with {data}")
                continue

            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                sys.exit(1)

        print(f"Total recorded items : {count}")

    except Exception as e:
        logging.error(f"Error in the data simulation process: {e}")
        sys.exit(1)

    finally:
        if producer:
            producer.flush()


# Define the DAG
dag = DAG(
    'iot_data_pipeline',
    default_args=default_args,
    description='Fetch iot data to Kafka',
    schedule_interval='*/30 * * * *',
    catchup=False,
    max_active_runs=1
)

# Create Airflow tasks
iot_data_ingestion = PythonOperator(
    task_id='iot_data_ingestion',
    python_callable=generate_iot_data,
    dag=dag,
)

iot_data_ingestion