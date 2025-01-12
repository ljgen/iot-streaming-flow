# **IoT Stream Flow: Real-Time Data Processing Pipeline**
 A real-time pipeline that simulates IoT data with Python, orchestrates data ingestion to Kafka (in KRaft mode) using Airflow, processes the data with Spark, and stores it in Cassandra.

## **Project Overview**
IoT Stream Flow is a real-time data pipeline that:
- Simulates IoT data using Python.
- Orchestrates data ingestion to Kafka (KRaft mode) using Airflow.
- Streams and processes data in real time with Apache Spark.
- Stores the processed data in Cassandra for efficient querying and analysis.

This project demonstrates an end-to-end IoT data streaming architecture, highlighting the integration of Python, Kafka, Spark, Cassandra, and Airflow.

## **Tech Stack**
- **Python:** Simulates IoT data.
- **Apache Kafka (KRaft mode):** Manages real-time data streaming.
- **Apache Airflow:** Orchestrates IoT data ingestion into Kafka.
- **Apache Spark:** Processes and transforms streamed data.
- **Apache Cassandra:** Stores processed data for querying.
- **Docker Compose:** Manages the containerized environment.

## **Project Architecture**
**Data Generation:**
Simulate IoT data (e.g., sensor readings) with Python.

**Data Ingestion:**
Use Airflow to schedule and stream IoT data to Kafka.

**Data Processing:**
Spark processes the data from Kafka in real time.

**Data Storage:**
Cassandra stores the processed data for scalable querying.

## **How It Works**

![System Architecture "IoT Stream Flow: Real-Time Data Processing Pipeline"](images/iot-stream.jpg)

- **Python** generates simulated IoT data (device IDs, timestamps, sensor readings).
- **Airflow** ingests the data into Kafka.
- **Spark** processes Kafka streams in real time, performing transformations or analytics.
- **Cassandra** stores the final processed data for querying.