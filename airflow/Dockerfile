# Use the official Airflow image as a parent image
FROM apache/airflow:2.8.0

USER airflow

# Copy secrets from local to docker env
COPY dags/secrets.yaml /opt/airflow/dags/

# Copy plugins (aka scripts) from local to docker env
COPY plugins/. /opt/airflow/plugins/

# Copy your requirements file into the image
COPY requirements.txt .

# Install the additional requirements
RUN pip install --no-cache-dir pandas PyYAML requests pymongo

# Create the .env file with AIRFLOW_UID
RUN echo -e "AIRFLOW_UID=50000" > .env

ENV PYTHONPATH ${PYTHONPATH}:/opt/airflow/plugins

USER root
