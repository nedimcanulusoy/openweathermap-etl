# OpenWeatherMap API - ETL

## Overview
This project orchestrates an ETL pipeline through Apache Airflow, designed to retrieve weather data from the OpenWeatherMap API and initially store it as JSON in MongoDB. Subsequently, the data undergoes a transformation process to conform to a structured format suitable for insertion into a PostgreSQL database. This structured data is then ready for further analysis or integration with other applications.

## Used Technologies
- **Apache Airflow**: Orchestrates the workflow of the ETL process.
- **Python**: Scripts for fetching, transforming, and loading the data.
- **MongoDB**: The initial storage of raw JSON data from the OpenWeatherMap API.
- **PostgreSQL**: The final storage where transformed data is loaded for future use.
- **Docker**: Containerization of the Airflow environment for consistent deployment.

## Files
- **openweather_fetch_api.py**: This script contains the DAG responsible for extracting weather data from the OpenWeatherMap API. It defines the tasks to hit the API endpoint and fetches the latest weather information. After that, these raw data is stored in a MongoDB database.
  
- **openweather_transform.py**: This script transforms the raw JSON data fetched from the OpenWeatherMap API into a structured format that is suitable for storage in the PostgreSQL database. The transformation process includes cleaning, mapping, and aggregating the data.

- **openweather_load.py**: This cctains the DAG for loading the transformed weather data into the PostgreSQL database. This script handles the connection to the database and ensures that the data is loaded correctly.
  
- **openweathermap_dag.py**: This is the main DAG file that orchestrates the execution of the fetching, transforming, and loading tasks. It schedules and sequences the ETL process, ensuring that each step is executed in the correct order and at the right time.


## Setup Instructions

### NOTE: Before starting setup the project, don't forget to create your `secrets.yaml` file under `dags/` folder and fill in required parameters.

- **`secrets.yaml` Template**:

    ```
        api_key: fancy-api-key
        longitude: longitude-of-a-city
        latitude: latitude-of-a-city
        city: name-of-the-best-city
        mongo_uri: mongodb://your-username:your-password@your-host:your-port
        db_name: your-mongodb-database-name
        collection_name: your-mongodb-collection-name

        postgres_uri: postgresql+psycopg2://airflow-username:airflow-password@postgres/airflow-database-name 
        #If you haven't changed anything, these should be set to default value whic is airflow.
        postgres_table_name: your-postgresql-database-name

        log_file_path: logs/fancy-file-name.log
    ```

### A) Python Environment Setup
1. **Create a Python Virtual Environment:**

    Use the `venv` module to create a new virtual environment. Open your terminal and execute:

    ```bash
    python3 -m venv env
    ```
    This command creates a virtual environment named `env`.

2. **Activate the Virtual Environment:**

    You need to activate the virtual environment to use it. Depending on your OS, the activation command differs:

    - For Windows:
      ```bash
      .\env\Scripts\activate
      ```
    
    - For Unix or MacOS:
      ```bash
      source env/bin/activate
      ```

3. **Install the Dependencies:**

    With the virtual environment activated, install the project dependencies using the command:

    ```bash
    pip install -r requirements.txt
    ```
    This installs all the packages defined in `requirements.txt`.

### B) MongoDB with Docker
In the API fecthing stage (aka. Extract), we store json response in MongoDB and therefore we need to initialize it. To do this;

1. **Change directory to `mongodb-docker`:
    ```cd mongodb-docker```

2. **Run docker-compose.yaml**
    ```docker-compose up -d```
    or
    ```docker compose up -d```

### C) Airflow Setup with Docker
Since this project is using external dependencies, it needs to be built with customized docker image and everything is provided inside of `Dockerfile` In order to do this;

1. **Initialize database and Create the first user account**

    ```
    docker-compose up airflow-init
    ```
    or
    ```
    docker compose up airflow-init
    ```

    After initialization is complete, you should see a message like this:

    ```
    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | 2.8.1
    start_airflow-init_1 exited with code 0
    ```

2. **Run build command for our custom docker image**

    ```
    docker-compose build
    ```
    or

    ```
    docker compose build
    ```

3. **Run airflow docker**
    ```
    docker-compose up -d
    ```
    or
    ```
    docker compose up -d
    ```

## Usage
Once the containers are up and running, access the Airflow web interface at `http://localhost:8080`. Trigger the ETL DAG to start the data pipeline. If you don't change anything, username and password will be the default value which is `airflow` for both.


## Documentation References
For further information and to understand the setup and management of the Airflow environment within a Docker context, refer to the following documentation:

1. **Airflow with Docker**: Detailed guide on how to use Apache Airflow with Docker Compose.
   [Airflow Docker Compose Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

2. **Airflow Module Management**: Information on managing Python modules in Apache Airflow.
   [Airflow Modules Management Documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html)
