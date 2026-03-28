# Contents
- [Setup](#setup)
- [Tutorials](#tutorials)

# Setup

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### Fetch docker compose 
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.5/docker-compose.yaml'
```



### Setting the right Airflow user

```bash 
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


### Initialize airflow.cfg

```bash
docker compose run airflow-cli airflow config list
```


### Initialize the database

```bash
docker compose up airflow-init
```


### Cleaning-up the environment

```bash
# Run in the directory where you downloaded the docker-compose.yaml file
docker compose down --volumes --remove-orphans

# Remove the entire directory where you downloaded the docker-compose.yaml file
rm -rf '<DIRECTORY>'

# Run through guide from the very beginning, starting by redownloading the docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.5/docker-compose.yaml'
```


### Accessing the environment
- CLI Commands
    ```bash
    docker compose run airflow-worker airflow info
    ```

    On Linux and Mac OS, can use airflow wrapper script
    ```bash
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.5/airflow.sh'
    chmod +x airflow.sh

    ./airflow.sh info
    
    # Enter interactive bash shell
    ./airflow.sh bash

    # Enter Python container
    ./airflow.sh python
    ```
    
- Web Interface
    - http://localhost:8080
    - login: airflow
    - password: airflow
- REST API
    - http://localhost:8080
    - login: airflow
    - password: airflow

    ```bash
    ENDPOINT_URL="http://localhost:8080"
    curl -X GET --user "airflow:airflow" "${ENDPOINT_URL}/api/v1/pools"
    ```

# Tutorials
[Airflow 101: Building Your First Workflow](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)

[Pythonic Dags with the TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)

[Building a Simple Data Pipeline](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html)

