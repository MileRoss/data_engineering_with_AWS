#!/bin/bash

/opt/airflow/start-services.sh
/opt/airflow/start.sh
airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin
airflow scheduler