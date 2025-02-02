FROM puckel/docker-airflow

USER root

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/etl_dag.py /usr/local/airflow/dags/etl_dag.py
COPY scripts/etl.py /usr/local/airflow/scripts/etl.py
COPY data/vehicle_sensor_data.csv /usr/local/airflow/data/vehicle_sensor_data.csv
