#!/bin/bash

echo "⏳ Attente de PostgreSQL..."
while ! pg_isready -h airflow-db -p 5432 -U airflow; do
    sleep 2
done
echo "PostgreSQL est prêt !"

echo "Correction des permissions des logs..."
mkdir -p /opt/airflow/logs
chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} /opt/airflow/logs 2>/dev/null || echo "⚠️ Impossible de changer les permissions"
chmod -R 777 /opt/airflow/logs 2>/dev/null || echo " Impossible de changer les permissions"

echo "Initialisation de la base de données Airflow..."
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

echo "Base de données initialisée !"
