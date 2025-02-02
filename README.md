# Pipeline ETL avec Airflow, PySpark et PostgreSQL 🚀

Ce projet met en place un environnement **Apache Airflow** orchestrant un pipeline **ETL** avec **PySpark** et **PostgreSQL**. Il permet de traiter et analyser des données issues de capteurs de véhicules autonomes pour détecter des pannes en temps réel.

[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-blue?logo=apache-airflow)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-blue?logo=postgresql)](https://www.postgresql.org/)
[![PySpark](https://img.shields.io/badge/PySpark-orange?logo=apachespark)](https://spark.apache.org/)

---

## **📌 Table des Matières**
- [🚀 Fonctionnalités](#-fonctionnalités)
- [🔧 Prérequis](#-prérequis)
- [📦 Installation et Démarrage](#-installation-et-démarrage)
- [💻 Utilisation](#-utilisation)
- [📊 Pipeline ETL](#-pipeline-etl)
- [⚙️ Création et Gestion des DAGs](#-création-et-gestion-des-dags)
- [📜 Licence](#-licence)

---

## **🚀 Fonctionnalités**
- Orchestration avec **Apache Airflow**.
- Extraction et transformation des données avec **PySpark**.
- Sauvegarde des résultats dans **PostgreSQL**.
- Exécution **dockerisée** via **Docker Compose**.
- Interface Web pour gérer les DAGs Airflow.

---

## **🔧 Prérequis**
Avant de commencer, assure-toi d’avoir installé :
- **Docker** et **Docker Compose** ([Installation](https://docs.docker.com/get-docker/))
- **Git** ([Installation](https://git-scm.com/downloads))

---

## **📦 Installation et Démarrage**
### **1️⃣ Cloner le projet**
```bash
git clone https://github.com/ollosalomon/Pyspark.git
