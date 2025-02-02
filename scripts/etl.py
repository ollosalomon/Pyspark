from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, min, max, when, lag
from pyspark.sql.window import Window
import psycopg2

# Initialisation de Spark
spark = SparkSession.builder.appName("Vehicle_Failure_Analysis").getOrCreate()

# Charger le dataset
df = spark.read.csv("/usr/local/airflow/data/vehicle_sensor_data.csv", header=True, inferSchema=True)

# Analyse descriptive
df.describe().show()
df.select(
    mean("temperature_engine").alias("Mean_Temperature"),
    stddev("temperature_engine").alias("Std_Temperature"),
    min("temperature_engine").alias("Min_Temperature"),
    max("temperature_engine").alias("Max_Temperature")
).show()

# Détection et traitement des valeurs manquantes
df = df.fillna({"temperature_engine": df.select(mean("temperature_engine")).collect()[0][0]})
df = df.dropna(thresh=int(len(df.columns) * 0.5))  # Supprime les lignes avec +50% de NaN

# Détection et gestion des valeurs aberrantes (Outliers)
Q1, Q3 = df.approxQuantile("temperature_engine", [0.25, 0.75], 0.01)
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

df = df.withColumn("outlier_temperature", when((col("temperature_engine") < lower_bound) | (col("temperature_engine") > upper_bound), 1).otherwise(0))
df = df.filter((col("temperature_engine") >= lower_bound) & (col("temperature_engine") <= upper_bound))

# Feature Engineering
window_spec = Window.partitionBy("vehicle_id").orderBy("timestamp")

df = df.withColumn("temperature_variation", col("temperature_engine") - lag("temperature_engine", 1).over(window_spec))
df = df.withColumn(
    "risk_score",
    (col("temperature_engine") / 100) * 2 +
    (1 - col("tire_pressure") / 3) * 2 +
    (col("brake_status") * 3)
)

# Connexion à PostgreSQL et insertion des données
conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="airflow-db",
    port="5432"
)
cur = conn.cursor()

# Création de la table
cur.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_analysis (
        vehicle_id TEXT,
        timestamp TIMESTAMP,
        temperature_engine DOUBLE PRECISION,
        tire_pressure DOUBLE PRECISION,
        battery_voltage DOUBLE PRECISION,
        speed_kmh INT,
        brake_status INT,
        failure_detected INT,
        outlier_temperature INT,
        temperature_variation DOUBLE PRECISION,
        risk_score DOUBLE PRECISION
    )
""")
conn.commit()

# Insérer les données dans PostgreSQL
for row in df.collect():
    cur.execute("""
        INSERT INTO vehicle_analysis (
            vehicle_id, timestamp, temperature_engine, tire_pressure, battery_voltage, 
            speed_kmh, brake_status, failure_detected, outlier_temperature, 
            temperature_variation, risk_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["vehicle_id"], row["timestamp"], row["temperature_engine"], row["tire_pressure"],
        row["battery_voltage"], row["speed_kmh"], row["brake_status"], row["failure_detected"],
        row["outlier_temperature"], row["temperature_variation"], row["risk_score"]
    ))

conn.commit()
conn.close()
spark.stop()
print("✅ Données sauvegardées dans PostgreSQL")
