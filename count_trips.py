#! usr/bin/python
# -*- coding: ISO-8859-1 -*-
# [START dataproc_pyspark_daily_areas]
"""
Ce code PySpark va alimenter le Data Lake en comptant le nombre de pickup
et dropoff chaque jour pour chaque zone.
Connecteurs Spark :
- BigQuery : gs://spark-lib/bigquery/spark-bigquery-latest.jar
Commande DataProc :
pyspark --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar
"""
import pyspark.sql.functions as func

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext
sql_c = SQLContext(spark.sparkContext)

# Récupérer tous les trajets du Data Lake ayant les localisations sous forme d'ID
data = spark.read.format('bigquery') \
        .option('table', 'datalake.trips') \
        .load()
data.printSchema()
data_id = data.where(func.col('with_areas') == True)

# Définir un MapReduce pour dénombrer le nombre de départs et d'arrivés dans chaque zone, pour chaque jour.
def map_per_date_area(item):
    pickup = ((item['pickup_datetime'], item['PULocationID'], "num_pickup"), 1)
    dropoff = ((item['dropoff_datetime'], item['DOLocationID'], "num_dropoff"), 1)
    return [pickup, dropoff]

# Opération map : chaque pickup et dropoff est apparu une seule fois
data_count_mapped = data_id.rdd.flatMap(map_per_date_area)
# Opération reduce : on compte le nombre de pickups et dropoff dans la zone
data_count_reduced = data_count_mapped.reduceByKey(lambda x, y: x + y)
data_count = data_count_reduced.collect()
data_count[0]

# Création d'un DataFrame
data_count_df = sql_c.createDataFrame(data_count, ['Key', 'Count'])
data_count_df.printSchema()

# On sélectionne les colonnes qui nous intéressent
data_count_df = data_count_df.select("Key.*", "Count")
data_count_df = data_count_df.toDF("date", "area_id", "type", "count")
data_count_df.take(1)

# Spark pivot
data_count_df_pivot = data_count_df \
    .withColumn("area_id", data_count_df['area_id'].cast("int")) \
    .groupBy("date", "area_id") \
    .pivot("type") \
    .sum("count")

# Le bucket de transition
spark.conf.set('temporaryGcsBucket', "formation-mehdi-blent-2020")

# À l'aide du connecteur JDBC, on alimente la table daily_areas par ajout
data_count_df_pivot.write.format('bigquery') \
    .option('table', 'datalake.daily_areas') \
    .save()