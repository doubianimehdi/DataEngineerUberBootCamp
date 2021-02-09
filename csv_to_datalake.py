#! usr/bin/python
# -*- coding: ISO-8859-1 -*-
# [START dataproc_pyspark_bigquery]
"""
Ce code Pyspark a plusieurs fonctions.
Il va inscrire dans le Data Lake (Big Query) tous les trajets normalisÃ©s effectuÃ©s.
Connecteurs Spark :
- BigQuery : gs://spark-lib/bigquery/spark-bigquery-latest.jar
! Cela va prendre plus de deux heures !
"""
import pyspark.sql.functions as func

from pyspark.sql import SparkSession, SQLContext

# TODO : Inscrire le nom du bucket oÃ¹ sont stockÃ©s les fichiers CSV
BUCKET_NAME = "formation-mehdi-blent-2020"

BUCKET = "gs://{}".format(BUCKET_NAME)

# La table oÃ¹ seront stockÃ©s les trajets
TARGET_TABLE = "datalake.trips"

spark = SparkSession \
    .builder \
    .appName("PySpark") \
    .getOrCreate()

sc = spark.sparkContext
sql_c = SQLContext(spark.sparkContext)

# Chargement des fichiers CSV et normalisation
columns_names = "vendor_id,pickup_datetime,dropoff_datetime,passenger_count," \
    + "trip_distance,rate_code,store_and_fwd_flag,pickup_longitude,pickup_latitude," \
    + "dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount," \
    + "tolls_amount,improvement_surcharge,total_amount"
columns_names = columns_names.split(",")

print("Loading files 2010 -> 2014 ...")
data = sql_c.read.csv(
    BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_201{0,1,2,3,4}-*.csv",
    header=True,
    sep=",")
print("Normalizing ...")
data = data \
    .withColumn('improvement_surcharge', func.lit(0)) \
    .withColumnRenamed('surcharge', 'extra')
data = data.select(columns_names)

print("Loading files 2015-01 -> 2015-06 ...")
tmp = sql_c.read.csv(
    BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-{01,02,03,04,05,06}.csv",
    header=True,
    sep=",")
print("Normalizing ...")
tmp = tmp.toDF(*columns_names)
data = data.union(tmp)

print("Loading files 2015-07 -> 2016-06 ...")
tmp = sql_c.read.csv(
    BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-{07,08,09,10,11,12}.csv",
    header=True,
    sep=",")
tmp = tmp.union(
    sql_c.read.csv(
        BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-{01,02,03,04,05,06}.csv",
        header=True,
        sep=","
    )
)
tmp = tmp.toDF(*columns_names)
data = data.union(tmp)


print("Loading files 2016-07 -> 2018 ...")
tmp = sql_c.read.csv(
    BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2016-{07,08,09,10,11,12}.csv",
    header=True,
    sep=",")
tmp = tmp.union(
    sql_c.read.csv(
        BUCKET + "/s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_201{7,8}-*.csv",
        header=True,
        sep=","
    )
)

# Ã‰tape de normalisation
print("Normalization ...")
new_columns_names = columns_names + ['PULocationID', 'DOLocationID', 'with_areas']
data = data \
    .withColumn('PULocationID', func.lit(None)) \
    .withColumn('DOLocationID', func.lit(None)) \
    .withColumn('with_areas', func.lit(False)) \
    .select(new_columns_names) # Important de rÃ©-ordonner les colonnes
tmp = tmp \
    .withColumn('pickup_longitude', func.lit(None)) \
    .withColumn('pickup_latitude', func.lit(None)) \
    .withColumn('dropoff_longitude', func.lit(None)) \
    .withColumn('dropoff_latitude', func.lit(None)) \
    .withColumn('with_areas', func.lit(True)) \
    .withColumnRenamed('VendorID', 'vendor_id') \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
    .withColumnRenamed('RatecodeID', 'rate_code') \
    .select(new_columns_names)
data_final = data.union(tmp)

print("Casting columns")
data_final = data_final \
    .withColumn("pickup_datetime", func.to_date("pickup_datetime")) \
    .withColumn("dropoff_datetime", func.to_date("dropoff_datetime"))

data_final = data_final \
    .withColumn("passenger_count", data_final.passenger_count.cast("int")) \
    .withColumn("trip_distance", data_final.trip_distance.cast("float")) \
    .withColumn("payment_type", data_final.payment_type.cast("float")) \
    .withColumn("pickup_longitude", data_final.pickup_longitude.cast("float")) \
    .withColumn("pickup_latitude", data_final.pickup_latitude.cast("float")) \
    .withColumn("dropoff_longitude", data_final.dropoff_longitude.cast("float")) \
    .withColumn("dropoff_latitude", data_final.dropoff_latitude.cast("float")) \
    .withColumn("fare_amount", data_final.fare_amount.cast("float")) \
    .withColumn("extra", data_final.extra.cast("float")) \
    .withColumn("mta_tax", data_final.mta_tax.cast("float")) \
    .withColumn("tip_amount", data_final.tip_amount.cast("float")) \
    .withColumn("tolls_amount", data_final.tolls_amount.cast("float")) \
    .withColumn("improvement_surcharge", data_final.improvement_surcharge.cast("float")) \
    .withColumn("total_amount", data_final.total_amount.cast("float")) \
    .withColumn("PULocationID", data_final.PULocationID.cast("int")) \
    .withColumn("DOLocationID", data_final.DOLocationID.cast("int"))

print("Final schema")
data_final.printSchema()

# Le bucket de transition
spark.conf.set('temporaryGcsBucket', BUCKET_NAME)

# Ã‰criture 
data_final.write.format('bigquery') \
    .option('table', TARGET_TABLE) \
    .save()