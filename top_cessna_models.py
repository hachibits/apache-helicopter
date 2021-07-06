from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
import re
import sys

def top_cessna_models(spark_session, flights_path, aircrafts_path):
    aircrafts = (
        spark_session.read.csv(aircrafts_path, inferSchema=True, header=True)
                     #.select(F.col('tailnum').alias('tail_number'))
    )

    flights = spark_session.read.csv(flights_path, inferSchema=True, header=True)

    cessna_models = aircrafts.filter(
        F.col('manufacturer') == "CESSNA"
    ).withColumn('model', F.regexp_extract(F.col('model'), "\d{3}", 0))

    flights_count = (
        flights.select(
            flights.colRegex("^[ \t]+|[ \t]+$")
        )
        .groupBy('tail_number')
        .count()
    )

    cessna_flights_count = (
        F.broadcast(cessna_models).join(
            flights_count,
            cessna_models.tailnum == flights_count.tail_number,
            how="left"
        )
        .groupBy('model')
        .count()
        .orderBy('count', ascending=False)
        #cessna_models.join(flights_count, 'tail_number', 'inner').drop('tail_number')
        #             .groupBy('model')
        #             .count()
        #             .orderBy('count', ascending=False)
    )

    models = cessna_flights_count.take(3)

    for (model, count) in models:
        print("Cessna %s\t%i" % (model, count), file=sys.stdout)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("top_cessna_models")
        .getOrCreate()
    )

    #stagemetrics = StageMetrics(spark)

    prefix = "ontimeperformance"
    size = "small"
    top_cessna_models(spark, f"./data/{prefix}_flights_{size}.csv", f"./data/{prefix}_aircrafts.csv") 

    spark.stop()
