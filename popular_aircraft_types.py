from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark)
from utils import clean_column_names, evaluate_metrics
from download_helper import download_csv
import re

def popular_aircraft_types(spark_session, flights_path, airlines_path, aircrafts_path, country):
    flights = spark_session.read.csv(flights_path, inferSchema=True, header=True)
    flights = (flights.select([F.col(col).alias(col.replace(' ', '')) for col in flights.columns])
                      .select(F.col('carrier_code'), F.col('tail_number')))

    airlines = (spark_session.read.csv(airlines_path, inferSchema=True, header=True)
                             .filter(F.col('country') == country)
                             .drop('country'))

    aircrafts = (spark_session.read.csv(aircrafts_path, inferSchema=True, header=True)
                              .select(F.col('tailnum').alias('tail_number'), F.col('manufacturer'), F.col('model')))

    airlines_flights = airlines.join(flights, 'carrier_code', 'left_outer').filter(airlines.name.isNotNull())

    airlines_aircrafts = (airlines_flights.join(aircrafts, 'tail_number', 'full_outer').dropna()
                                                                                       .groupBy('name', 'manufacturer', 'model')
                                                                                       .count())
    result = airlines_aircrafts.sort('name', airlines_aircrafts['count'].desc())


if __name__ == "__main__":
    spark = (SparkSesssion
             .builder.appName('popular_aircraft_types')
             .getOrCreate())

    download_csv()

    prefix = "/data/ontimeperformance"
    size = "small"

    top_cessna_models(spark, 
                      f"{prefix}_flights_{size}.csv", 
                      f"{prefix}_airlines.csv",
                      f"{prefix}_aircrafts.csv",
                      country=input()) 
    spark.stop()
