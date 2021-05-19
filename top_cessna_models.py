from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark)
from utils import clean_column_names, evaluate_metrics
from download_helper import download_csv
import re

def top_cessna_models(spark_session, flights_path, aircrafts_path):
    aircrafts = (spark_session.read.csv(aircrafts_path, inferSchema=True, header=True)
                              .select(F.col('tailnum').alias('tail_num')))

    flights = (spark_session.read.csv(flights_path, inferSchema=True, header=True)
                            .select(F.col('tail_number').alias('tail_num')))

    aircrafts_flights = aircrafts.join(flights, 'tail_num', 'left_outer').drop('tail_num')

    cessna_flights = aircrafts_flights.filter(F.col('manufacturer') == 'CESSNA')

    cessna_flights_count = (cessna_flights.groupBy('model')
                                          .count()
                                          .orderBy('count', ascending=False))

    models = clean_column_names(cessna_flights_count).drop('tail_num').take(3)

    for (model, count) in models:
        print("Cessna %s: %i", (model[0:3], count))


if __name__ == "__main__":
    spark = (SparkSesssion
             .builder.appName('top_cessna_models')
             .getOrCreate())

    download_csv()

    prefix = "/data/ontimeperformance"
    size = "small"

    top_cessna_models(spark, 
                      f"{prefix}_flights_{size}.csv", 
                      f"{prefix}_aircrafts.csv") 
    spark.stop()
