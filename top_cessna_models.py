from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark)
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
    cessna_flights_count.show()

if __name__ == "__main__":
    spark = (SparkSesssion
             .builder.appName('top_cessna_models')
             .getOrCreate())

    dbfs_fileStore_prefix = "/FileStore/tables"
    prefix = "ontimeperformance"
    size = "small"

    top_cessna_models(spark, 
                      f"{dbfs_fileStore_prefix}/{prefix}_flights_{size}.csv", 
                      f"{dbfs_fileStore_prefix}/{prefix}_aircrafts.csv") 
    spark.stop()










