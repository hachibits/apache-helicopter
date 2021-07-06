from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
import re

def popular_aircraft_types(spark_session, flights_path, airlines_path, aircrafts_path, country):
    flights = spark_session.read.csv(flights_path, inferSchema=True, header=True)
    flights = (
        flights.select([F.col(col).alias(col.replace(' ', '')) for col in flights.columns])
               .select(F.col('carrier_code'), F.col('tail_number'))
    )

    airlines = (
        spark_session.read.csv(airlines_path, inferSchema=True, header=True)
                     .filter(F.col('country') == country)
                     .drop('country')
    )

    aircrafts = (
        spark_session.read.csv(aircrafts_path, inferSchema=True, header=True)
                     .select(F.col('tailnum').alias('tail_number'), F.col('manufacturer'), F.col('model'))
    )

    airlines_flights = airlines.join(flights, 'carrier_code', 'left_outer').filter(airlines.name.isNotNull())

    airlines_aircrafts = (
        airlines_flights.join(aircrafts, 'tail_number', 'full_outer').dropna()
                        .groupBy('name', 'manufacturer', 'model')
                        .count()
    )

    result = airlines_aircrafts.withColumn(
        "rank", F.rank().over(Window.partitionBy('name').orderBy(F.desc('count')))
     )
    #result.show()
    name = result.collect()[0][0]
    line = ''
    i = 0
    for r in result.collect():
        if (f.name == name):
            if (i >= 5):
                continue
            else:
                model = r.model.strip("-")
                line += f"{r.manufacturer} {model}, "
                i += 1
        else:
            i = 0
            print("%s \t[%s]" % (name, line[:-2]))
            name = r.name
            line = ''


if __name__ == "__main__":
    spark = (
        SparkSesssion
        .builder.appName('aircrafts')
        .getOrCreate()
    )

    stagemetrics = StageMetrics(spark)

    prefix = "/data/ontimeperformance"
    size = "small"

    popular_aircraft_types(
        spark, 
        f"{prefix}_flights_{size}.csv", 
        f"{prefix}_airlines.csv",
        f"{prefix}_aircrafts.csv",
        country="United States"
    ) 

    spark.stop()
