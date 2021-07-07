from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics
import sys
import re

prefix = "ontimeperformance"
size = "small"

def trim(df):
    return df.toDF(*[re.sub('^[ \t]+|[ \t]+$', '', x) for x in df.columns])

def airlines_rankings(spark_session, flights_path, airlines_path, aircrafts_path, country):
    flights = spark_session.read.csv(
        flights_path,
        inferSchema=True,
        header=True
    )
    flights = trim(flights).select(F.col('carrier_code'), F.col('tail_number'))

    airlines = (
        spark_session.read.csv(
            airlines_path,
            inferSchema=True,
            header=True
        ).filter(F.col('country') == country).drop('country')
    )

    aircrafts = (
        spark_session.read.csv(
            aircrafts_path,
            inferSchema=True,
            header=True
        ).select([
            'tailnum',
            'manufacturer',
            'model'
        ])
    )

    aircrafts = aircrafts.withColumn(
        'model', F.regexp_replace(F.col('model'), '-', '')
    ).withColumn('model', F.regexp_extract(F.col('model'), '.+?(?=\d)\d{2}', 0))


    airlines_aircrafts = (
        airlines.join(
            flights,
            on='carrier_code',
            how='left_outer'
        )
        .filter(airlines.name.isNotNull())
        .join(
            aircrafts,
            flights.tail_number == aircrafts.tailnum,
            'inner'
        ).dropna()
        .groupBy('name', 'manufacturer', 'model')
        .count()
    )

    airline_ranked_models = airlines_aircrafts.withColumn(
        "rank", F.rank().over(Window.partitionBy('name').orderBy(F.desc('count')))
    ).filter(F.col('rank') <= 5)

    result = (
        airline_ranked_models.withColumn('aircraft', 
            F.concat(
                F.col('manufacturer'), 
                F.lit(" "), F.col('model')
            )
        ).orderBy('rank', descending=False)
    ).groupBy('name').agg(
        F.collect_list(F.struct('aircraft')).alias('aircrafts_per_airline')
    ).orderBy('name')

    output = ""
    for (name, aircrafts) in result.collect():
        output += f"{name} \t["
        for i in range(0, len(aircrafts)-1):
            output += "%s, " % aircrafts[i]
        output += "%s]\n" % aircrafts[-1]

    print(output, file=sys.stdout)


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder.appName('airlines')
        .getOrCreate()
    )

    airlines_rankings(
        spark, 
        f"./data/{prefix}_flights_{size}.csv", 
        f"./data/{prefix}_airlines.csv",
        f"./data/{prefix}_aircrafts.csv",
        country="United States"
    ) 

    spark.stop()
