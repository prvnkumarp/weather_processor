import findspark
import os
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, arrays_zip, explode, to_json, struct
from packages.schema.weather_schema import weather_schema, zipped_columns


def main():
    kafka_bootstrap_servers = "localhost:9092"
    spark = (SparkSession.builder.appName("weather_processor")
             .config("spark.driver.memory", "6g")
             .config("spark.executor.memory", "6g")
             .config("spark.jars.packages", ",".join(packages))
             .master("local[*]")
             .getOrCreate())
    weather_stream = (spark.readStream.format("kafka")
                      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                      .option("subscribe", "weather_raw")
                      .option("startingOffsets", "earliest")
                      .option("failOnDataLoss", "false")
                      .load())
    weather_df = (weather_stream.select(col("value").cast("string").alias("value"))
                  .withColumn("value", from_json(col("value"), weather_schema)))
    weather_df_flatten = (weather_df.select("value.*", "value.hourly.*")
                          .withColumn("zipped", explode(arrays_zip(*zipped_columns)))
                          .withColumnRenamed("admin1", "state")
                          .drop("hourly", "postcodes", "generationtime_ms", "utc_offset_seconds", "admin1_id",
                                "admin2_id", "admin2", "timezone_abbreviation", "feature_code", *zipped_columns)
                          )
    weather_df_final = weather_df_flatten.select("*", "zipped.*").drop("zipped")
    weather_df_value = weather_df_final.select(col("*"), to_json(struct("*")).alias("value"))
    weather_df_value.printSchema()
    logging.info("sending data to kafka")
    (weather_df_value.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
     .option("topic", "weather_raw_flatten")
     .option("checkpointLocation", "./checkpoint")
     .start().awaitTermination())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
        'org.apache.kafka:kafka-clients:2.8.1'
    ]
    findspark.init(os.environ['SPARK_HOME'])
    main()
