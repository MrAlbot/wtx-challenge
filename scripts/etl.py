import os
import re
from pyspark.sql import *
from pyspark.sql.functions import *


def average_importing_value_by_country():
    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'csv/trades_qa.csv'
    scrapedPortsFileName = 'csv/port_info_final.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe
    trades_df = spark.read.option("header", True).option("sep", ';').csv(tradesFileName)

    avg_imported_value_df = trades_df.groupBy(col("destination_country")).agg(
        avg("value_fob_usd").alias("avg_imported_value"))
    avg_imported_value_df.show(10)


def average_exporting_value_by_country():
    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'csv/trades_qa.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe
    trades_df = spark.read.option("header", True).option("sep", ';').csv(tradesFileName)

    avg_exported_value_df = trades_df.groupBy(col("source_country")).agg(
        avg("value_fob_usd").alias("avg_exported_value"))
    avg_exported_value_df.show(10)


def total_importing_value_by_country():
    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'csv/trades_qa.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe
    trades_df = spark.read.option("header", True).option("sep", ';').csv(tradesFileName)

    total_imported_value_df = trades_df.groupBy(col("destination_country")).agg(
        sum("value_fob_usd").alias("total_imported_value"))
    total_imported_value_df.show(10)


def total_exporting_value_by_country():
    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'csv/trades_qa.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe
    trades_df = spark.read.option("header", True).option("sep", ';').csv(tradesFileName)

    total_exported_value_df = trades_df.groupBy(col("source_country")).agg(
        sum("value_fob_usd").alias("total_exported_value"))
    total_exported_value_df.show(10)


# This function counts the top ports used in transactions.
# It calculates the top seaports used for exports and imports and sums their contributions
def popular_shipping_routes():
    spark = SparkSession.builder.getOrCreate()

    tradesFileName = 'csv/trades_qa.csv'

    # load both Dataframes and enrich the scraped ports with the lat lon dataframe
    trades_df = spark.read.option("header", True).option("sep", ';').csv(tradesFileName)

    popular_source_df = trades_df.groupBy(col("source_port")).agg(count(lit(1)).alias("source_count"))
    popular_destination_df = trades_df.groupBy(col("destination_port")).agg(count(lit(1)).alias("destination_count"))
    popular_df = popular_source_df.join(popular_destination_df,
                                        popular_source_df['source_port'] == popular_destination_df['destination_port'],
                                        'outer'). \
        withColumn("port", when(col("source_port").isNull(), col("destination_port")).otherwise(col("source_port"))).\
        withColumn("source_count", when(col("source_count").isNull(), lit(0)).otherwise(col("source_count"))).\
        withColumn("destination_count", when(col("destination_count").isNull(), lit(0)).otherwise(col("destination_count"))).\
        withColumn("total_count", col("source_count") + col("destination_count")). \
        select(coalesce("source_port", "destination_port").alias("port"), "total_count"). \
        groupBy("port").agg(sum("total_count").alias("total_count")).orderBy(col("total_Count").desc())
    popular_df.show(10)

