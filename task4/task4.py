import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode, split, window, col, count, concat_ws
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum, avg, max, when
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("NasaLogSparkStreaming") \
        .config("spark.jars", "/correct/path/to/graphframes-0.8.2-spark3.0-s_2.12.jar")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Question 1 (2 points) TODO: load data by specifying the host value and port value
    logsDF = spark.readStream.format("socket") \
        .option("host", 'stream-emulator.data-science-tools.svc.cluster.local') \
        .option("port", 5551) \
        .option("includeTimestamp", "true") \
        .load()

    logsDF = logsDF.select(explode(split(logsDF.value, " ")).alias("logs"), logsDF.timestamp)

    # Question 2 (2 points) TODO: define a watermark on the timestamp column with a delay of 3 seconds.
    logsDF = logsDF.withWatermark("timestamp", "3 seconds")

    # Q3
    logsDF = logsDF.withColumn('idx', split(logsDF['logs'], ',').getItem(0)) \
        .withColumn('hostname', split(logsDF['logs'], ',').getItem(1)) \
        .withColumn('time', split(logsDF['logs'], ',').getItem(2)) \
        .withColumn('method', split(logsDF['logs'], ',').getItem(3)) \
        .withColumn('url', split(logsDF['logs'], ',').getItem(4)) \
        .withColumn('responsecode', split(logsDF['logs'], ',').getItem(5)) \
        .withColumn('bytes', split(logsDF['logs'], ',').getItem(6))

    '''# Q1, Q2, Q3
    query = logsDF.writeStream.outputMode("append") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .start() 
    '''

    # Q4:
    windowDuration = '60 seconds'
    slideDuration = '30 seconds'

    gifDF = logsDF.filter(col("url").contains("gif"))

    gifCountDF = gifDF.groupBy(window(gifDF.timestamp, windowDuration, slideDuration)) \
        .agg(count("url").alias("gif_count"))

    '''
    query = gifCountDF.writeStream.outputMode("update") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    '''

    # Q5:
    hostnameDF = logsDF.groupBy(window(logsDF.timestamp, windowDuration, slideDuration), logsDF.hostname) \
        .agg(sum('bytes').alias("total_bytes"))

    orderedDF = hostnameDF.orderBy(col("total_bytes"), ascending=False)

    '''
    query = orderedDF.writeStream.outputMode("complete") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .format("console") \
        .start()
    '''

    # Q6:
    responseDF = logsDF.filter((col("method")=="GET") & (col("responsecode") == 200))

    groupedDF = responseDF.groupBy(responseDF.hostname) \
        .agg(count("method").alias("Correct_count"))

    formattedDF = groupedDF.select(col("hostname").alias("window"), "Correct_count")

    query = formattedDF.writeStream.outputMode("complete") \
        .option("numRows", "100000") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .start()
    
    try:
        time.sleep(60)
        query.awaitTermination()
        
    except KeyboardInterrupt:
        query.stop()
        
    finally: 
        spark.stop()


