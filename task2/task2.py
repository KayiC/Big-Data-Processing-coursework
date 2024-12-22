import sys, string
import os
import math
import socket
# import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from datetime import datetime

from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, \
    year, countDistinct, expr, round, unix_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("task2") \
        .config("spark.jars", "/correct/path/to/graphframes-0.8.2-spark3.0-s_2.12.jar")\
        .getOrCreate()

    blocks_schema = StructType([
        StructField("number", IntegerType(), True),
        StructField("hash", StringType(), True),
        StructField("parent_hash", StringType(), True),
        StructField("nonce", LongType(), True),
        StructField("sha3_uncles", StringType(), True),
        StructField("logs_bloom", StringType(), True),
        StructField("transactions_root", StringType(), True),
        StructField("state_root", StringType(), True),
        StructField("receipts_root", StringType(), True),
        StructField("miner", StringType(), True),
        StructField("difficulty", LongType(), True),
        StructField("total_difficulty", DoubleType(), True),
        StructField("size", IntegerType(), True),
        StructField("extra_data", StringType(), True),
        StructField("gas_limit", LongType(), True),
        StructField("gas_used", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("transaction_count", IntegerType(), True),
        StructField("base_fee_per_gas", LongType(), True),
    ])

    transactions_schema = StructType([
        StructField("hash", StringType(), True),
        StructField("nonce", LongType(), True),
        StructField("block_hash", StringType(), True),
        StructField("block_number", LongType(), True),
        StructField("transaction_index", IntegerType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("value", LongType(), True),
        StructField("gas", LongType(), True),
        StructField("gas_price", LongType(), True),
        StructField("input", StringType(), True),
        StructField("block_timestamp", LongType(), True),
        StructField("max_fee_per_gas", LongType(), True),
        StructField("max_priority_fee_per_gas", LongType(), True),
        StructField("transaction_type", IntegerType(), True)
    ])

    try:
        s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
        s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
        s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        s3_bucket = os.environ['BUCKET_NAME']
    
        hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
        hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
        hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
        hadoopConf.set("fs.s3a.path.style.access", "true")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
        blocks_path = f"s3a://{os.environ['DATA_REPOSITORY_BUCKET']}/ECS765/ethereum/blocks.csv"
        transactions_path = f"s3a://{os.environ['DATA_REPOSITORY_BUCKET']}/ECS765/ethereum/transactions.csv"

        blocks_df = spark.read.csv(blocks_path, header=True, schema=blocks_schema)
        transactions_df = spark.read.csv(transactions_path, header=True, schema=transactions_schema)

        # Q1
        blocks_df.printSchema()
        transactions_df.printSchema()
        

        # Q2
        new_blocks_df = blocks_df.orderBy("size", ascending=False)
        ordered_blocks_df = new_blocks_df.withColumnRenamed("size", "total_size")
        ordered_blocks_df.select("miner", "total_size").show(10, truncate=False)
        

        # Q3
        formatted_date_blocks_df = blocks_df.withColumn('formatted_date', date_format(from_unixtime(blocks_df['timestamp']), 'yyyy-MM-dd')) 
        formatted_date_blocks_df.select('timestamp', 'formatted_date').show(10, truncate=False)
        

        # Q4
        joined_df = transactions_df.join(formatted_date_blocks_df, transactions_df.block_hash == formatted_date_blocks_df.hash, "inner")
        merged_count = joined_df.count()
        print(f"The number of lines is: {merged_count}")
        sys.stdout.flush()
        

        # Q5
        september_date_df = joined_df.filter((col('formatted_date') >= '2015-09-01') & (col('formatted_date') <= '2015-09-30'))
        grouped_september_df = september_date_df.groupBy('formatted_date')\
            .agg(count('timestamp').alias('block_count'),
                 countDistinct('from_address').alias('unique_senders_count_number')
            )
        ordered_september_df = grouped_september_df.orderBy('formatted_date')
        ordered_september_df.select("formatted_date", "block_count", "unique_senders_count_number").show(truncate=False)
        

        ''' # The script used for plotting
        now = datetime.now()
        date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
        ordered_september_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/september_data_" + date_time + ".csv", header=True)

        # the dataframe become nested struct
        nested_ordered_september_df = ordered_september_df.toPandas() 
        
        # Plotting block_count histogram 
        plt.bar(nested_ordered_september_df['formatted_date'], nested_ordered_september_df['block_count'])
        plt.xlabel('Date') 
        plt.ylabel('Block Count')
        plt.title('Block Count per Day in September 2015')
        plt.xticks(rotation=90)
        plt.savefig('September_2015_blocks.png', format='png')
        plt.show()
        
        # Plotting unique_senders_count_number histogram 
        plt.bar(nested_ordered_september_df['formatted_date'], nested_ordered_september_df['unique_senders_count_number'])
        plt.xlabel('Date') 
        plt.ylabel('Unique Senders Count Number')
        plt.title('Unique Senders Count per Day in September 2015')
        plt.xticks(rotation=90)
        plt.savefig('Unique_senders_count_number_2015.png', format='png')
        plt.show()
        '''

        # Q6
        october_date_df = joined_df.filter((col('formatted_date') >= '2015-10-01') & (col('formatted_date') <= '2015-10-31'))
        zero_oct_data_df = october_date_df.filter(october_date_df.transaction_index == 0)
        new_october_date_df = zero_oct_data_df.withColumn('transaction_fee', col("gas").cast('double') * col("gas_price").cast('double'))
        
        grouped_october_date_df = new_october_date_df.groupBy('formatted_date')\
                    .agg(sum('transaction_fee').alias("total_transaction_fee"),
                         count('timestamp').alias('block_count')
                        )
        ordered_october_date_df = grouped_october_date_df.orderBy('formatted_date')
        ordered_october_date_df.select("formatted_date", "total_transaction_fee").show(truncate=False)

        '''# The script used for plotting
        ordered_october_date_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/october_data_" + date_time, header=True)

        # the dataframe become nested struct
        nested_ordered_october_date_df = ordered_october_date_df.toPandas() 

        # Plotting block_count histogram 
        plt.bar(nested_ordered_october_date_df['formatted_date'], nested_ordered_october_date_df['block_count'])
        plt.xlabel('Date') 
        plt.ylabel('Block Count')
        plt.title('Block Count per Day in October 2015')
        plt.xticks(rotation=90)
        plt.savefig('October_2015_blocks.png', format='png')
        plt.show()

        # Plotting total_transaction_fee histogram 
        plt.bar(nested_ordered_october_date_df['formatted_date'], nested_ordered_october_date_df['total_transaction_fee'])
        plt.xlabel('Date') 
        plt.ylabel('Total Transaction Fee')
        plt.title('Total Transaction Fee per Day in October 2015')
        plt.xticks(rotation=90)
        plt.savefig('October_2015_Total_Transaction_Fee.png', format='png')
        plt.show()
        '''

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        spark.stop()