import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("task1")\
        .config("spark.jars", "/correct/path/to/graphframes-0.8.2-spark3.0-s_2.12.jar")\
        .getOrCreate()

    yellow_taxi_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", FloatType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", FloatType(), True),
        StructField("extra", FloatType(), True),
        StructField("mta_tax", FloatType(), True),
        StructField("tip_amount", FloatType(), True),
        StructField("tolls_amount", FloatType(), True),
        StructField("total_amount", FloatType(), True),
        StructField("congestion_surcharge", FloatType(), True),
        StructField("airport_fee", FloatType(), True),
        StructField("taxi_type", StringType(), True)
                                  ])

    taxi_zone_schema = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
                                  ])

    try:
        s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
        s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
        s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        s3_bucket = os.environ['BUCKET_NAME']
    
        hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
        hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
        hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
        hadoopConf.set("fs.s3a.path.style.access", "true")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

        taxi_zone_lookup_path = f"s3a://{s3_bucket}/taxi_zone_lookup.csv"
        yellow_sampledata_path = f"s3a://{s3_bucket}/task1_yellow_sample_dataset.csv"
        yellow_tripdata_path = f"s3a://{os.environ['DATA_REPOSITORY_BUCKET']}/ECS765/nyc_taxi/yellow_tripdata/2023/"
    
        taxi_zone_df = spark.read.csv(taxi_zone_lookup_path, header=True, schema=taxi_zone_schema)
        yellow_sampledata_df = spark.read.csv(yellow_sampledata_path, header=True, schema=yellow_taxi_schema)

        yellow_datasets_files = [f"{yellow_tripdata_path}yellow_tripdata_2023-{month:02d}.csv" for month in range(1, 8)] 
        yellow_tripdata_df = spark.read.csv(yellow_datasets_files, header=True, schema=yellow_taxi_schema)

        # Q1:
        # Sample dataset:
        sample_count = yellow_sampledata_df.count()
        print(f"The number of entries is: {sample_count}")
        sys.stdout.flush()

        # Large dataset
        total_entries = yellow_tripdata_df.count() 
        print(f"Total number of entries in the NYC Yellow Taxi dataframe: {total_entries}")
        sys.stdout.flush()
        
        
        # Q2:
        # Sample dataset:
        # Debug: Show the table before filter
        yellow_sampledata_df.select("tpep_pickup_datetime", "fare_amount", "trip_distance").show(5, truncate=False)
        print("The table is showing the first 5 data rows from the local sample dataset")
        sys.stdout.flush()

        # Filter the data base on the condition
        yellow_sample_filtered_df = yellow_sampledata_df.filter(
                    (col("tpep_pickup_datetime") >= "2023-01-01 00:00:00") &
                    (col("tpep_pickup_datetime") <= "2023-01-01 01:00:00") &
                    (col("fare_amount") > 50) &
                    (col("trip_distance") < 1)
                )

        # Debug: Show the table after filter
        yellow_sample_filtered_df.select("tpep_pickup_datetime", "fare_amount", "trip_distance").show(5, truncate=False)
        print("The table is showing the first 5 data rows from the local sample dataset with the condition")
        sys.stdout.flush()

        # Group and aggregate the data        
        yellow_sample_grouped_df = yellow_sample_filtered_df.groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("trip_date"))\
                    .agg(count("*").alias("trip_count"))\
                    .orderBy("trip_date")
        
        yellow_sample_grouped_df.show()
        print("The table is showing the total number of trips in sample dataset with the condition")
        sys.stdout.flush()

        # Large dataset:
        yellow_Feb_dataset_file = f"{yellow_tripdata_path}yellow_tripdata_2023-02.csv"
        yellow_Feb_tripdata_df = spark.read.csv(yellow_Feb_dataset_file, header=True, schema=yellow_taxi_schema)
        
        # Debug: Show the table before filter
        yellow_Feb_tripdata_df.select("tpep_pickup_datetime", "fare_amount", "trip_distance").show(5, truncate=False)
        print("The table is showing the first 5 data rows from yellow_tripdata_2023-02.csv")
        sys.stdout.flush()

        # Filter the data base on the condition
        Feb_filtered_df = yellow_Feb_tripdata_df.filter(
            (col("tpep_pickup_datetime") >= "2023-02-01") &
            (col("tpep_pickup_datetime") <= "2023-02-07") &
            (col("fare_amount") > 50) &
            (col("trip_distance") < 1)
        )

        # Debug: Show the table after filter
        Feb_filtered_df.select("tpep_pickup_datetime", "fare_amount", "trip_distance").show(5, truncate=False)
        print("The table is showing the first 5 data rows from yellow_tripdata_2023-02.csv with the condition")
        sys.stdout.flush()

        # Group and aggregate the data
        Feb_grouped_df = Feb_filtered_df.groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("trip_date"))\
            .agg(count("*").alias("trip_count"))\
            .orderBy("trip_date")
        
        Feb_grouped_df.show()
        print("The table is showing the total number of trips from yellow_tripdata_2023-02.csv with the condition")
        sys.stdout.flush()
        

        # Q3
        # Step 1: Join the datasets using PULocationID, and rename the corresponding columns as Pickup_Borough, Pickup_Zone, and Pickup_service_zone 
        dataset_joined_df = yellow_tripdata_df.join(taxi_zone_df, 
            yellow_tripdata_df.PULocationID == taxi_zone_df.LocationID, "left")\
            .withColumnRenamed("Borough", "Pickup_Borough")\
            .withColumnRenamed("Zone", "Pickup_Zone")\
            .withColumnRenamed("service_zone", "Pickup_service_zone")

        # Step 2: Drop the LocationID and PULocationID columns
        dataset_joined_df = dataset_joined_df.drop("trip_id", "LocationID", "PULocationID") 

        # Debug: Show the schema
        dataset_joined_df.printSchema()

        # Step 3: Use the result from this step to perform a second join using DOLocationID, 
        #         renaming the corresponding fields as Dropoff_Borough, Dropoff_Zone, and Dropoff_service_zone 
        final_joined_df = dataset_joined_df.join(taxi_zone_df, dataset_joined_df.DOLocationID == taxi_zone_df.LocationID, "left")\
            .withColumnRenamed("Borough", "Dropoff_Borough")\
            .withColumnRenamed("Zone", "Dropoff_Zone")\
            .withColumnRenamed("service_zone", "Dropoff_service_zone")

        # Debug: Show the schema
        final_joined_df.printSchema()

        # Step 4: Drop the LocationID and DOLocationID columns
        final_joined_df = final_joined_df.drop("LocationID", "DOLocationID")

        # Show the final schema
        final_joined_df.printSchema()

        # Q4
        # Add the "route" and "month" columns
        final_joined_df = final_joined_df.withColumn("route", concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough"))) 
        final_joined_df = final_joined_df.withColumn("Month", month(col("tpep_pickup_datetime"))) 
        
        final_joined_df.show(10)

        # Q5 
        new_dataset_df = final_joined_df.groupBy(col("Month"), col("route"))\
            .agg(round(sum(final_joined_df.tip_amount),2).alias("total_tip_amount"),
                 round(sum(final_joined_df.passenger_count).cast('integer')).alias("total_passenger_count")
                )

        new_dataset_df = new_dataset_df.withColumn("average_tip_per_passenger", round((new_dataset_df.total_tip_amount / new_dataset_df.total_passenger_count),2))

        new_dataset_df.show(10)

        # Q6
        zero_avg_dataset_df = new_dataset_df.filter(new_dataset_df.average_tip_per_passenger == 0)
        
        zero_avg_dataset_df.show()
        

        # Q7
        new_dataset_df = new_dataset_df.orderBy("average_tip_per_passenger", ascending=False)

        new_dataset_df.show(10)
        
    
    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        spark.stop()